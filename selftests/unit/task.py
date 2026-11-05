import os
import shutil
import signal
import socket
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from avocado.core.messages import MessageHandler
from avocado.core.nrunner.app import BaseRunnerApp
from avocado.core.nrunner.runnable import Runnable
from avocado.core.nrunner.task import Task, TaskStatusService
from avocado.core.utils import messages


class TaskStatusServiceTest(unittest.TestCase):
    def setUp(self):
        self.service = TaskStatusService("127.0.0.1:8888")

    def test_close_does_not_create_a_connection(self):
        with patch.object(self.service, "_create_connection") as create_connection:
            self.service.close()

        create_connection.assert_not_called()


class TaskTest(unittest.TestCase):
    def test_post_delivers_complete_message(self):
        for reconnect, timeout in (
            (False, False),
            (True, False),
            (False, True),
            (True, True),
        ):
            with self.subTest(reconnect=reconnect, timeout=timeout):
                service = TaskStatusService("127.0.0.1:8888")
                sent = bytearray()
                connection = Mock()
                # A socket may accept only part of a send() buffer.
                connection.send.side_effect = (
                    lambda data, sent=sent: sent.extend(data[:1]) or 1
                )
                connection.sendall.side_effect = (
                    socket.timeout if timeout else sent.extend
                )
                service._connection = connection
                if reconnect:
                    old = Mock()
                    old.send.side_effect = old.sendall.side_effect = BrokenPipeError
                    service._connection = old

                with patch.object(
                    service,
                    "_create_connection",
                    side_effect=lambda service=service, connection=connection: setattr(
                        service, "_connection", connection
                    ),
                ):
                    self.assertEqual(service.post({}), not timeout)

                connection.sendall.assert_called_once_with(b"{}\n")
                self.assertEqual(sent, b"" if timeout else b"{}\n")

    def test_default_category(self):
        runnable = Runnable("noop", "noop_uri")
        task = Task(runnable, "task_id")
        self.assertEqual(task.category, "test")

    def test_set_category(self):
        runnable = Runnable("noop", "noop_uri")
        task = Task(runnable, "task_id", category="new_category")
        self.assertEqual(task.category, "new_category")


class TemporaryOutputTest(unittest.TestCase):
    def test_command_preserves_job_destination(self):
        task = Task(Runnable("noop", "uri", output_dir="/job/results"), "1")
        args = task.get_command_args(temporary_output=True)
        parsed = vars(BaseRunnerApp().parser.parse_args(["task-run", *args]))
        self.assertTrue(parsed["temporary_output"])
        self.assertIsNone(Runnable.from_args(parsed).output_dir)
        self.assertEqual(task.runnable.output_dir, "/job/results")
        self.assertIn("output_dir=/job/results", task.get_command_args())

    def test_exec_output_reaches_job_before_completion(self):
        script = """import os
import pathlib
import sys
output = pathlib.Path(os.environ['AVOCADO_TEST_OUTPUTDIR'])
(output / 'nested').mkdir()
(output / 'nested' / 'binary').write_bytes(bytes(range(256)) * 1600)
(output / 'empty').touch()
(output / 'debug.log').write_text('direct log file', encoding='utf-8')
print('one stdout line')
sys.exit(int(sys.argv[1]))
"""
        for exit_code in (0, 1):
            with self.subTest(
                exit_code=exit_code
            ), tempfile.TemporaryDirectory() as root:
                destination = os.path.join(root, "results", "1-test")
                host_task = Task(
                    Runnable(
                        "exec-test",
                        sys.executable,
                        "-c",
                        script,
                        str(exit_code),
                        output_dir=destination,
                    ),
                    "1-test",
                    job_id="job",
                )
                app = BaseRunnerApp(echo=Mock())
                args = vars(
                    app.parser.parse_args(
                        ["task-run", *host_task.get_command_args(temporary_output=True)]
                    )
                )
                app.command_task_run(args)
                statuses = [call.args[0] for call in app.echo.call_args_list]
                temporary_dir = statuses[0]["output_dir"]
                self.assertNotEqual(temporary_dir, destination)
                self.assertFalse(os.path.exists(temporary_dir))
                self.assertEqual(statuses[-1]["status"], "finished")
                self.assertEqual(
                    statuses[-1]["result"], "pass" if exit_code == 0 else "fail"
                )
                self.assertEqual(
                    [s["time"] for s in statuses], sorted(s["time"] for s in statuses)
                )

                handler = MessageHandler()
                job = Mock(logdir=root, unique_id="job", test_results_path=root)
                for status in statuses:
                    if status["status"] == "finished":
                        self.assertEqual(
                            Path(destination, "nested", "binary").read_bytes(),
                            bytes(range(256)) * 1600,
                        )
                        self.assertEqual(Path(destination, "empty").read_bytes(), b"")
                    handler.process_message(status.copy(), host_task, job)
                self.assertEqual(
                    Path(destination, "stdout").read_text(encoding="utf-8"),
                    "one stdout line\n",
                )
                self.assertEqual(
                    Path(destination, "debug.log").read_text(encoding="utf-8"),
                    "direct log file",
                )
                file_statuses = [s for s in statuses if s.get("type") == "file"]
                self.assertTrue(all(len(s["log"]) <= 200000 for s in file_statuses))

    def test_file_snapshot_replaces_streamed_data_and_skips_symlinks(self):
        class Runner:
            @staticmethod
            def run(runnable):
                output = Path(runnable.output_dir)
                (output / "custom.log").write_bytes(b"complete file")
                (output / "empty").touch()
                (output / "link").symlink_to("/etc/passwd")
                (output / "directory-link").symlink_to("/etc", target_is_directory=True)
                yield messages.StartedMessage.get()
                yield messages.FileMessage.get(b"partial data", "./custom.log")
                yield messages.FileMessage.get(b"stale content", "empty")
                yield messages.FinishedMessage.get("pass")

        task = Task(Runnable("noop", "uri"), temporary_output=True)
        with patch.object(task.runnable, "pick_runner_class", return_value=Runner):
            statuses = list(task.run())
        with tempfile.TemporaryDirectory() as destination:
            handler = MessageHandler()
            host_task = Task(Runnable("noop", "uri", output_dir=destination), "1")
            job = Mock(
                logdir=destination, unique_id="job", test_results_path=destination
            )
            for status in statuses:
                handler.process_message(status.copy(), host_task, job)
            self.assertEqual(
                Path(destination, "custom.log").read_bytes(), b"complete file"
            )
            self.assertEqual(Path(destination, "empty").read_bytes(), b"")
            self.assertFalse(Path(destination, "link").exists())
            self.assertFalse(Path(destination, "directory-link").exists())

    def test_instrumented_test_artifact_is_collected(self):
        with tempfile.TemporaryDirectory() as root:
            test_file = Path(root, "artifact.py")
            test_file.write_text(
                """from pathlib import Path
from avocado import Test

class Artifact(Test):
    def test(self):
        Path(self.outputdir, 'artifact').write_bytes(b'instrumented artifact')
""",
                encoding="utf-8",
            )
            task = Task(
                Runnable("avocado-instrumented", f"{test_file}:Artifact.test"),
                temporary_output=True,
            )
            self.addCleanup(
                signal.signal, signal.SIGTERM, signal.getsignal(signal.SIGTERM)
            )
            statuses = list(task.run())
        self.assertEqual(statuses[-1]["result"], "pass")
        artifact = [s for s in statuses if s.get("path") == "data/artifact"]
        self.assertEqual(len(artifact), 1)
        self.assertEqual(artifact[0]["log"], b"instrumented artifact")

    def test_failed_delivery_preserves_output(self):
        task = Task(Runnable("noop", "uri"), temporary_output=True)
        service = Mock()
        service.post.return_value = False
        task.status_services = [service]
        with self.assertLogs("avocado.core.nrunner.task", level="WARNING"):
            statuses = list(task.run())
        temporary_dir = statuses[0]["output_dir"]
        self.addCleanup(shutil.rmtree, temporary_dir)
        self.assertTrue(os.path.isdir(temporary_dir))
        self.assertIsNone(task.runnable.output_dir)
        service.close.assert_called_once()

    def test_collection_error_preserves_output_and_reports_error(self):
        task = Task(Runnable("noop", "uri"), temporary_output=True)
        with patch(
            "avocado.core.nrunner.task.os.walk", side_effect=PermissionError("denied")
        ):
            with self.assertLogs("avocado.core.nrunner.task", level="WARNING"):
                statuses = list(task.run())
        temporary_dir = statuses[0]["output_dir"]
        self.addCleanup(shutil.rmtree, temporary_dir)
        self.assertTrue(os.path.isdir(temporary_dir))
        self.assertEqual(statuses[-1]["result"], "error")
        self.assertIn("denied", statuses[-1]["fail_reason"])
