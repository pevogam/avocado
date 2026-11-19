import asyncio
import os
import shlex
import tempfile
import unittest
from unittest import mock

from avocado_spawner_remote import REMOTE_PID_MARKER, RemoteSpawner

from avocado.core.nrunner.app import BaseRunnerApp
from avocado.core.nrunner.runnable import Runnable
from avocado.core.nrunner.task import Task
from avocado.core.task.runtime import RuntimeTask


class RemoteOutput(unittest.TestCase):
    def _check_spawn(self, copy_output=None):
        with tempfile.TemporaryDirectory() as root:
            destination = os.path.join(root, "results")
            task = Task(Runnable("noop", None, output_dir=destination), "1")
            runtime_task = RuntimeTask(task)
            runtime_task.spawner_handle = mock.Mock()
            config = {"spawner.remote.slots": []}
            if copy_output is not None:
                config["spawner.remote.copy_output"] = copy_output
            spawner = RemoteSpawner(config)
            with (
                mock.patch.object(RemoteSpawner, "slots_cache", {}),
                mock.patch.object(
                    RemoteSpawner,
                    "run_remote_cmd",
                    side_effect=[(1, ""), (0, f"{REMOTE_PID_MARKER}123\n")],
                ) as launch,
            ):
                self.assertTrue(asyncio.run(spawner.spawn_task(runtime_task)))

            command = launch.call_args.args[1].split(" > /dev/null", 1)[0]
            args = shlex.split(command)[3:]
            parsed = vars(BaseRunnerApp().parser.parse_args(args))
            copies_output = copy_output is not False
            self.assertEqual(parsed["temporary_output"], copies_output)
            self.assertEqual(
                Runnable.from_args(parsed).output_dir,
                None if copies_output else destination,
            )
            self.assertEqual(task.runnable.output_dir, destination)
            self.assertTrue(os.path.isdir(destination))

    def test_copies_output_by_default(self):
        self._check_spawn()

    def test_copies_output_when_enabled(self):
        self._check_spawn(True)

    def test_uses_mounted_output_when_copying_disabled(self):
        self._check_spawn(False)
