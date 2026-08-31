import faulthandler
import multiprocessing
import os
import signal
import sys
import tempfile
import time
import traceback
from multiprocessing.connection import wait

from avocado.core.exceptions import TestInterrupt
from avocado.core.nrunner.app import BaseRunnerApp
from avocado.core.nrunner.runner import RUNNER_RUN_STATUS_INTERVAL, BaseRunner
from avocado.core.test import TestID
from avocado.core.tree import TreeNodeEnvOnly
from avocado.core.utils import loader, messages
from avocado.core.varianter import is_empty_variant
from avocado.utils.deprecation import log_deprecation


class AvocadoInstrumentedTestRunner(BaseRunner):
    """
    Runner for avocado-instrumented tests

    Runnable attributes usage:

     * uri: path to a test file, combined with an Avocado.Test
       inherited class name and method.  The test file path and
       class and method names should be separated by a ":".  One
       example of a valid uri is "mytest.py:Class.test_method".

     * args: not used
    """

    name = "avocado-instrumented"
    description = "Runner for avocado-instrumented tests"

    CONFIGURATION_USED = [
        "run.test_parameters",
        "datadir.paths.cache_dirs",
        "core.show",
        "job.output.loglevel",
        "job.run.store_logging_stream",
    ]

    PROCESS_JOIN_TIMEOUT = 1.0
    QUEUE_WRITER_BLOCKED_WARNING = 5.0
    QUEUE_WRITER_BLOCKED_TIMEOUT = 30.0

    @staticmethod
    def signal_handler(signum, frame):  # pylint: disable=W0613
        if signum == signal.SIGTERM.value:
            raise TestInterrupt("Test interrupted: Timeout reached")

    @staticmethod
    def _create_params(runnable):
        """Create params for the test"""
        if runnable.variant is None:
            return None

        # rebuild the variant tree
        variant_tree_nodes = [
            TreeNodeEnvOnly(path, env) for path, env in runnable.variant["variant"]
        ]

        if not is_empty_variant(variant_tree_nodes):
            tree_nodes = variant_tree_nodes
            paths = runnable.variant["paths"]
            return tree_nodes, paths

    @staticmethod
    def _register_stack_dump(result_dir):
        """Register an out-of-band signal-triggered Python stack dump."""
        signum = getattr(signal, "SIGUSR2", None)
        if signum is None or not hasattr(faulthandler, "register"):
            return None

        stack_file = None
        try:
            stack_path = os.path.join(result_dir, "runner-stacks.log")
            stack_file = open(  # pylint: disable=consider-using-with
                stack_path, "a", encoding="utf-8", buffering=1
            )
            stack_file.write(
                "\nInstrumented test process "
                f"PID {os.getpid()}: send SIGUSR2 to dump all Python thread "
                "stacks.\n"
            )
            faulthandler.register(
                signum, file=stack_file, all_threads=True, chain=False
            )
        except (OSError, RuntimeError, ValueError):
            if stack_file is not None:
                stack_file.close()
            return None
        return stack_file

    @staticmethod
    def _unregister_stack_dump(stack_file):
        """Undo stack dump registration and close its out-of-band file."""
        if stack_file is None:
            return
        signum = getattr(signal, "SIGUSR2", None)
        try:
            if signum is not None and hasattr(faulthandler, "unregister"):
                try:
                    faulthandler.unregister(signum)
                except (OSError, RuntimeError, ValueError):
                    pass
        finally:
            stack_file.close()

    @staticmethod
    def _queue_writer_locked(queue):
        """Return the SimpleQueue writer lock state without acquiring it."""
        lock = getattr(queue, "_wlock", None)
        semlock = getattr(lock, "_semlock", None)
        get_value = getattr(semlock, "_get_value", None)
        if get_value is None:
            return None
        try:
            return get_value() == 0
        except (OSError, NotImplementedError, ValueError):
            return None

    @classmethod
    def _cleanup_process(cls, process):
        """Reap a child, terminating it if it outlives the runner."""
        if process is None:
            return
        try:
            if process.pid is None:
                return
            process.join(cls.PROCESS_JOIN_TIMEOUT)
            if process.is_alive():
                process.terminate()
                process.join(cls.PROCESS_JOIN_TIMEOUT)
            if process.is_alive() and hasattr(process, "kill"):
                process.kill()
                process.join(cls.PROCESS_JOIN_TIMEOUT)
            if not process.is_alive():
                process.close()
        except (AssertionError, OSError, ValueError):
            # Cleanup must not replace the status already produced by the runner.
            return

    @staticmethod
    def _close_parent_queue_writer(queue):
        """Close the parent's unused writer so a dead producer causes EOF."""
        writer = getattr(queue, "_writer", None)
        if writer is not None:
            try:
                writer.close()
            except (OSError, ValueError):
                pass

    @staticmethod
    def _close_queue(queue):
        """Close the parent side of a multiprocessing queue."""
        close = getattr(queue, "close", None)
        if close is not None:
            try:
                close()
            except (OSError, ValueError):
                pass

    @staticmethod
    def _run_avocado(runnable, queue):
        def load_and_run_test(test_factory):
            instance = loader.load_test(test_factory)
            early_state = instance.get_state()
            early_state["type"] = "early_state"
            queue.put(early_state)
            log_deprecation.flush()
            instance.run_avocado()
            return instance.get_state()

        stack_file = None
        try:
            # This assumes that a proper resolution (see resolver module)
            # was performed, and that a URI contains:
            # 1) path to python module
            # 2) class
            # 3) method
            #
            # To be defined: if the resolution uri should be composed like
            # this, or broken down and stored into other data fields
            signal.signal(signal.SIGTERM, AvocadoInstrumentedTestRunner.signal_handler)
            module_path, klass_method = runnable.uri.split(":", 1)

            klass, method = klass_method.split(".", 1)

            params = AvocadoInstrumentedTestRunner._create_params(runnable)
            result_dir = runnable.output_dir or tempfile.mkdtemp(prefix=".avocado-task")
            stack_file = AvocadoInstrumentedTestRunner._register_stack_dump(result_dir)
            test_factory = [
                klass,
                {
                    "name": TestID(1, runnable.uri, runnable.variant),
                    "methodName": method,
                    "config": runnable.config,
                    "modulePath": module_path,
                    "params": params,
                    "tags": runnable.tags,
                    "run.results_dir": result_dir,
                },
            ]

            messages.start_logging(runnable.config, queue)

            # running the actual test
            if "COVERAGE_RUN" in os.environ:
                from coverage import Coverage

                coverage = Coverage(data_suffix=True)
                with coverage.collect():
                    state = load_and_run_test(test_factory)
                coverage.save()
            else:
                state = load_and_run_test(test_factory)

            fail_reason = state.get("fail_reason")
            queue.put(messages.WhiteboardMessage.get(state["whiteboard"]))
            queue.put(
                messages.FinishedMessage.get(
                    state["status"].lower(),
                    fail_reason=fail_reason,
                    class_name=klass,
                    fail_class=state.get("fail_class"),
                    traceback=state.get("traceback"),
                )
            )
        except Exception as e:
            queue.put(messages.StderrMessage.get(traceback.format_exc()))
            queue.put(
                messages.FinishedMessage.get(
                    "error",
                    fail_reason=str(e),
                    fail_class=e.__class__.__name__,
                    traceback=traceback.format_exc(),
                )
            )
        finally:
            AvocadoInstrumentedTestRunner._unregister_stack_dump(stack_file)

    @classmethod
    def _monitor(cls, process, queue):
        """Forward child messages while also watching for child process death."""
        reader = queue._reader  # pylint: disable=protected-access
        process_sentinel = process.sentinel
        next_status_time = time.monotonic()
        received_messages = 0
        forwarded_messages = 0
        last_message = None
        writer_locked_since = None
        queue_error = None

        def available_messages():
            nonlocal last_message, queue_error, received_messages
            while reader.poll():
                try:
                    message = queue.get()
                except (EOFError, OSError) as error:
                    # Preserve a truncated-frame error if a later drain only
                    # observes the clean EOF that follows process exit.
                    if queue_error is None:
                        queue_error = error
                    return
                received_messages += 1
                last_message = message
                yield message

        while True:
            now = time.monotonic()
            timeout = max(0, next_status_time - now)
            # Closing the last queue writer makes the reader report EOF just
            # before the process sentinel necessarily becomes readable.  Once
            # that normal EOF has been observed, waiting on the reader again
            # would both spin and misclassify this ordering window as a queue
            # failure while the child still appears alive.
            waitables = (
                (process_sentinel,)
                if isinstance(queue_error, EOFError)
                else (reader, process_sentinel)
            )
            ready = wait(waitables, timeout)
            child_exited = process_sentinel in ready

            # Always drain first, including when child death and queue data become
            # visible together.  A queued FinishedMessage is authoritative.
            if reader in ready or child_exited:
                for message in available_messages():
                    if message.get("type") != "early_state":
                        forwarded_messages += 1
                        yield message
                        next_status_time = time.monotonic() + RUNNER_RUN_STATUS_INTERVAL
                    if message.get("status") == "finished":
                        return

            if (
                queue_error is not None
                and not isinstance(queue_error, EOFError)
                and not child_exited
                and process.is_alive()
            ):
                writer_locked = cls._queue_writer_locked(queue)
                fail_reason = (
                    f"Instrumented test process PID {process.pid} message queue "
                    f"failed with {queue_error.__class__.__name__}: {queue_error}; "
                    f"child alive={process.is_alive()}, exit code={process.exitcode}, "
                    f"received {received_messages} queue message(s), forwarded "
                    f"{forwarded_messages}, writer lock held={writer_locked!r}"
                )
                yield messages.FinishedMessage.get(
                    "error",
                    fail_reason=fail_reason,
                    returncode=process.exitcode,
                    fail_class="InstrumentedTestMessageQueueError",
                    child_pid=process.pid,
                    queue_messages_received=received_messages,
                    queue_messages_forwarded=forwarded_messages,
                    queue_writer_locked=writer_locked,
                )
                return

            if child_exited or not process.is_alive():
                # Refresh exitcode and make one final drain after observing death.
                process.join(timeout=0)
                if queue_error is None:
                    for message in available_messages():
                        if message.get("type") != "early_state":
                            forwarded_messages += 1
                            yield message
                        if message.get("status") == "finished":
                            return

                # EOF is the normal end of the one-way pipe once the producer
                # exits.  Other errors indicate a truncated/corrupt frame.
                if queue_error is not None and not isinstance(queue_error, EOFError):
                    writer_locked = cls._queue_writer_locked(queue)
                    fail_reason = (
                        f"Instrumented test process PID {process.pid} exited with "
                        f"code {process.exitcode} and its message queue failed with "
                        f"{queue_error.__class__.__name__}: {queue_error}; received "
                        f"{received_messages} queue message(s), forwarded "
                        f"{forwarded_messages}, writer lock held={writer_locked!r}"
                    )
                    yield messages.FinishedMessage.get(
                        "error",
                        fail_reason=fail_reason,
                        returncode=process.exitcode,
                        fail_class="InstrumentedTestMessageQueueError",
                        child_pid=process.pid,
                        queue_messages_received=received_messages,
                        queue_messages_forwarded=forwarded_messages,
                        queue_writer_locked=writer_locked,
                    )
                    return

                writer_locked = cls._queue_writer_locked(queue)
                last_status = (
                    last_message.get("status") if last_message is not None else None
                )
                last_type = (
                    last_message.get("type") if last_message is not None else None
                )
                fail_reason = (
                    f"Instrumented test process PID {process.pid} exited with "
                    f"code {process.exitcode} without sending a finished message; "
                    f"received {received_messages} queue message(s), forwarded "
                    f"{forwarded_messages}, last status={last_status!r}, "
                    f"last type={last_type!r}, writer lock held={writer_locked!r}"
                )
                yield messages.FinishedMessage.get(
                    "error",
                    fail_reason=fail_reason,
                    returncode=process.exitcode,
                    fail_class="InstrumentedTestProcessError",
                    child_pid=process.pid,
                    queue_messages_received=received_messages,
                    queue_messages_forwarded=forwarded_messages,
                    queue_writer_locked=writer_locked,
                    last_queue_message_status=last_status,
                    last_queue_message_type=last_type,
                )
                return

            now = time.monotonic()
            if now >= next_status_time:
                writer_locked = cls._queue_writer_locked(queue)
                if writer_locked:
                    if writer_locked_since is None:
                        writer_locked_since = now
                else:
                    writer_locked_since = None

                diagnostic = {
                    "child_pid": process.pid,
                    "queue_messages_received": received_messages,
                    "queue_messages_forwarded": forwarded_messages,
                }
                if writer_locked_since is not None:
                    writer_locked_for = now - writer_locked_since
                    if writer_locked_for >= cls.QUEUE_WRITER_BLOCKED_WARNING:
                        diagnostic["queue_writer_blocked_seconds"] = round(
                            writer_locked_for, 3
                        )
                    if writer_locked_for >= cls.QUEUE_WRITER_BLOCKED_TIMEOUT:
                        fail_reason = (
                            f"Instrumented test process PID {process.pid} message "
                            f"queue writer lock remained held for "
                            f"{writer_locked_for:.3f} seconds while no message was "
                            "readable; the logging queue is deadlocked"
                        )
                        yield messages.FinishedMessage.get(
                            "error",
                            fail_reason=fail_reason,
                            fail_class="InstrumentedTestMessageQueueDeadlock",
                            child_pid=process.pid,
                            queue_messages_received=received_messages,
                            queue_messages_forwarded=forwarded_messages,
                            queue_writer_locked=True,
                            queue_writer_blocked_seconds=round(writer_locked_for, 3),
                        )
                        return
                yield messages.RunningMessage.get(**diagnostic)
                next_status_time = now + RUNNER_RUN_STATUS_INTERVAL

    def run(self, runnable):
        # pylint: disable=W0201
        signal.signal(signal.SIGTERM, AvocadoInstrumentedTestRunner.signal_handler)
        self.runnable = runnable
        yield messages.StartedMessage.get()
        queue = None
        process = None
        try:
            queue = multiprocessing.SimpleQueue()
            process = multiprocessing.Process(
                target=self._run_avocado, args=(self.runnable, queue)
            )

            process.start()
            self._close_parent_queue_writer(queue)

            yield from self._monitor(process, queue)

        except TestInterrupt:
            if process is not None and process.pid is not None and process.is_alive():
                process.terminate()
            if process is not None and process.pid is not None and queue is not None:
                yield from self._monitor(process, queue)
            else:
                yield messages.FinishedMessage.get(
                    "error",
                    fail_reason="Runner interrupted before the test process started",
                    fail_class="InstrumentedTestProcessError",
                )
        except Exception as e:
            yield messages.StderrMessage.get(traceback.format_exc())
            yield messages.FinishedMessage.get(
                "error",
                fail_reason=str(e),
                fail_class=e.__class__.__name__,
                traceback=traceback.format_exc(),
            )
        finally:
            self._cleanup_process(process)
            self._close_queue(queue)


class RunnerApp(BaseRunnerApp):
    PROG_NAME = "avocado-runner-avocado-instrumented"
    PROG_DESCRIPTION = "nrunner application for avocado-instrumented tests"
    RUNNABLE_KINDS_CAPABLE = ["avocado-instrumented"]


def main():
    if sys.platform == "darwin":
        multiprocessing.set_start_method("fork")
    app = RunnerApp(print)
    app.run()


if __name__ == "__main__":
    main()
