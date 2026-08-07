import multiprocessing
import os
import struct
import time
import unittest
from unittest.mock import patch

from avocado.core.utils import messages
from avocado.plugins.runners.avocado_instrumented import (
    AvocadoInstrumentedTestRunner,
)


def _put_finished(queue):
    queue.put(messages.LogMessage.get("last log"))
    queue.put(messages.FinishedMessage.get("pass"))


def _exit_without_finished(_queue):
    os._exit(7)  # pylint: disable=protected-access


def _write_partial_queue_frame(queue):
    # multiprocessing.connection uses a network-order signed length followed
    # by the pickle payload.  Deliberately die halfway through that frame.
    os.write(queue._writer.fileno(), struct.pack("!i", 4096) + b"partial")
    # Give the monitor time to enter recv() before closing the only writer.
    time.sleep(0.05)
    os._exit(8)  # pylint: disable=protected-access


def _put_many_messages(queue, count):
    for index in range(count):
        queue.put(messages.LogMessage.get(f"log {index}"))
    queue.put(messages.FinishedMessage.get("pass"))


def _sleep_without_messages(_queue):
    time.sleep(10)


class InstrumentedRunnerMonitorTest(unittest.TestCase):
    def setUp(self):
        self.runner = AvocadoInstrumentedTestRunner()
        self.context = (
            multiprocessing.get_context("fork")
            if os.name != "nt"
            else multiprocessing.get_context()
        )
        self.process = None
        self.queue = None

    def tearDown(self):
        self.runner._cleanup_process(self.process)
        self.runner._close_queue(self.queue)

    def start_process(self, target, *args):
        self.queue = self.context.SimpleQueue()
        self.process = self.context.Process(target=target, args=(self.queue, *args))
        self.process.start()
        self.runner._close_parent_queue_writer(self.queue)

    def monitor(self):
        started = time.monotonic()
        result = list(self.runner._monitor(self.process, self.queue))
        self.assertLess(time.monotonic() - started, 5)
        return result

    def test_forwards_queued_finished_message_before_child_exit(self):
        self.start_process(_put_finished)

        output = self.monitor()

        self.assertEqual(
            [item.get("log") for item in output if item.get("type") == "log"],
            [b"last log"],
        )
        self.assertEqual(output[-1]["result"], "pass")

    def test_dead_child_without_finished_becomes_error(self):
        self.start_process(_exit_without_finished)

        output = self.monitor()

        self.assertEqual(output[-1]["status"], "finished")
        self.assertEqual(output[-1]["result"], "error")
        self.assertEqual(output[-1]["returncode"], 7)
        self.assertIn("without sending a finished message", output[-1]["fail_reason"])

    @unittest.skipIf(os.name == "nt", "relies on POSIX pipe framing and os._exit")
    def test_partial_queue_frame_does_not_block_monitor(self):
        self.start_process(_write_partial_queue_frame)

        output = self.monitor()

        self.assertEqual(output[-1]["status"], "finished")
        self.assertEqual(output[-1]["result"], "error")
        self.assertEqual(output[-1]["fail_class"], "InstrumentedTestMessageQueueError")

    def test_drains_log_burst_without_one_message_per_poll_delay(self):
        message_count = 500
        self.start_process(_put_many_messages, message_count)

        output = self.monitor()

        logs = [item for item in output if item.get("type") == "log"]
        self.assertEqual(len(logs), message_count)
        self.assertEqual(output[-1]["result"], "pass")

    @unittest.skipIf(os.name == "nt", "SimpleQueue has no writer lock on Windows")
    def test_orphaned_queue_writer_lock_becomes_error(self):
        self.start_process(_sleep_without_messages)
        writer_lock = self.queue._wlock
        writer_lock.acquire()
        try:
            with (
                patch(
                    "avocado.plugins.runners.avocado_instrumented."
                    "RUNNER_RUN_STATUS_INTERVAL",
                    0.01,
                ),
                patch.object(
                    AvocadoInstrumentedTestRunner,
                    "QUEUE_WRITER_BLOCKED_WARNING",
                    0.0,
                ),
                patch.object(
                    AvocadoInstrumentedTestRunner,
                    "QUEUE_WRITER_BLOCKED_TIMEOUT",
                    0.03,
                ),
            ):
                output = self.monitor()
        finally:
            writer_lock.release()

        self.assertEqual(output[-1]["status"], "finished")
        self.assertEqual(output[-1]["result"], "error")
        self.assertEqual(
            output[-1]["fail_class"], "InstrumentedTestMessageQueueDeadlock"
        )
        self.assertTrue(output[-1]["queue_writer_locked"])


if __name__ == "__main__":
    unittest.main()
