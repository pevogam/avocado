import gc
import logging
import unittest

from avocado.core.utils.messages import RunnerLogHandler, StreamToQueue


class BrokenQueue:
    @staticmethod
    def put(_message):
        raise RuntimeError("queue is broken")


class QueueLoggingGarbageCollectionTest(unittest.TestCase):
    def setUp(self):
        self.gc_was_enabled = gc.isenabled()
        gc.enable()

    def tearDown(self):
        if self.gc_was_enabled:
            gc.enable()
        else:
            gc.disable()

    def test_log_handler_restores_gc_after_queue_error(self):
        handler = RunnerLogHandler(BrokenQueue(), "log")
        record = logging.LogRecord(
            "avocado.test", logging.INFO, __file__, 1, "message", (), None
        )

        with self.assertRaisesRegex(RuntimeError, "queue is broken"):
            handler.emit(record)

        self.assertTrue(gc.isenabled())

    def test_stream_restores_gc_after_queue_error(self):
        stream = StreamToQueue(BrokenQueue(), "stdout")

        with self.assertRaisesRegex(RuntimeError, "queue is broken"):
            stream.write("message")

        self.assertTrue(gc.isenabled())

    def test_handler_preserves_previously_disabled_gc(self):
        handler = RunnerLogHandler(BrokenQueue(), "log")
        record = logging.LogRecord(
            "avocado.test", logging.INFO, __file__, 1, "message", (), None
        )
        gc.disable()

        with self.assertRaisesRegex(RuntimeError, "queue is broken"):
            handler.emit(record)

        self.assertFalse(gc.isenabled())


if __name__ == "__main__":
    unittest.main()
