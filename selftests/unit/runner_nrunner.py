import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from avocado.plugins.runner_nrunner import Runner


class FakeStatusRepo:
    def __init__(self, message_count):
        self.journal = [
            (float(index), "1-noop", "running", index) for index in range(message_count)
        ]
        self.messages = [
            {"status": "running", "type": "log", "sequence": index}
            for index in range(message_count)
        ]

    @property
    def status_journal_size(self):
        return len(self.journal)

    def status_journal_summary_pop(self):
        if not self.journal:
            raise IndexError
        return self.journal.pop(0)

    def get_task_data(self, _task_id, index):
        return self.messages[index]


class RunnerStatusUpdate(unittest.IsolatedAsyncioTestCase):
    async def test_update_status_yields_after_bounded_batch(self):
        runner = Runner()
        runner._STATUS_MESSAGES_PER_EVENT_LOOP_TURN = 2
        runner.status_repo = FakeStatusRepo(3)
        runner.tsm = SimpleNamespace(tasks_by_id={"1-noop": Mock()})
        handler = Mock()

        with patch(
            "avocado.plugins.runner_nrunner.MessageHandler", return_value=handler
        ):
            updater = asyncio.create_task(runner._update_status(Mock()))
            await asyncio.sleep(0)

            self.assertEqual(handler.process_message.call_count, 2)

            updater.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await updater

    async def test_wait_for_status_journal_tracks_actual_backlog(self):
        runner = Runner()
        runner._STATUS_JOURNAL_DRAIN_TIMEOUT = 0.5
        runner.status_repo = FakeStatusRepo(1)

        async def consume_message():
            await asyncio.sleep(0)
            runner.status_repo.status_journal_summary_pop()

        consumer = asyncio.create_task(consume_message())
        await runner._wait_for_status_journal()
        await consumer

        self.assertEqual(runner.status_repo.status_journal_size, 0)


if __name__ == "__main__":
    unittest.main()
