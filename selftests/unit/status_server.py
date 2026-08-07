import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, call

from avocado.core.status.server import StatusServer
from avocado.core.status.utils import StatusMsgInvalidJSONError


class StatusServerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.repo = Mock()
        self.server = StatusServer("127.0.0.1:8888", self.repo)
        self.writer = Mock()
        self.writer.get_extra_info.return_value = ("127.0.0.1", 12345)
        self.writer.wait_closed = AsyncMock()

    async def test_processes_messages_and_closes_client(self):
        reader = Mock()
        reader.readline = AsyncMock(side_effect=[b'{"status": "running"}\n', b""])

        await self.server.cb(reader, self.writer)

        self.repo.process_raw_message.assert_called_once_with(
            b'{"status": "running"}\n'
        )
        self.writer.close.assert_called_once_with()
        self.writer.wait_closed.assert_awaited_once_with()
        self.assertNotIn(self.writer, self.server._connections)

    async def test_invalid_json_does_not_discard_following_message(self):
        first = b"truncated JSON\n"
        second = b'{"status": "finished"}\n'
        reader = Mock()
        reader.readline = AsyncMock(side_effect=[first, second, b""])
        self.repo.process_raw_message.side_effect = [
            StatusMsgInvalidJSONError(first),
            None,
        ]

        with self.assertLogs("avocado.core.status.server", level="WARNING"):
            await self.server.cb(reader, self.writer)

        self.assertEqual(
            self.repo.process_raw_message.call_args_list,
            [call(first), call(second)],
        )

    async def test_repo_error_does_not_discard_following_message(self):
        first = b'{"status": "started"}\n'
        second = b'{"status": "finished"}\n'
        reader = Mock()
        reader.readline = AsyncMock(side_effect=[first, second, b""])
        self.repo.process_raw_message.side_effect = [RuntimeError("bad status"), None]

        with self.assertLogs("avocado.core.status.server", level="ERROR"):
            await self.server.cb(reader, self.writer)

        self.assertEqual(
            self.repo.process_raw_message.call_args_list,
            [call(first), call(second)],
        )

    async def test_oversized_message_is_diagnosed_and_connection_is_closed(self):
        reader = Mock()
        finished = b'{"status": "finished"}\n'
        reader.readline = AsyncMock(
            side_effect=[
                ValueError("Separator is found, but chunk is longer than limit"),
                finished,
                b"",
            ]
        )
        self.server._buffer_limit = 1024

        with self.assertLogs("avocado.core.status.server", level="ERROR") as logs:
            await self.server.cb(reader, self.writer)

        self.assertIn("exceeded the stream buffer limit", logs.output[0])
        self.assertIn("limit=1024", logs.output[0])
        self.repo.process_raw_message.assert_called_once_with(finished)
        self.writer.close.assert_called_once_with()
        self.writer.wait_closed.assert_awaited_once_with()

    async def test_limit_overrun_reports_consumed_bytes(self):
        reader = Mock()
        reader.readline = AsyncMock(
            side_effect=asyncio.LimitOverrunError("too large", consumed=2048)
        )

        with self.assertLogs("avocado.core.status.server", level="ERROR") as logs:
            await self.server.cb(reader, self.writer)

        self.assertIn("consumed=2048", logs.output[0])

    async def test_read_error_is_diagnosed_and_connection_is_closed(self):
        reader = Mock()
        reader.readline = AsyncMock(side_effect=ConnectionResetError("reset"))

        with self.assertLogs("avocado.core.status.server", level="WARNING"):
            await self.server.cb(reader, self.writer)

        self.writer.close.assert_called_once_with()
        self.writer.wait_closed.assert_awaited_once_with()

    def test_close_is_safe_before_server_creation(self):
        self.server.close()

    def test_close_stops_listener_and_connected_clients(self):
        listener = Mock()
        self.server._server_task = listener
        self.server._connections.add(self.writer)

        self.server.close()

        listener.close.assert_called_once_with()
        self.writer.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
