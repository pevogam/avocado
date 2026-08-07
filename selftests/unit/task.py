import unittest
from unittest.mock import Mock, patch

from avocado.core.nrunner.runnable import Runnable
from avocado.core.nrunner.task import Task, TaskStatusService, json_dumps


class TaskStatusServiceTest(unittest.TestCase):
    def setUp(self):
        self.service = TaskStatusService("127.0.0.1:8888")
        self.status = {"id": "1-example", "status": "running"}
        self.data = (json_dumps(self.status) + "\n").encode("ascii")

    def test_post_sends_the_complete_framed_message(self):
        connection = Mock()
        self.service._connection = connection

        self.assertTrue(self.service.post(self.status))

        connection.sendall.assert_called_once_with(self.data)
        connection.send.assert_not_called()

    def test_post_reconnects_and_resends_after_connection_error(self):
        old_connection = Mock()
        old_connection.sendall.side_effect = BrokenPipeError("closed")
        new_connection = Mock()
        self.service._connection = old_connection

        def reconnect(retry=True):
            self.assertFalse(retry)
            self.service._connection = new_connection

        with patch.object(
            self.service, "_create_connection", side_effect=reconnect
        ) as create_connection:
            self.assertTrue(self.service.post(self.status))

        old_connection.close.assert_called_once_with()
        create_connection.assert_called_once_with(retry=False)
        new_connection.sendall.assert_called_once_with(self.data)

    def test_post_reports_failed_reconnect_and_forgets_connection(self):
        connection = Mock()
        connection.sendall.side_effect = ConnectionResetError("reset")
        self.service._connection = connection

        with patch.object(
            self.service,
            "_create_connection",
            side_effect=ConnectionRefusedError("refused"),
        ):
            self.assertFalse(self.service.post(self.status))

        self.assertIsNone(self.service._connection)
        connection.close.assert_called_once_with()

    def test_close_does_not_create_a_connection(self):
        with patch.object(self.service, "_create_connection") as create_connection:
            self.service.close()

        create_connection.assert_not_called()

    def test_tcp_connection_uses_configured_timeout(self):
        service = TaskStatusService("127.0.0.1:8888", timeout=1.25)
        connection = Mock()

        with patch(
            "avocado.core.nrunner.task.socket.create_connection",
            return_value=connection,
        ) as create_connection:
            service._create_connection(retry=False)

        create_connection.assert_called_once_with(("127.0.0.1", 8888), timeout=1.25)
        self.assertIs(service.connection, connection)

    def test_unix_connection_uses_configured_timeout(self):
        service = TaskStatusService("/tmp/avocado-status.sock", timeout=2.5)
        connection = Mock()

        with patch("avocado.core.nrunner.task.socket.socket", return_value=connection):
            service._create_connection(retry=False)

        connection.settimeout.assert_called_once_with(2.5)
        connection.connect.assert_called_once_with("/tmp/avocado-status.sock")
        self.assertIs(service.connection, connection)


class TaskTest(unittest.TestCase):
    def test_default_category(self):
        runnable = Runnable("noop", "noop_uri")
        task = Task(runnable, "task_id")
        self.assertEqual(task.category, "test")

    def test_set_category(self):
        runnable = Runnable("noop", "noop_uri")
        task = Task(runnable, "task_id", category="new_category")
        self.assertEqual(task.category, "new_category")
