import socket
import unittest
from unittest.mock import Mock, patch

from avocado.core.nrunner.runnable import Runnable
from avocado.core.nrunner.task import Task, TaskStatusService


class TaskStatusServiceTest(unittest.TestCase):
    def setUp(self):
        self.service = TaskStatusService("127.0.0.1:8888")

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
            service._create_connection()

        create_connection.assert_called_once_with(("127.0.0.1", 8888), timeout=1.25)
        self.assertIs(service.connection, connection)

    def test_unix_connection_uses_configured_timeout(self):
        service = TaskStatusService("/tmp/avocado-status.sock", timeout=2.5)
        connection = Mock()

        with patch("avocado.core.nrunner.task.socket.socket", return_value=connection):
            service._create_connection()

        connection.settimeout.assert_called_once_with(2.5)
        connection.connect.assert_called_once_with("/tmp/avocado-status.sock")
        self.assertIs(service.connection, connection)


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
