import asyncio
import logging
import os

from avocado.core.settings import settings
from avocado.core.status.utils import StatusMsgInvalidJSONError

LOG = logging.getLogger(__name__)


class StatusServer:
    """Server that listens for status messages and updates a StatusRepo."""

    # StreamReader.readline() does not suspend while complete records are
    # already buffered.  Bound the amount of synchronous work one hot client
    # can perform before other connections, workers and timers get a turn.
    _RECORDS_PER_EVENT_LOOP_TURN = 64

    def __init__(self, uri, repo):
        """Initializes a new StatusServer.

        :param uri: either a "host:port" string or a path to a UNIX socket
        :type uri: str
        :param repo: the repository to use to process received status
                     messages
        :type repo: :class:`avocado.core.status.repo.StatusRepo`
        """
        self._uri = uri
        self._repo = repo
        self._server_task = None
        self._connections = set()
        self._buffer_limit = None

    @property
    def uri(self):
        return self._uri

    async def create_server(self):
        limit = settings.as_dict().get("run.status_server_buffer_size")
        self._buffer_limit = limit
        if ":" in self._uri:
            host, port = self._uri.split(":")
            port = int(port)
            self._server_task = await asyncio.start_server(
                self.cb, host=host, port=port, limit=limit
            )
        else:
            self._server_task = await asyncio.start_unix_server(
                self.cb, path=self._uri, limit=limit
            )

    async def serve_forever(self):
        if self._server_task is None:
            await self.create_server()

        await self._server_task.serve_forever()

    def close(self):
        """Stop accepting connections and close connected status clients."""
        if self._server_task is None:
            return

        self._server_task.close()
        for writer in tuple(self._connections):
            try:
                writer.close()
            except OSError as error:
                LOG.debug("Error closing status client connection: %s", error)

        if ":" not in self._uri:
            try:
                os.unlink(self._uri)
            except FileNotFoundError:
                pass
            except OSError:
                LOG.exception("Could not remove status server socket %s", self._uri)

    async def wait_closed(self):
        """Wait until the listening socket and client sockets are closed."""
        if self._server_task is not None:
            await self._server_task.wait_closed()

        writers = tuple(self._connections)
        if writers:
            await asyncio.gather(
                *(writer.wait_closed() for writer in writers),
                return_exceptions=True,
            )

    async def cb(self, reader, writer):
        """Read newline-delimited status messages from one client."""
        peer = None
        if writer is not None:
            try:
                peer = writer.get_extra_info("peername")
            except (AttributeError, OSError):
                pass
            self._connections.add(writer)

        sequence = 0
        received_bytes = 0
        records_this_turn = 0
        try:
            while True:
                try:
                    raw_message = await reader.readline()
                except ValueError as error:
                    consumed = getattr(error, "consumed", None)
                    consumed_text = (
                        f", consumed={consumed}" if consumed is not None else ""
                    )
                    LOG.error(
                        "Status message from %r exceeded the stream buffer "
                        "limit (limit=%r%s): %s",
                        peer,
                        self._buffer_limit,
                        consumed_text,
                        error,
                    )
                    # StreamReader.readline() removes the oversized record (or
                    # clears its buffered fragment) before raising ValueError.
                    # Continue so a terminal message already buffered behind
                    # it still reaches the repository.
                except asyncio.LimitOverrunError as error:
                    # readline() normally translates this into ValueError and
                    # recovers its buffer.  If a custom reader exposes it
                    # directly, recovery is unknown and retrying could spin on
                    # the same bytes forever.
                    LOG.error(
                        "Status message from %r exceeded the stream buffer "
                        "limit (limit=%r, consumed=%d): %s",
                        peer,
                        self._buffer_limit,
                        error.consumed,
                        error,
                    )
                    return
                except (ConnectionResetError, BrokenPipeError) as error:
                    LOG.warning(
                        "Status client connection from %r was lost: %s", peer, error
                    )
                    return
                except OSError as error:
                    LOG.warning(
                        "I/O error reading status message from %r: %s", peer, error
                    )
                    return
                except Exception:  # pylint: disable=W0718
                    LOG.exception(
                        "Unexpected error reading status message from %r", peer
                    )
                    return
                else:
                    if not raw_message:
                        return

                    sequence += 1
                    message_size = len(raw_message)
                    received_bytes += message_size
                    try:
                        self._repo.process_raw_message(raw_message)
                    except StatusMsgInvalidJSONError:
                        preview = raw_message[:256]
                        if message_size > len(preview):
                            preview += b"..."
                        LOG.warning(
                            "Invalid JSON in internal status message from %r "
                            "(sequence=%d, size=%d, preview=%r)",
                            peer,
                            sequence,
                            message_size,
                            preview,
                        )
                    except Exception:  # pylint: disable=W0718
                        # A malformed status must not terminate the callback
                        # and silently discard all messages that follow it on
                        # the same connection (especially a terminal status).
                        LOG.exception(
                            "Unexpected error processing internal status message "
                            "from %r (sequence=%d, size=%d)",
                            peer,
                            sequence,
                            message_size,
                        )

                # Recoverable oversized records consume a turn as well, or a
                # client with many malformed buffered records could still
                # monopolize the loop.
                records_this_turn += 1
                if records_this_turn >= self._RECORDS_PER_EVENT_LOOP_TURN:
                    records_this_turn = 0
                    await asyncio.sleep(0)
        finally:
            LOG.debug(
                "Status client connection from %r closed after %d message(s) "
                "and %d byte(s)",
                peer,
                sequence,
                received_bytes,
            )
            if writer is not None:
                self._connections.discard(writer)
                try:
                    writer.close()
                    await writer.wait_closed()
                except (AttributeError, ConnectionError, OSError) as error:
                    LOG.debug(
                        "Error while closing status client connection from %r: %s",
                        peer,
                        error,
                    )
