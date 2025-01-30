# Intercept compute -> pageserver connections, to simulate various failure modes

from __future__ import annotations

import asyncio
import struct
import threading
import traceback
from asyncio import TaskGroup
from enum import Enum

from fixtures.log_helper import log


class ConnectionState(Enum):
    HANDSHAKE = (1,)
    AUTHOK = (2,)
    COPYBOTH = (3,)


class BreakConnectionException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class ProxyShutdownException(Exception):
    """Exception raised to shut down connection when the proxy is shut down."""


# The handshake flow:
#
# 1. compute establishes TCP connection
# 2. libpq handshake and auth
# 3. Enter CopyBoth mode
#
# From then on, CopyData messages are exchanged in both directions
class Connection:
    def __init__(
        self,
        conn_id,
        compute_reader,
        compute_writer,
        shutdown_event,
        dest_port: int,
        response_cb=None,
    ):
        self.conn_id = conn_id
        self.compute_reader = compute_reader
        self.compute_writer = compute_writer
        self.shutdown_event = shutdown_event
        self.response_cb = response_cb
        self.dest_port = dest_port

        self.state = ConnectionState.HANDSHAKE

    async def run(self):
        async def wait_for_shutdown():
            await self.shutdown_event.wait()
            raise ProxyShutdownException

        try:
            addr = self.compute_writer.get_extra_info("peername")
            log.info(f"[{self.conn_id}] connection received from {addr}")

            async with TaskGroup() as group:
                group.create_task(wait_for_shutdown())

                self.ps_reader, self.ps_writer = await asyncio.open_connection(
                    "localhost", self.dest_port
                )

                group.create_task(self.handle_compute_to_pageserver())
                group.create_task(self.handle_pageserver_to_compute())

        except* ProxyShutdownException:
            log.info(f"[{self.conn_id}] proxy shutting down")

        except* asyncio.exceptions.IncompleteReadError as e:
            log.info(f"[{self.conn_id}] EOF reached: {e}")

        except* BreakConnectionException as eg:
            for e in eg.exceptions:
                log.info(f"[{self.conn_id}] callback breaks connection: {e}")

        except* Exception as e:
            s = "\n".join(traceback.format_exception(e))
            log.info(f"[{self.conn_id}] {s}")

        self.compute_writer.close()
        self.ps_writer.close()
        await self.compute_writer.wait_closed()
        await self.ps_writer.wait_closed()

    async def handle_compute_to_pageserver(self):
        while self.state == ConnectionState.HANDSHAKE:
            rawmsg = await self.compute_reader.read(1000)
            log.debug(f"[{self.conn_id}] C -> PS: handshake msg len {len(rawmsg)}")
            self.ps_writer.write(rawmsg)
            await self.ps_writer.drain()

        while True:
            msgtype = await self.compute_reader.readexactly(1)
            msglen_bytes = await self.compute_reader.readexactly(4)
            (msglen,) = struct.unpack("!L", msglen_bytes)
            payload = await self.compute_reader.readexactly(msglen - 4)

            # request_callback()
            # CopyData
            if msgtype == b"d":
                # TODO: call callback
                log.debug(f"[{self.conn_id}] C -> PS: CopyData ({msglen} bytes)")
                pass
            else:
                log.debug(f"[{self.conn_id}] C -> PS: message of type '{msgtype}' ({msglen} bytes)")

            self.ps_writer.write(msgtype)
            self.ps_writer.write(msglen_bytes)
            self.ps_writer.write(payload)
            await self.ps_writer.drain()

    async def handle_pageserver_to_compute(self):
        while True:
            msgtype = await self.ps_reader.readexactly(1)

            # response to SSLRequest
            if msgtype == b"N" and self.state == ConnectionState.HANDSHAKE:
                log.debug(f"[{self.conn_id}] PS -> C: N")
                self.compute_writer.write(msgtype)
                await self.compute_writer.drain()
                continue

            msglen_bytes = await self.ps_reader.readexactly(4)
            (msglen,) = struct.unpack("!L", msglen_bytes)
            payload = await self.ps_reader.readexactly(msglen - 4)

            # AuthenticationOK
            if msgtype == b"R":
                self.state = ConnectionState.AUTHOK
                log.debug(f"[{self.conn_id}] PS -> C: AuthenticationOK ({msglen} bytes)")

            # CopyBothresponse
            elif msgtype == b"W":
                self.state = ConnectionState.COPYBOTH
                log.debug(f"[{self.conn_id}] PS -> C: CopyBothResponse ({msglen} bytes)")

            # CopyData
            elif msgtype == b"d":
                log.debug(f"[{self.conn_id}] PS -> C: CopyData ({msglen} bytes)")
                if self.response_cb is not None:
                    await self.response_cb(self.conn_id)
                pass

            else:
                log.debug(f"[{self.conn_id}] PS -> C: message of type '{msgtype}' ({msglen} bytes)")

            self.compute_writer.write(msgtype)
            self.compute_writer.write(msglen_bytes)
            self.compute_writer.write(payload)
            await self.compute_writer.drain()


class PageserverProxy:
    def __init__(self, listen_port: int, dest_port: int, response_cb=None):
        self.listen_port = listen_port
        self.dest_port = dest_port
        self.response_cb = response_cb
        self.conn_counter = 0
        self.shutdown_event = asyncio.Event()

    def shutdown(self):
        self.serve_task.cancel()
        self.shutdown_event.set()

    async def handle_client(self, compute_reader, compute_writer):
        self.conn_counter += 1
        conn_id = self.conn_counter
        conn = Connection(
            conn_id,
            compute_reader,
            compute_writer,
            self.shutdown_event,
            self.dest_port,
            self.response_cb,
        )
        await conn.run()

    async def run_server(self):
        log.info("run_server called")
        server = await asyncio.start_server(self.handle_client, "localhost", self.listen_port)

        self.serve_task = asyncio.create_task(server.serve_forever())

        try:
            await self.serve_task
        except asyncio.CancelledError:
            log.info("proxy shutting down")

    def launch_server_in_thread(self):
        t1 = threading.Thread(target=asyncio.run, args=self.run_server)
        t1.start()
