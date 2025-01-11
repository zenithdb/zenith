#!/usr/bin/env python3
#
# This program helps to test the WebSocket tunneling in proxy. It listens for a TCP
# connection on a port, and when you connect to it, it opens a websocket connection,
# and forwards all the traffic to the websocket connection, wrapped in WebSocket binary
# frames.
#
# This is used in the test_proxy::test_websockets test, but it is handy for manual testing too.
#
# Usage for manual testing:
#
# ## Launch Posgres on port 3000:
# postgres -D data -p3000
#
# ## Launch proxy with WSS enabled:
# openssl req -new -x509 -days 365 -nodes -text -out server.crt -keyout server.key -subj '/CN=*.neon.localtest.me'
# ./target/debug/proxy --wss 127.0.0.1:40433 --http 127.0.0.1:28080 --mgmt 127.0.0.1:9099 --proxy 127.0.0.1:4433 --tls-key server.key --tls-cert server.crt --auth-backend postgres
#
# ## Launch the tunnel:
#
# poetry run ./test_runner/websocket_tunnel.py --ws-port 40433 --ws-url "wss://ep-test.neon.localtest.me"
#
# ## Now you can connect with psql:
# psql "postgresql://heikki@localhost:40433/postgres"
#

import argparse
import asyncio
import logging
import ssl
from ssl import Purpose

import websockets
from fixtures.log_helper import log

# Enable verbose logging of all the traffic


def enable_verbose_logging():
    logger = logging.getLogger("websockets")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())


async def start_server(tcp_listen_host, tcp_listen_port, ws_url, ctx):
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, ws_url, ctx), tcp_listen_host, tcp_listen_port
    )
    return server


async def handle_tcp_to_websocket(tcp_reader, ws):
    try:
        while not tcp_reader.at_eof():
            data = await tcp_reader.read(1024)

            await ws.send(data)
    except websockets.exceptions.ConnectionClosedError as e:
        log.debug(f"connection closed: {e}")
    except websockets.exceptions.ConnectionClosedOK:
        log.debug("connection closed")
    except Exception as e:
        log.error(e)


async def handle_websocket_to_tcp(ws, tcp_writer):
    try:
        async for message in ws:
            tcp_writer.write(message)
            await tcp_writer.drain()
    except websockets.exceptions.ConnectionClosedError as e:
        log.debug(f"connection closed: {e}")
    except websockets.exceptions.ConnectionClosedOK:
        log.debug("connection closed")
    except Exception as e:
        log.error(e)


async def handle_client(tcp_reader, tcp_writer, ws_url: str, ctx: ssl.SSLContext):
    try:
        log.info("Received TCP connection. Connecting to websockets proxy.")

        async with websockets.connect(ws_url, ssl=ctx) as ws:
            try:
                log.info("Connected to websockets proxy")

                async with asyncio.TaskGroup() as tg:
                    task1 = tg.create_task(handle_tcp_to_websocket(tcp_reader, ws))
                    task2 = tg.create_task(handle_websocket_to_tcp(ws, tcp_writer))

                    done, pending = await asyncio.wait(
                        [task1, task2], return_when=asyncio.FIRST_COMPLETED
                    )
                    tcp_writer.close()
                    await ws.close()

            except* Exception as ex:
                log.error(ex.exceptions)
    except Exception as e:
        log.error(e)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tcp-listen-addr",
        default="localhost",
        help="TCP addr to listen on",
    )
    parser.add_argument(
        "--tcp-listen-port",
        default="40444",
        help="TCP port to listen on",
    )

    parser.add_argument(
        "--ws-url",
        default="wss://localhost/",
        help="websocket URL to connect to. This determines the Host header sent to the server",
    )
    parser.add_argument(
        "--ws-host",
        default="127.0.0.1",
        help="websockets host to connect to",
    )
    parser.add_argument(
        "--ws-port",
        type=int,
        default=443,
        help="websockets port to connect to",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="enable verbose logging",
    )
    args = parser.parse_args()

    if args.verbose:
        enable_verbose_logging()

    ctx = ssl.create_default_context(Purpose.SERVER_AUTH)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    server = await start_server(args.tcp_listen_addr, args.tcp_listen_port, args.ws_url, ctx)
    print(
        f"Listening for connections at {args.tcp_listen_addr}:{args.tcp_listen_port}, forwarding them to {args.ws_host}:{args.ws_port}"
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
