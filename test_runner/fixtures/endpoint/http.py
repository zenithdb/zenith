from __future__ import annotations

import urllib.parse

import requests
from requests.adapters import HTTPAdapter


class EndpointHttpClient(requests.Session):
    def __init__(
        self,
        external_port: int,
        internal_port: int,
    ):
        super().__init__()
        self.external_port: int = external_port
        self.internal_port: int = internal_port

        self.mount("http://", HTTPAdapter())

    def dbs_and_roles(self):
        res = self.get(f"http://localhost:{self.external_port}/dbs_and_roles")
        res.raise_for_status()
        return res.json()

    def database_schema(self, database: str):
        res = self.get(
            f"http://localhost:{self.external_port}/database_schema?database={urllib.parse.quote(database, safe='')}"
        )
        res.raise_for_status()
        return res.text

    def extensions(self, extension: str, version: str, database: str):
        body = {
            "extension": extension,
            "version": version,
            "database": database,
        }
        res = self.post(f"http://localhost:{self.internal_port}/extensions", json=body)
        res.raise_for_status()
        return res.json()

    def set_role_grants(self, database: str, role: str, schema: str, privileges: list[str]):
        res = self.post(
            f"http://localhost:{self.internal_port}/grants",
            json={"database": database, "schema": schema, "role": role, "privileges": privileges},
        )
        res.raise_for_status()
        return res.json()

    def metrics(self) -> str:
        res = self.get(f"http://localhost:{self.external_port}/metrics")
        res.raise_for_status()
        return res.text

    def configure_failpoints(self, *args: tuple[str, str]) -> None:
        body: list[dict[str, str]] = []

        for fp in args:
            body.append(
                {
                    "name": fp[0],
                    "action": fp[1],
                }
            )

        res = self.post(f"http://localhost:{self.internal_port}/failpoints", json=body)
        res.raise_for_status()
