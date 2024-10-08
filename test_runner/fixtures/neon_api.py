from __future__ import annotations

import time
from typing import TYPE_CHECKING, cast

import requests

from fixtures.log_helper import log

if TYPE_CHECKING:
    from typing import Any, Dict, Literal, Optional, Union

    from fixtures.pg_version import PgVersion


def connection_parameters_to_env(params: Dict[str, str]) -> Dict[str, str]:
    return {
        "PGHOST": params["host"],
        "PGDATABASE": params["database"],
        "PGUSER": params["role"],
        "PGPASSWORD": params["password"],
    }


class NeonAPI:
    def __init__(self, neon_api_key: str, neon_api_base_url: str):
        self.__neon_api_key = neon_api_key
        self.__neon_api_base_url = neon_api_base_url.strip("/")

    def __request(
        self, method: Union[str, bytes], endpoint: str, **kwargs: Any
    ) -> requests.Response:
        if "headers" not in kwargs:
            kwargs["headers"] = {}
        kwargs["headers"]["Authorization"] = f"Bearer {self.__neon_api_key}"

        resp = requests.request(method, f"{self.__neon_api_base_url}{endpoint}", **kwargs)
        if resp.status_code < 400:
            return resp

        if resp.headers["Content-Type"] == "application/json":
            data = resp.json()
        else:
            data = resp.text

        log.error("%s %s failed (%d): %s", method, endpoint, resp.status_code, data)
        resp.raise_for_status()

        return resp

    def create_project(
        self,
        pg_version: Optional[PgVersion] = None,
        name: Optional[str] = None,
        branch_name: Optional[str] = None,
        branch_role_name: Optional[str] = None,
        branch_database_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "project": {
                "branch": {},
            },
        }
        if name:
            data["project"]["name"] = name
        if pg_version:
            data["project"]["pg_version"] = int(pg_version)
        if branch_name:
            data["project"]["branch"]["name"] = branch_name
        if branch_role_name:
            data["project"]["branch"]["role_name"] = branch_role_name
        if branch_database_name:
            data["project"]["branch"]["database_name"] = branch_database_name

        resp = self.__request(
            "POST",
            "/projects",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=data,
        )

        return cast("Dict[str, Any]", resp.json())

    def get_project_details(self, project_id: str) -> Dict[str, Any]:
        resp = self.__request(
            "GET",
            f"/projects/{project_id}",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )

        return cast("Dict[str, Any]", resp.json())

    def delete_project(
        self,
        project_id: str,
    ) -> Dict[str, Any]:
        resp = self.__request(
            "DELETE",
            f"/projects/{project_id}",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )

        return cast("Dict[str, Any]", resp.json())

    def start_endpoint(
        self,
        project_id: str,
        endpoint_id: str,
    ) -> Dict[str, Any]:
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/endpoints/{endpoint_id}/start",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("Dict[str, Any]", resp.json())

    def suspend_endpoint(
        self,
        project_id: str,
        endpoint_id: str,
    ) -> Dict[str, Any]:
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/endpoints/{endpoint_id}/suspend",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("Dict[str, Any]", resp.json())

    def restart_endpoint(
        self,
        project_id: str,
        endpoint_id: str,
    ) -> Dict[str, Any]:
        resp = self.__request(
            "POST",
            f"/projects/{project_id}/endpoints/{endpoint_id}/restart",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("Dict[str, Any]", resp.json())

    def create_endpoint(
        self,
        project_id: str,
        branch_id: str,
        endpoint_type: Literal["read_write", "read_only"],
        settings: Dict[str, Any],
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "endpoint": {
                "branch_id": branch_id,
            },
        }

        if endpoint_type:
            data["endpoint"]["type"] = endpoint_type
        if settings:
            data["endpoint"]["settings"] = settings

        resp = self.__request(
            "POST",
            f"/projects/{project_id}/endpoints",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=data,
        )

        return cast("Dict[str, Any]", resp.json())

    def get_connection_uri(
        self,
        project_id: str,
        branch_id: Optional[str] = None,
        endpoint_id: Optional[str] = None,
        database_name: str = "neondb",
        role_name: str = "neondb_owner",
        pooled: bool = True,
    ) -> Dict[str, Any]:
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/connection_uri",
            params={
                "branch_id": branch_id,
                "endpoint_id": endpoint_id,
                "database_name": database_name,
                "role_name": role_name,
                "pooled": pooled,
            },
            headers={
                "Accept": "application/json",
            },
        )

        return cast("Dict[str, Any]", resp.json())

    def get_branches(self, project_id: str) -> Dict[str, Any]:
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/branches",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("Dict[str, Any]", resp.json())

    def get_endpoints(self, project_id: str) -> Dict[str, Any]:
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/endpoints",
            headers={
                "Accept": "application/json",
            },
        )

        return cast("Dict[str, Any]", resp.json())

    def get_operations(self, project_id: str) -> Dict[str, Any]:
        resp = self.__request(
            "GET",
            f"/projects/{project_id}/operations",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.__neon_api_key}",
            },
        )

        return cast("Dict[str, Any]", resp.json())

    def wait_for_operation_to_finish(self, project_id: str):
        has_running = True
        while has_running:
            has_running = False
            operations = self.get_operations(project_id)["operations"]
            for op in operations:
                if op["status"] in {"scheduling", "running", "cancelling"}:
                    has_running = True
            time.sleep(0.5)


class NeonApiEndpoint:
    def __init__(self, neon_api: NeonAPI, pg_version: PgVersion, project_id: Optional[str]):
        self.neon_api = neon_api
        if project_id is None:
            project = neon_api.create_project(pg_version)
            neon_api.wait_for_operation_to_finish(project["project"]["id"])
            self.project_id = project["project"]["id"]
            self.endpoint_id = project["endpoints"][0]["id"]
            self.connstr = project["connection_uris"][0]["connection_uri"]
            self.pgbench_env = connection_parameters_to_env(
                project["connection_uris"][0]["connection_parameters"]
            )
            self.is_new = True
        else:
            project = neon_api.get_project_details(project_id)
            if int(project["project"]["pg_version"]) != int(pg_version):
                raise Exception(
                    f"A project with the provided ID exists, but it's not of the specified version (expected {pg_version}, got {project['project']['pg_version']})"
                )
            self.project_id = project_id
            eps = neon_api.get_endpoints(project_id)["endpoints"]
            self.endpoint_id = eps[0]["id"]
            self.connstr = neon_api.get_connection_uri(
                project_id, endpoint_id=self.endpoint_id, pooled=False
            )["uri"]
            pw = self.connstr.split("@")[0].split(":")[-1]
            self.pgbench_env = {
                "PGHOST": eps[0]["host"],
                "PGDATABASE": "neondb",
                "PGUSER": "neondb_owner",
                "PGPASSWORD": pw,
            }
            self.is_new = False

    def restart(self):
        self.neon_api.restart_endpoint(self.project_id, self.endpoint_id)
        self.neon_api.wait_for_operation_to_finish(self.project_id)

    def get_synthetic_storage_size(self) -> int:
        return int(
            self.neon_api.get_project_details(self.project_id)["project"]["synthetic_storage_size"]
        )
