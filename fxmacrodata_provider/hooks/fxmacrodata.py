"""Hook for the FXMacroData REST API."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

DEFAULT_BASE_URL = "https://api.fxmacrodata.com/v1"
DEFAULT_TIMEOUT = 30


class FXMacroDataHook(BaseHook):
    """
    Interact with the FXMacroData REST API.

    FXMacroData aggregates official publishers - statistical agencies, central
    banks and exchanges - across 18 currencies behind one contract, and stamps
    every observation with the instant it was published.

    USD data is public, so this hook works with no connection configured at all.
    To reach the other seventeen currencies, FX rates, rate differentials, COT
    positioning and commodities, create an Airflow connection and put the API key
    in its ``password`` field. The key is never read from the DAG file.

    :param conn_id: Airflow connection ID. The connection's ``password`` is used
        as the API key and its ``host`` may override the API base URL. Missing
        connections are tolerated so that public USD data still works.
    :param timeout: Per-request HTTP timeout in seconds.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "fxmacrodata_default"
    conn_type = "fxmacrodata"
    hook_name = "FXMacroData"

    def __init__(
        self,
        conn_id: str = default_conn_name,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.timeout = timeout
        self._api_key: str | None = None
        self._base_url: str = DEFAULT_BASE_URL
        self._resolved = False

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Render only the fields this connection actually uses."""
        return {
            "hidden_fields": ["schema", "port", "login", "extra"],
            "relabeling": {
                "host": "API base URL (optional)",
                "password": "API key (optional - USD data is public)",
            },
            "placeholders": {
                "host": DEFAULT_BASE_URL,
                "password": "leave blank for public USD data",
            },
        }

    def _resolve_connection(self) -> None:
        """Read the connection once, tolerating its absence."""
        if self._resolved:
            return
        self._resolved = True
        try:
            conn = self.get_connection(self.conn_id)
        except Exception:  # noqa: BLE001 - Airflow raises different types per version
            # No connection configured. Public USD endpoints still work, so this
            # is a normal setup rather than an error.
            self.log.info(
                "No %r connection found; continuing with public FXMacroData access. "
                "Add one with the API key in its password field to reach the other "
                "currencies and market data.",
                self.conn_id,
            )
            return
        self._api_key = conn.password or None
        if conn.host:
            self._base_url = str(conn.host).rstrip("/")

    @property
    def base_url(self) -> str:
        """The API base URL in force for this hook."""
        self._resolve_connection()
        return self._base_url

    @property
    def has_api_key(self) -> bool:
        """Whether an API key is configured, without exposing the key itself."""
        self._resolve_connection()
        return self._api_key is not None

    def run(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """
        Call an FXMacroData endpoint and return the decoded JSON body.

        :param endpoint: Path relative to the API base URL, e.g.
            ``announcements/usd/inflation``.
        :param params: Optional query parameters. ``None`` values are dropped.
        :raises AirflowException: on transport failure, a non-2xx response or a
            body that is not valid JSON.
        """
        self._resolve_connection()

        clean = {k: v for k, v in (params or {}).items() if v is not None}
        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        if clean:
            url = f"{url}?{urllib.parse.urlencode(clean)}"

        # The base URL is operator-settable via the connection host, so pin the
        # scheme: urlopen would otherwise honour file:// and read local files.
        if urllib.parse.urlparse(url).scheme not in ("http", "https"):
            raise AirflowException(
                f"FXMacroData base URL must be http or https, got {self._base_url!r}"
            )

        headers = {"Accept": "application/json"}
        if self._api_key:
            # Header rather than a query parameter, so the key is not written to
            # proxy logs, server access logs or the Airflow task log URL.
            headers["X-API-Key"] = self._api_key

        request = urllib.request.Request(url, headers=headers)  # noqa: S310
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:  # noqa: S310
                payload = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            if exc.code in (401, 403):
                raise AirflowException(
                    f"FXMacroData denied {endpoint} (HTTP {exc.code}). That data needs an "
                    f"API key; set one in the {self.conn_id!r} connection's password field. "
                    "USD macro data is available without a key."
                ) from None
            raise AirflowException(
                f"FXMacroData request to {endpoint} failed with HTTP {exc.code}."
            ) from None
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise AirflowException(f"FXMacroData request to {endpoint} failed: {exc}") from None

        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            raise AirflowException(
                f"FXMacroData response from {endpoint} was not valid JSON."
            ) from None

    def test_connection(self) -> tuple[bool, str]:
        """Validate the connection from the Airflow UI."""
        try:
            self.run("ping")
        except AirflowException as exc:
            return False, str(exc)
        scope = "authenticated" if self.has_api_key else "public USD only"
        return True, f"Connected to FXMacroData ({scope})."
