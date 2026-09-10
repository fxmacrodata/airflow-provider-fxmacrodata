"""Operators for pulling FXMacroData datasets into a DAG."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from airflow.models import BaseOperator

from fxmacrodata_provider.hooks.fxmacrodata import FXMacroDataHook


class FXMacroDataOperator(BaseOperator):
    """
    Fetch one FXMacroData endpoint and return its decoded JSON via XCom.

    This is the general escape hatch: any endpoint, any parameters. For the
    common cases prefer :class:`FXMacroDataSnapshotOperator` or
    :class:`FXMacroDataCalendarOperator`, which name what they do.

    :param endpoint: Path relative to the API base URL, e.g.
        ``announcements/usd/inflation``.
    :param params: Optional query parameters. ``None`` values are dropped.
    :param conn_id: Airflow connection ID holding the API key, if one is needed.
    """

    template_fields: Sequence[str] = ("endpoint", "params")
    ui_color = "#00e5ff"

    def __init__(
        self,
        *,
        endpoint: str,
        params: dict[str, Any] | None = None,
        conn_id: str = FXMacroDataHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.params = params
        self.conn_id = conn_id

    def execute(self, context: Any) -> Any:
        return FXMacroDataHook(conn_id=self.conn_id).run(self.endpoint, self.params)


class FXMacroDataSnapshotOperator(BaseOperator):
    """
    Fetch the latest value of every indicator for a currency, in one request.

    This is the aggregation the API exists for: one call returns the most recent
    print of each indicator for an economy, with the instant each was published,
    rather than one request per series.

    :param currency: Three-letter currency code, e.g. ``USD``.
    :param conn_id: Airflow connection ID holding the API key, if one is needed.
    """

    template_fields: Sequence[str] = ("currency",)
    ui_color = "#00e5ff"

    def __init__(
        self,
        *,
        currency: str = "USD",
        conn_id: str = FXMacroDataHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.currency = currency
        self.conn_id = conn_id

    def execute(self, context: Any) -> Any:
        return FXMacroDataHook(conn_id=self.conn_id).run(
            f"announcements/{self.currency.lower()}/latest"
        )


class FXMacroDataCalendarOperator(BaseOperator):
    """
    Fetch the upcoming scheduled release calendar for a currency.

    Useful for deciding what a downstream branch should wait on, or for keeping a
    local schedule table in step with the publishers'.

    :param currency: Three-letter currency code, e.g. ``USD``.
    :param limit: Maximum releases to return. The API caps this at 100.
    :param conn_id: Airflow connection ID holding the API key, if one is needed.
    """

    template_fields: Sequence[str] = ("currency",)
    ui_color = "#00e5ff"

    def __init__(
        self,
        *,
        currency: str = "USD",
        limit: int = 50,
        conn_id: str = FXMacroDataHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.currency = currency
        self.limit = limit
        self.conn_id = conn_id

    def execute(self, context: Any) -> Any:
        return FXMacroDataHook(conn_id=self.conn_id).run(
            f"calendar/{self.currency.lower()}", {"limit": self.limit}
        )
