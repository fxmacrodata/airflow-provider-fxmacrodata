"""Sensors that wait for a macroeconomic release to actually land."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator

from fxmacrodata_provider.hooks.fxmacrodata import FXMacroDataHook


class FXMacroDataReleaseSensor(BaseSensorOperator):
    """
    Wait until a macroeconomic figure has actually been published.

    A scheduled release time is a plan, not an event. Statistical agencies run
    late, and a DAG that simply starts at the scheduled minute either reads the
    previous month's figure or races the publisher. This sensor pokes the
    indicator until an observation appears whose publication timestamp is newer
    than the one already known, so downstream tasks run on the print itself
    rather than on the clock.

    The value it returns via XCom is the new observation, so a downstream task
    does not have to fetch it again::

        wait = FXMacroDataReleaseSensor(
            task_id="wait_for_cpi",
            currency="USD",
            indicator="inflation",
            poke_interval=30,
            timeout=60 * 60 * 6,
        )

    :param currency: Three-letter currency code, e.g. ``USD``.
    :param indicator: Indicator slug, e.g. ``inflation`` or ``non_farm_payrolls``.
        Use ``/v1/data_catalogue/{currency}`` to discover valid slugs.
    :param after_announcement_datetime: Only treat an observation as new if it was
        published strictly after this Unix timestamp. When omitted, the sensor
        records the newest timestamp it sees on its first poke and waits for
        something newer, which is what a DAG scheduled ahead of a release wants.
    :param newer_than_period: Optionally require the new observation to cover a
        period strictly after this ISO date, guarding against a revision to an
        older period being mistaken for the awaited release.
    :param conn_id: Airflow connection ID holding the API key, if one is needed.
    """

    template_fields: Sequence[str] = (
        "currency",
        "indicator",
        "newer_than_period",
    )
    ui_color = "#00e5ff"

    def __init__(
        self,
        *,
        currency: str,
        indicator: str,
        after_announcement_datetime: int | None = None,
        newer_than_period: str | None = None,
        conn_id: str = FXMacroDataHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.currency = currency
        self.indicator = indicator
        self.after_announcement_datetime = after_announcement_datetime
        self.newer_than_period = newer_than_period
        self.conn_id = conn_id
        self._baseline: int | None = after_announcement_datetime

    def _hook(self) -> FXMacroDataHook:
        return FXMacroDataHook(conn_id=self.conn_id)

    def poke(self, context: Any) -> bool:
        payload = self._hook().run(
            f"announcements/{self.currency.lower()}/{self.indicator}",
            {"limit": 1},
        )
        rows = payload.get("data") if isinstance(payload, dict) else None
        if not rows:
            self.log.info(
                "FXMacroData returned no rows yet for %s/%s.",
                self.currency,
                self.indicator,
            )
            return False

        latest = rows[0]
        announced = latest.get("announcement_datetime")
        if not isinstance(announced, int):
            # Without a publication timestamp there is no way to tell a new print
            # from an old one, so refuse rather than guess.
            raise AirflowException(
                f"FXMacroData row for {self.currency}/{self.indicator} carries no "
                "announcement_datetime; cannot decide whether it is newly released."
            )

        if self._baseline is None:
            # First poke with no explicit baseline: remember what was already
            # published and wait for something newer.
            self._baseline = announced
            self.log.info(
                "Baseline for %s/%s is the print published at %s; waiting for a newer one.",
                self.currency,
                self.indicator,
                latest.get("announcement_datetime_local") or announced,
            )
            return False

        if announced <= self._baseline:
            self.log.info(
                "%s/%s not published yet (newest is still %s).",
                self.currency,
                self.indicator,
                latest.get("announcement_datetime_local") or announced,
            )
            return False

        if self.newer_than_period and str(latest.get("date", "")) <= self.newer_than_period:
            # A revision to an older period is not the release we are waiting for.
            self.log.info(
                "%s/%s published at %s covers period %s, which is not after %s; still waiting.",
                self.currency,
                self.indicator,
                latest.get("announcement_datetime_local") or announced,
                latest.get("date"),
                self.newer_than_period,
            )
            return False

        self.log.info(
            "%s/%s released: period %s, value %s, published %s.",
            self.currency,
            self.indicator,
            latest.get("date"),
            latest.get("val"),
            latest.get("announcement_datetime_local") or announced,
        )
        self.xcom_push(context, key="return_value", value=latest)
        return True
