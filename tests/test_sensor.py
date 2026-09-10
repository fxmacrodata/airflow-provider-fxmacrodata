"""Behaviour of the release sensor.

The sensor exists because a scheduled release time is a plan, not an event.
These tests pin the distinction it has to make: a genuinely new print versus the
figure that was already there, a revision to an older period, and a row with no
publication timestamp at all.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from airflow.exceptions import AirflowException

from fxmacrodata_provider.sensors.release import FXMacroDataReleaseSensor

OLD = 1_780_000_000
NEW = 1_790_000_000


def _row(announced: int | None, date: str = "2026-08-31", val: float = 3.4) -> dict:
    row: dict = {"date": date, "val": val}
    if announced is not None:
        row["announcement_datetime"] = announced
    return row


def _sensor(**kwargs) -> FXMacroDataReleaseSensor:
    kwargs.setdefault("task_id", "wait")
    kwargs.setdefault("currency", "USD")
    kwargs.setdefault("indicator", "inflation")
    return FXMacroDataReleaseSensor(**kwargs)


def _with_rows(*payloads):
    """Patch the hook so successive pokes see successive payloads."""
    return patch(
        "fxmacrodata_provider.sensors.release.FXMacroDataHook.run",
        side_effect=[{"data": p} for p in payloads],
    )


def test_first_poke_takes_a_baseline_and_waits():
    # Without an explicit baseline the sensor must not fire on whatever is
    # already published, or a DAG scheduled ahead of a release would succeed
    # immediately on last month's figure.
    sensor = _sensor()
    with _with_rows([_row(OLD)]):
        assert sensor.poke({}) is False
    assert sensor._baseline == OLD


def test_fires_once_a_newer_print_appears():
    sensor = _sensor()
    with _with_rows([_row(OLD)], [_row(NEW, date="2026-09-30")]):
        assert sensor.poke({}) is False
        with patch.object(sensor, "xcom_push"):
            assert sensor.poke({}) is True


def test_does_not_fire_on_the_same_print():
    sensor = _sensor(after_announcement_datetime=OLD)
    with _with_rows([_row(OLD)]):
        assert sensor.poke({}) is False


def test_explicit_baseline_fires_immediately_on_a_newer_print():
    sensor = _sensor(after_announcement_datetime=OLD)
    with _with_rows([_row(NEW)]), patch.object(sensor, "xcom_push"):
        assert sensor.poke({}) is True


def test_revision_to_an_older_period_is_not_the_awaited_release():
    # A publisher revising July while we wait for August republishes with a new
    # announcement timestamp. newer_than_period stops that counting.
    sensor = _sensor(after_announcement_datetime=OLD, newer_than_period="2026-08-31")
    with _with_rows([_row(NEW, date="2026-07-31")]):
        assert sensor.poke({}) is False


def test_period_guard_allows_the_real_release_through():
    sensor = _sensor(after_announcement_datetime=OLD, newer_than_period="2026-08-31")
    with _with_rows([_row(NEW, date="2026-09-30")]), patch.object(sensor, "xcom_push"):
        assert sensor.poke({}) is True


def test_empty_response_just_waits():
    sensor = _sensor(after_announcement_datetime=OLD)
    with _with_rows([]):
        assert sensor.poke({}) is False


def test_row_without_a_publication_timestamp_is_refused():
    # With no announcement_datetime there is no way to tell a new print from an
    # old one. Guessing here would silently fire a DAG on stale data.
    sensor = _sensor(after_announcement_datetime=OLD)
    with _with_rows([_row(None)]):
        with pytest.raises(AirflowException, match="announcement_datetime"):
            sensor.poke({})


def test_the_released_row_is_pushed_to_xcom():
    sensor = _sensor(after_announcement_datetime=OLD)
    released = _row(NEW, date="2026-09-30", val=3.9)
    with _with_rows([released]), patch.object(sensor, "xcom_push") as push:
        assert sensor.poke({}) is True
    # Downstream tasks should get the print itself, not have to re-fetch it.
    assert push.call_args.kwargs["value"] == released


def test_currency_is_lowercased_into_the_endpoint():
    sensor = _sensor(currency="JPY", indicator="policy_rate", after_announcement_datetime=OLD)
    with patch(
        "fxmacrodata_provider.sensors.release.FXMacroDataHook.run",
        return_value={"data": [_row(OLD)]},
    ) as run:
        sensor.poke({})
    assert run.call_args.args[0] == "announcements/jpy/policy_rate"
