"""Run a DAG on the CPI print itself, not on the scheduled minute.

A scheduled release time is a plan. Agencies run late, and a DAG that simply
starts at 08:30 either reads last month's figure or races the publisher. The
sensor below waits until an observation appears whose publication timestamp is
newer than the one already known, then hands the print straight to the next task
via XCom.

No API key is needed for USD. To reach the other seventeen currencies, create an
Airflow connection of type "fxmacrodata" and put the key in its password field.
"""

from __future__ import annotations

import datetime

from airflow.decorators import task
from airflow.models.dag import DAG

from fxmacrodata_provider.operators.fxmacrodata import FXMacroDataCalendarOperator
from fxmacrodata_provider.sensors.release import FXMacroDataReleaseSensor

with DAG(
    dag_id="fxmacrodata_wait_for_cpi",
    description="React to the US CPI print the moment it is published",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    schedule="30 12 * * *",
    catchup=False,
    tags=["fxmacrodata", "macro"],
) as dag:
    upcoming = FXMacroDataCalendarOperator(
        task_id="upcoming_releases",
        currency="USD",
        limit=10,
    )

    wait_for_cpi = FXMacroDataReleaseSensor(
        task_id="wait_for_cpi",
        currency="USD",
        indicator="inflation",
        # Poke rather than sleep: the figure lands when it lands.
        poke_interval=30,
        timeout=60 * 60 * 6,
        mode="reschedule",
    )

    @task
    def report(release: dict) -> str:
        """Do something with the print. Here, just describe it."""
        return (
            f"US CPI for {release['date']} came in at {release['val']}, "
            f"published {release.get('announcement_datetime_local')}"
        )

    upcoming >> wait_for_cpi >> report(wait_for_cpi.output)
