# airflow-provider-fxmacrodata

An Apache Airflow provider for [FXMacroData](https://fxmacrodata.com): official-source
macroeconomic, FX and central-bank data for 18 currencies — with a sensor that waits for a
release to *actually land* rather than for the clock to strike.

## Why a sensor

A scheduled release time is a plan, not an event. Statistical agencies run late. A DAG that
starts at 08:30 because CPI is "due at 08:30" either reads last month's figure or races the
publisher, and both failures are quiet.

`FXMacroDataReleaseSensor` pokes the indicator until an observation appears whose
**publication timestamp** is newer than the one already known, then pushes that observation
to XCom so the downstream task does not have to fetch it again.

```python
wait_for_cpi = FXMacroDataReleaseSensor(
    task_id="wait_for_cpi",
    currency="USD",
    indicator="inflation",
    poke_interval=30,
    timeout=60 * 60 * 6,
    mode="reschedule",
)
```

It distinguishes three things that look alike from the outside:

- the figure that was **already** published when the DAG started,
- a **revision to an older period** republished while you wait (guard with `newer_than_period`),
- the **release you are actually waiting for**.

A row with no publication timestamp raises rather than guessing — firing a DAG on stale data
silently is worse than failing.

## Installation

```bash
pip install airflow-provider-fxmacrodata
```

Airflow discovers the provider automatically through the `apache_airflow_provider` entry
point: the connection type, hook, operators and sensor all register at startup.

## Authentication

**USD data is public — the provider works with no connection at all.**

For the other seventeen currencies, FX rates, rate differentials, COT positioning and
commodities, create an Airflow connection:

| Field | Value |
| --- | --- |
| Connection Id | `fxmacrodata_default` |
| Connection Type | `FXMacroData` |
| API key | your key (stored in the connection's `password`) |
| API base URL | optional override |

The key lives in Airflow's connection store and is sent as an `X-API-Key` **header**, so it
never appears in a DAG file, a URL, a proxy log or the task log.

## Components

| Component | Purpose |
| --- | --- |
| `FXMacroDataReleaseSensor` | Wait until an indicator is actually published |
| `FXMacroDataSnapshotOperator` | Latest value of **every** indicator for a currency, in one request |
| `FXMacroDataCalendarOperator` | Upcoming scheduled releases |
| `FXMacroDataOperator` | Any endpoint, any parameters |
| `FXMacroDataHook` | Direct API access from your own task |

`FXMacroDataSnapshotOperator` is the aggregation the API exists for: one call returns the
most recent print of each indicator for an economy, with the instant each was published,
instead of one request per series.

## Example

A complete DAG is in [`example_dags/example_wait_for_cpi.py`](example_dags/example_wait_for_cpi.py).

## Development

```bash
pip install -e ".[dev]"
pytest
ruff check .
```

## License

Apache 2.0.
