"""Provider metadata read by Airflow at startup."""

from __future__ import annotations

from typing import Any


def get_provider_info() -> dict[str, Any]:
    """Describe this provider to Airflow's provider manager."""
    return {
        "package-name": "airflow-provider-fxmacrodata",
        "name": "FXMacroData",
        "description": (
            "Official-source macroeconomic, FX and central-bank data for 18 "
            "currencies, with a sensor that waits for a release to actually land."
        ),
        "versions": ["0.1.0"],
        "connection-types": [
            {
                "connection-type": "fxmacrodata",
                "hook-class-name": "fxmacrodata_provider.hooks.fxmacrodata.FXMacroDataHook",
            }
        ],
        "hooks": [
            {
                "integration-name": "FXMacroData",
                "python-modules": ["fxmacrodata_provider.hooks.fxmacrodata"],
            }
        ],
        "operators": [
            {
                "integration-name": "FXMacroData",
                "python-modules": ["fxmacrodata_provider.operators.fxmacrodata"],
            }
        ],
        "sensors": [
            {
                "integration-name": "FXMacroData",
                "python-modules": ["fxmacrodata_provider.sensors.release"],
            }
        ],
        "integrations": [
            {
                "integration-name": "FXMacroData",
                "external-doc-url": (
                    "https://fxmacrodata.com/documentation?utm_source=airflow"
                    "&utm_medium=integration&utm_campaign=airflow-provider-fxmacrodata"
                    "&utm_content=docs"
                ),
                "tags": ["service"],
            }
        ],
    }
