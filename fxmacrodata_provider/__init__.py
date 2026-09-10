"""Apache Airflow provider for FXMacroData."""

__version__ = "0.1.0"


def get_provider_info():
    """Re-exported so Airflow can find the metadata from the package root."""
    from fxmacrodata_provider.get_provider_info import get_provider_info as _info

    return _info()
