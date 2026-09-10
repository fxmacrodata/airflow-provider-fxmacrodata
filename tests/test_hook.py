"""Behaviour of the hook, especially where credentials travel."""

from __future__ import annotations

import json
import urllib.error
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from fxmacrodata_provider.get_provider_info import get_provider_info
from fxmacrodata_provider.hooks.fxmacrodata import FXMacroDataHook

PAYLOAD = json.dumps({"currency": "USD", "data": []})


def _conn(password: str | None = None, host: str | None = None):
    conn = MagicMock()
    conn.password = password
    conn.host = host
    return conn


def _mock_urlopen(captured, body: str = PAYLOAD):
    def _open(request, timeout=None):
        captured["url"] = request.full_url
        captured["headers"] = dict(request.headers)
        response = MagicMock()
        response.read.return_value = body.encode("utf-8")
        response.__enter__ = lambda self: self
        response.__exit__ = lambda self, *args: None
        return response

    return _open


def test_api_key_is_a_header_not_a_query_parameter():
    # A key in the query string would be written to proxy logs, server access
    # logs and the URL shown in the Airflow task log.
    hook = FXMacroDataHook()
    captured: dict = {}
    with patch.object(FXMacroDataHook, "get_connection", return_value=_conn("secret-key")):
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen(captured)):
            hook.run("market_sessions")

    headers = {k.lower(): v for k, v in captured["headers"].items()}
    assert headers["x-api-key"] == "secret-key"
    assert "secret-key" not in captured["url"]


def test_missing_connection_still_allows_public_access():
    # USD data is public, so an absent connection is a normal setup, not a failure.
    hook = FXMacroDataHook()
    captured: dict = {}
    with patch.object(FXMacroDataHook, "get_connection", side_effect=Exception("no such conn")):
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen(captured)):
            hook.run("announcements/usd/latest")

    assert "x-api-key" not in {k.lower() for k in captured["headers"]}
    assert hook.has_api_key is False


def test_connection_host_overrides_the_base_url():
    hook = FXMacroDataHook()
    captured: dict = {}
    with (
        patch.object(
            FXMacroDataHook, "get_connection", return_value=_conn(None, "https://example.test/v1/")
        ),
        patch("urllib.request.urlopen", side_effect=_mock_urlopen(captured)),
    ):
        hook.run("ping")

    assert captured["url"] == "https://example.test/v1/ping"


def test_non_http_base_url_is_refused():
    # The base URL comes from an operator-editable connection field, so a
    # file:// host must never reach urlopen.
    hook = FXMacroDataHook()
    with patch.object(FXMacroDataHook, "get_connection", return_value=_conn(None, "file:///etc")):
        with pytest.raises(AirflowException, match="http or https"):
            hook.run("ping")


def test_none_params_are_dropped():
    hook = FXMacroDataHook()
    captured: dict = {}
    with patch.object(FXMacroDataHook, "get_connection", return_value=_conn()):
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen(captured)):
            hook.run("announcements/usd/gdp", {"limit": 5, "start_date": None})

    assert "limit=5" in captured["url"]
    assert "start_date" not in captured["url"]


def test_auth_failure_names_the_connection_field_to_fix():
    hook = FXMacroDataHook()
    error = urllib.error.HTTPError("url", 403, "Forbidden", {}, None)
    with patch.object(FXMacroDataHook, "get_connection", return_value=_conn()):
        with patch("urllib.request.urlopen", side_effect=error):
            with pytest.raises(AirflowException, match="password field"):
                hook.run("cot/gbp")


def test_server_error_is_not_reported_as_an_auth_problem():
    hook = FXMacroDataHook()
    error = urllib.error.HTTPError("url", 500, "Server Error", {}, None)
    with patch.object(FXMacroDataHook, "get_connection", return_value=_conn()):
        with patch("urllib.request.urlopen", side_effect=error):
            with pytest.raises(AirflowException, match="HTTP 500"):
                hook.run("commodities/latest")


def test_invalid_json_is_reported():
    hook = FXMacroDataHook()
    with patch.object(FXMacroDataHook, "get_connection", return_value=_conn()):
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen({}, "<html>")):
            with pytest.raises(AirflowException, match="not valid JSON"):
                hook.run("ping")


def test_provider_info_declares_the_hook_and_sensor():
    info = get_provider_info()
    assert info["package-name"] == "airflow-provider-fxmacrodata"
    assert (
        info["connection-types"][0]["hook-class-name"]
        == "fxmacrodata_provider.hooks.fxmacrodata.FXMacroDataHook"
    )
    assert "fxmacrodata_provider.sensors.release" in info["sensors"][0]["python-modules"]
