# -*- coding: utf-8 -*-
"""Shared AUTO network policy for collector REST and WebSocket transports."""

from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit
from urllib.request import ProxyHandler, Request, build_opener, getproxies, proxy_bypass


MODE_DIRECT = "DIRECT"
MODE_SYSTEM_PROXY = "SYSTEM_PROXY"


def network_routes(url, *, proxies_fn=None, bypass_fn=None):
    """Resolve the current Windows/environment proxy on every call.

    AUTO means: use the current system proxy first when configured, then try
    direct. Re-evaluating per request/connection lets a live collector recover
    after Clash/TUN/system-proxy changes without a process restart.
    """
    parsed_url = urlsplit(url)
    host = parsed_url.hostname or ""
    proxies_fn = proxies_fn or getproxies
    bypass_fn = bypass_fn or proxy_bypass

    try:
        if host and bypass_fn(host):
            return [{"mode": MODE_DIRECT}]
        proxies = proxies_fn() or {}
    except OSError:
        return [{"mode": MODE_DIRECT}]

    keys = ("https", "http") if parsed_url.scheme in ("https", "wss") else ("http",)
    proxy_url = next((proxies.get(key) for key in keys if proxies.get(key)), None)
    if not proxy_url:
        return [{"mode": MODE_DIRECT}]
    if "://" not in proxy_url:
        proxy_url = "http://" + proxy_url

    parsed_proxy = urlsplit(proxy_url)
    if not parsed_proxy.hostname:
        return [{"mode": MODE_DIRECT}]
    proxy_type = parsed_proxy.scheme.lower()
    if proxy_type not in ("http", "socks4", "socks4a", "socks5", "socks5h"):
        return [{"mode": MODE_DIRECT}]
    default_port = 1080 if proxy_type.startswith("socks") else 80
    auth = None
    if parsed_proxy.username is not None:
        auth = (unquote(parsed_proxy.username), unquote(parsed_proxy.password or ""))

    return [{
        "mode": MODE_SYSTEM_PROXY,
        "proxy_url": proxy_url,
        "proxy_host": parsed_proxy.hostname,
        "proxy_port": parsed_proxy.port or default_port,
        "proxy_type": proxy_type,
        "proxy_auth": auth,
    }, {"mode": MODE_DIRECT}]


def route_evidence(route):
    """Return the persistable route fields; credentials never leave memory."""
    return {
        "effective_network_mode": route["mode"],
        "proxy_host": route.get("proxy_host"),
        "proxy_port": route.get("proxy_port"),
        "proxy_type": route.get("proxy_type"),
    }


def websocket_options(route):
    if route["mode"] == MODE_DIRECT:
        return {"http_no_proxy": ["*"]}
    options = {
        "http_proxy_host": route["proxy_host"],
        "http_proxy_port": route["proxy_port"],
        "proxy_type": route["proxy_type"],
        # A nonempty list prevents websocket-client from silently consulting a
        # conflicting NO_PROXY environment value for this explicit route.
        "http_no_proxy": [""],
    }
    if route.get("proxy_auth") is not None:
        options["http_proxy_auth"] = route["proxy_auth"]
    return options


def _open_route(request, timeout, route):
    scheme = urlsplit(request.full_url).scheme
    proxies = {} if route["mode"] == MODE_DIRECT else {scheme: route["proxy_url"]}
    return build_opener(ProxyHandler(proxies)).open(request, timeout=timeout)


def open_url(
    request,
    *,
    timeout,
    proxies_fn=None,
    bypass_fn=None,
    open_route_fn=None,
    on_event=None,
):
    """Open one REST URL with the same AUTO policy used by WebSockets."""
    request = Request(request) if isinstance(request, str) else request
    open_route_fn = open_route_fn or _open_route
    on_event = on_event or (lambda _event: None)
    last_error = None

    for route in network_routes(
        request.full_url, proxies_fn=proxies_fn, bypass_fn=bypass_fn,
    ):
        evidence = route_evidence(route)
        on_event({"event_type": "CONNECTIVITY_ATTEMPT", **evidence})
        try:
            response = open_route_fn(request, timeout, route)
        except HTTPError:
            # An HTTP response, including an error status, proves the route
            # connected successfully. Preserve normal caller error handling.
            on_event({"event_type": "CONNECTIVITY_RESTORED", **evidence})
            raise
        except URLError as error:
            last_error = error
            on_event({
                "event_type": "CONNECTIVITY_LOST",
                **evidence,
                "failure_exception_type": type(error).__name__,
            })
            continue
        on_event({"event_type": "CONNECTIVITY_RESTORED", **evidence})
        return response

    if last_error is not None:
        raise URLError("ALL_NETWORK_ROUTES_FAILED") from last_error
    raise URLError("NO_NETWORK_ROUTE_AVAILABLE")
