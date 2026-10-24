"""Polite HTTP for NSE and GitHub: per-host concurrency caps, adaptive pacing and back-off.

Ban avoidance, not evasion:
* every host has a hard concurrency cap and a requests-per-second ceiling;
* pacing starts conservatively and only speeds up while responses stay clean (AIMD);
* any 403/429/503 halves the rate, pauses that host with growing cool-downs and, for the
  NSE website API, re-establishes the browser session cookies;
* repeated blocks trip a circuit breaker (:class:`HostBlocked`) so the step stops cleanly
  and can be resumed later instead of hammering a host that is refusing us.
No proxies, IP rotation or CAPTCHA tricks are used.
"""
from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx

BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/129.0 Safari/537.36")
NSE_HOME = "https://www.nseindia.com"
BLOCK_CODES = {401, 403, 429, 503}
RETRY_CODES = {500, 502, 504}


class HostBlocked(RuntimeError):
    """A host kept refusing requests; stop this step and resume later."""


class NotFound(RuntimeError):
    pass


@dataclass
class HostPolicy:
    max_concurrency: int
    start_rps: float
    max_rps: float
    min_rps: float = 0.3
    cooldown_s: float = 20.0
    max_strikes: int = 6


# Conservative defaults. NSE's website API is the most sensitive endpoint, so it gets two
# concurrent requests at most; the static archive CDN tolerates more.
POLICIES = {
    "www.nseindia.com": HostPolicy(max_concurrency=2, start_rps=2.0, max_rps=3.0, cooldown_s=30.0),
    "nsearchives.nseindia.com": HostPolicy(max_concurrency=10, start_rps=8.0, max_rps=16.0),
    "archives.nseindia.com": HostPolicy(max_concurrency=6, start_rps=5.0, max_rps=10.0),
    "raw.githubusercontent.com": HostPolicy(max_concurrency=8, start_rps=10.0, max_rps=25.0),
}
DEFAULT_POLICY = HostPolicy(max_concurrency=4, start_rps=4.0, max_rps=8.0)


class _Gate:
    """Thread-safe AIMD pacer + concurrency semaphore + circuit breaker for one host."""

    def __init__(self, host: str, policy: HostPolicy, scale: float):
        self.host, self.p = host, policy
        self.rps = max(policy.min_rps, policy.start_rps * scale)
        self.max_rps = max(self.rps, policy.max_rps * scale)
        self.sem = threading.BoundedSemaphore(max(1, policy.max_concurrency))
        self.lock = threading.Lock()
        self.next_at = 0.0
        self.paused_until = 0.0
        self.strikes = 0
        self.clean = 0
        self.blocks = 0

    def wait_turn(self):
        with self.lock:
            start = max(self.next_at, self.paused_until, time.monotonic())
            self.next_at = start + 1.0 / self.rps
        while True:  # a cool-down set after the reservation still applies
            delay = max(start, self.paused_until) - time.monotonic()
            if delay <= 0:
                return
            time.sleep(min(delay, 2.0) + random.uniform(0, 0.03))

    def ok(self):
        with self.lock:
            self.strikes = 0
            self.clean += 1
            if self.clean >= 25 and self.rps < self.max_rps:
                self.rps = min(self.max_rps, self.rps * 1.15)
                self.clean = 0

    def blocked(self):
        with self.lock:
            self.blocks += 1
            self.strikes += 1
            self.clean = 0
            self.rps = max(self.p.min_rps, self.rps / 2)
            pause = self.p.cooldown_s * (2 ** min(self.strikes - 1, 4)) + random.uniform(0, 5)
            self.paused_until = max(self.paused_until, time.monotonic() + pause)
            if self.strikes >= self.p.max_strikes:
                raise HostBlocked(
                    f"{self.host} refused {self.strikes} times in a row. Stopping this step so the "
                    "host is not hammered. Wait 15-30 minutes and run the same command again; it resumes.")
            return pause


class Net:
    """Shared client. ``scale`` (0.25-2) scales every host's pace; 1 is the tested default."""

    def __init__(self, scale: float = 1.0, timeout: float = 30.0, log=print):
        self.scale = max(0.25, min(2.0, scale))
        self.log = log
        self.gates: dict[str, _Gate] = {}
        self.glock = threading.Lock()
        self.timeout = timeout
        self._local = threading.local()
        self._cookie_lock = threading.Lock()
        self._cookies = httpx.Cookies()
        self._cookie_version = 0
        self._cookie_age = 0.0
        self.bytes = 0
        self.requests = 0

    # ------------------------------------------------------------------ plumbing
    def _gate(self, host: str) -> _Gate:
        with self.glock:
            if host not in self.gates:
                self.gates[host] = _Gate(host, POLICIES.get(host, DEFAULT_POLICY), self.scale)
            return self.gates[host]

    def _client(self) -> httpx.Client:
        c = getattr(self._local, "client", None)
        if c is None:
            c = httpx.Client(timeout=self.timeout, follow_redirects=True, http2=False,
                             headers={"User-Agent": BROWSER_UA, "Accept-Language": "en-US,en;q=0.9",
                                      "Accept": "*/*", "Accept-Encoding": "gzip, deflate"})
            self._local.client = c
        if getattr(self._local, "cookie_version", -1) != self._cookie_version:
            c.cookies = httpx.Cookies(self._cookies)
            self._local.cookie_version = self._cookie_version
        return c

    def warm_nse(self, force: bool = False):
        """Load the NSE home page like a browser so API calls carry valid session cookies."""
        with self._cookie_lock:
            if not force and self._cookie_age and time.monotonic() - self._cookie_age < 240:
                return
            gate = self._gate("www.nseindia.com")
            for page in ("/", "/companies-listing/corporate-filings-financial-results"):
                gate.wait_turn()
                try:
                    r = self._client().get(NSE_HOME + page, headers={
                        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8"})
                    self._cookies.update(r.cookies)
                except httpx.HTTPError:
                    pass
            self._cookie_version += 1
            self._cookie_age = time.monotonic()

    # ------------------------------------------------------------------ public
    def get(self, url: str, *, params=None, api: bool = False, retries: int = 4,
            accept: str | None = None) -> bytes:
        """GET with pacing. Raises NotFound for 404, HostBlocked if the host keeps refusing."""
        host = urlsplit(url).hostname or ""
        gate = self._gate(host)
        headers = {}
        if api:
            self.warm_nse()
            headers = {"Accept": "application/json, text/plain, */*", "Referer": NSE_HOME + "/",
                       "X-Requested-With": "XMLHttpRequest"}
        elif "nseindia.com" in host:
            headers = {"Referer": NSE_HOME + "/"}
        if accept:
            headers["Accept"] = accept
        last = None
        for attempt in range(retries + 1):
            gate.wait_turn()
            with gate.sem:
                try:
                    r = self._client().get(url, params=params, headers=headers)
                    status = r.status_code
                except (httpx.TimeoutException, httpx.TransportError) as exc:
                    status, r = None, None
                    last = type(exc).__name__
            self.requests += 1
            if status == 200:
                body = r.content
                if api and body.lstrip()[:1] == b"<":
                    # NSE serves an HTML block page with 200 when it dislikes a session.
                    status = 403
                else:
                    gate.ok()
                    self.bytes += len(body)
                    return body
            if status == 404:
                gate.ok()
                raise NotFound(url)
            if status in BLOCK_CODES:
                pause = gate.blocked()
                self.log(f"  [{host}] HTTP {status}: slowing down, pausing {pause:.0f}s")
                if api:
                    self.warm_nse(force=True)
                last = status
                continue
            last = status if status is not None else last
            time.sleep(min(30, 1.5 * 2 ** attempt) + random.uniform(0, 1))
        raise RuntimeError(f"GET failed after retries ({last}): {url}")

    def api(self, path: str, **params) -> bytes:
        return self.get(NSE_HOME + "/api/" + path.lstrip("/"), params=params, api=True)

    def stats(self) -> dict:
        return {"requests": self.requests, "megabytes": round(self.bytes / 1e6, 1),
                "blocks": {h: g.blocks for h, g in self.gates.items() if g.blocks}}
