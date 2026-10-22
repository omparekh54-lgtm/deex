"""Share a bounded request schedule across official-source clients and threads."""
from threading import Lock
import time
from urllib.parse import urlparse


class SourcePacer:
    def __init__(self, interval=0.55, *, clock=time.monotonic, sleep=time.sleep):
        self.interval = interval
        self.clock = clock
        self.sleep = sleep
        self.lock = Lock()
        self.next_request = {}

    def wait(self, url, deadline):
        host = urlparse(url).hostname
        with self.lock:
            now = self.clock()
            slot = max(now, self.next_request.get(host, now))
            if slot >= deadline:
                raise TimeoutError('source_request_deadline')
            self.next_request[host] = slot + self.interval
        if slot > now:
            self.sleep(slot - now)


PUBLIC_SOURCE_PACER = SourcePacer()
