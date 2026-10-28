"""Anonymous HTTP transport for public permalink documents."""

import requests
from curl_cffi import CurlOpt
from curl_cffi.requests import Session
from curl_cffi.requests.exceptions import RequestException

# Observed on a successful anonymous public-document request. Keep this paired
# with the tested transport; a Requests session with these headers was unresolved.
PUBLIC_DOCUMENT_HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/jxl,image/avif,image/webp,image/apng,"
        "*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/156.0.0.0 Safari/537.36"
    ),
    "sec-ch-ua": '"Not:A-Brand";v="8", "Chromium";v="156"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}


class PublicDocumentSession(Session):
    """Expose the session contract used by ObservedPublicSession.

    curl performs one explicit request without retries or redirect traversal.
    The outer observed session owns credentials, cookies, budgets and cooldowns.
    """

    def __init__(self):
        super().__init__(
            impersonate="chrome150",
            trust_env=False,
            allow_redirects=False,
            # libcurl reads proxy environment variables independently of the
            # requests-compatible trust_env attribute.
            curl_options={CurlOpt.PROXY: ""},
        )
        self.adapters = {}

    def mount(self, prefix, adapter):
        if adapter.max_retries.total != 0:
            raise ValueError("Public document transport cannot retry implicitly")
        self.adapters[prefix] = adapter

    def request(self, method, url, *args, **kwargs):
        try:
            return super().request(method, url, *args, **kwargs)
        except RequestException:
            # Preserve the observed session's counted, redacted failure path.
            raise requests.RequestException("Public document request failed") from None
