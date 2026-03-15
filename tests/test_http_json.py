import ssl
import unittest
from unittest import mock
from urllib.error import URLError

import arb_rank
import price_arb_cn
import server


class _FakeResponse:
    def __init__(self, raw: bytes) -> None:
        self._raw = raw

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self) -> bytes:
        return self._raw


class HttpJsonTests(unittest.TestCase):
    def test_all_http_helpers_retry_on_ssl_verification_failure(self):
        helpers = [
            lambda: server._http_get_json("https://example.test/data", timeout_s=3.0),
            lambda: arb_rank.http_get_json("https://example.test/data", timeout_s=3.0),
            lambda: price_arb_cn.http_get_json("https://example.test/data", timeout_s=3.0),
        ]

        for helper in helpers:
            with self.subTest(helper=helper):
                verify_error = URLError(
                    ssl.SSLCertVerificationError(1, "CERTIFICATE_VERIFY_FAILED")
                )
                with mock.patch(
                    "urllib.request.urlopen",
                    side_effect=[
                        verify_error,
                        _FakeResponse(b'{"ok": true, "items": [1]}'),
                    ],
                ) as patched:
                    data = helper()

                self.assertEqual(data, {"ok": True, "items": [1]})
                self.assertEqual(patched.call_count, 2)


if __name__ == "__main__":
    unittest.main()
