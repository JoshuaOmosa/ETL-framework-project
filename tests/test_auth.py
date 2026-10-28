import base64
import json
import time
import unittest

from tests import _path  # noqa: F401
from etl_ingest.auth import AuthError, issue_token, verify_token

SECRET = "s" * 40
OTHER = "o" * 40


def b64(obj) -> str:
    return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()


class AuthTests(unittest.TestCase):
    def test_round_trip(self):
        token = issue_token("ops-bot", ["etl:run"], SECRET)
        claims = verify_token(token, SECRET, required_role="etl:run")
        self.assertEqual(claims["sub"], "ops-bot")

    def test_wrong_secret_rejected(self):
        with self.assertRaisesRegex(AuthError, "signature"):
            verify_token(issue_token("a", [], SECRET), OTHER)

    def test_tampered_payload_rejected(self):
        header, _, sig = issue_token("a", ["etl:read"], SECRET).split(".")
        forged = b64({"sub": "a", "roles": ["etl:run"], "exp": time.time() + 999})
        with self.assertRaisesRegex(AuthError, "signature"):
            verify_token(f"{header}.{forged}.{sig}", SECRET, required_role="etl:run")

    def test_alg_none_rejected(self):
        token = f"{b64({'alg': 'none', 'typ': 'JWT'})}.{b64({'sub': 'a', 'exp': time.time() + 999})}.x"
        with self.assertRaisesRegex(AuthError, "algorithm"):
            verify_token(token, SECRET)

    def test_expired_rejected(self):
        token = issue_token("a", [], SECRET, ttl_seconds=-120)
        with self.assertRaisesRegex(AuthError, "expired"):
            verify_token(token, SECRET)

    def test_missing_role_rejected(self):
        with self.assertRaisesRegex(AuthError, "missing role"):
            verify_token(issue_token("a", ["etl:read"], SECRET), SECRET, required_role="etl:run")

    def test_issuer_and_audience(self):
        token = issue_token("a", [], SECRET, issuer="etl", audience="ops")
        verify_token(token, SECRET, issuer="etl", audience="ops")
        with self.assertRaises(AuthError):
            verify_token(token, SECRET, issuer="someone-else")
        with self.assertRaises(AuthError):
            verify_token(token, SECRET, audience="public")

    def test_garbage_tokens_rejected(self):
        for bad in ["", "abc", "a.b", "a.b.c.d", "!!.??.**", None]:
            with self.assertRaises(AuthError, msg=repr(bad)):
                verify_token(bad, SECRET)

    def test_short_secret_refused(self):
        with self.assertRaises(ValueError):
            issue_token("a", [], "too-short")


if __name__ == "__main__":
    unittest.main()
