import unittest

from tests import _path  # noqa: F401
from etl_ingest import transforms as t


class TransformTests(unittest.TestCase):
    def test_header_normalisation(self):
        self.assertEqual(t.normalize_header("  Signup Date "), "signup_date")
        self.assertEqual(t.normalize_header("Active?"), "active")
        self.assertEqual(t.normalize_header("E-mail"), "e_mail")

    def test_empty_markers_become_none(self):
        for v in ["", "  ", "N/A", "null", "-", None]:
            self.assertIsNone(t.to_string(v), v)

    def test_string_collapses_whitespace(self):
        self.assertEqual(t.to_string("  Wanjiru   Kamau "), "Wanjiru Kamau")

    def test_email(self):
        self.assertEqual(t.to_email(" Ruth.Achieng@Example.COM "), "ruth.achieng@example.com")
        with self.assertRaises(ValueError):
            t.to_email("grace@example")

    def test_dates_in_many_formats(self):
        for raw in ["2024-03-15", "2024/03/15", "15/03/2024", "15 Mar 2024", "Mar 15, 2024", "March 15, 2024", "20240315"]:
            self.assertEqual(t.to_date(raw), "2024-03-15", raw)
        with self.assertRaises(ValueError):
            t.to_date("2024-13-01")

    def test_custom_date_formats(self):
        self.assertEqual(t.to_date("03-15-2024", date_formats=["%m-%d-%Y"]), "2024-03-15")

    def test_phone(self):
        self.assertEqual(t.to_phone("+254 712 345 678"), "+254712345678")
        self.assertEqual(t.to_phone("00256 772 123 456"), "+256772123456")
        self.assertEqual(t.to_phone("0722-111-222"), "0722111222")
        with self.assertRaises(ValueError):
            t.to_phone("12")

    def test_country(self):
        for raw, code in [("Kenya", "KE"), ("ken", "KE"), ("ke", "KE"), ("U.S.A.", "US"), ("UK", "GB")]:
            self.assertEqual(t.to_country(raw), code, raw)
        self.assertEqual(t.to_country("Republic of Kenya", country_aliases={"republic of kenya": "ke"}), "KE")
        with self.assertRaises(ValueError):
            t.to_country("Atlantis")

    def test_decimal_keeps_precision(self):
        self.assertEqual(t.to_decimal("$12,500.00"), "12500.00")
        self.assertEqual(t.to_decimal("KES 7,450"), "7450")
        with self.assertRaises(ValueError):
            t.to_decimal("abc")

    def test_int_and_bool(self):
        self.assertEqual(t.to_int("1,200"), 1200)
        with self.assertRaises(ValueError):
            t.to_int("1.5")
        self.assertTrue(t.to_bool("Yes"))
        self.assertFalse(t.to_bool("0"))
        with self.assertRaises(ValueError):
            t.to_bool("maybe")


if __name__ == "__main__":
    unittest.main()
