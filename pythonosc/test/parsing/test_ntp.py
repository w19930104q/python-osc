import unittest
import time

from pythonosc.parsing import ntp


class TestNTP(unittest.TestCase):
    """TODO: Write real tests for this when I get time..."""

    def test_nto_to_system_time(self):
        unix_time = time.time()
        timestamp = ntp.system_time_to_ntp(unix_time)
        unix_time2 = ntp.ntp_to_system_time(timestamp)
        self.assertTrue(type(unix_time) is float)
        self.assertTrue(type(timestamp) is bytes)
        self.assertTrue(type(unix_time2) is float)
        self.assertAlmostEqual(unix_time, unix_time2, places=5)

    def test_timetag_to_system_time_matches_ntp_to_system_time(self):
        timestamp = ntp.system_time_to_ntp(1569899476.167749)
        timetag = int.from_bytes(timestamp, "big")
        self.assertEqual(
            ntp.ntp_to_system_time(timestamp), ntp.timetag_to_system_time(timetag)
        )

    def test_system_time_to_ntp_preserves_binary_exact_fraction(self):
        # 0.25 s is exactly representable: the round trip must be exact.
        unix_time = 1767225600.25  # 2026-01-01T00:00:00.25 UTC
        self.assertEqual(
            unix_time, ntp.ntp_to_system_time(ntp.system_time_to_ntp(unix_time))
        )

    def test_system_time_to_ntp_sub_microsecond_precision(self):
        unix_time = time.time()
        delta = unix_time - ntp.ntp_to_system_time(ntp.system_time_to_ntp(unix_time))
        self.assertLess(abs(delta), 1e-6)

    def test_system_time_to_ntp_raises_ntp_error_on_invalid_input(self):
        self.assertRaises(ntp.NtpError, ntp.system_time_to_ntp, "not a number")
        self.assertRaises(ntp.NtpError, ntp.system_time_to_ntp, None)


if __name__ == "__main__":
    unittest.main()
