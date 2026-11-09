import tempfile
import unittest
from unittest.mock import patch

import demo


class ExitedProcess:
    def __init__(self):
        self.exit_code = None

    def poll(self):
        return self.exit_code


class LoadElapsedTest(unittest.TestCase):
    def test_finished_duration_stays_frozen(self):
        with tempfile.TemporaryDirectory() as work:
            raw = f"{work}/load.csv"
            open(raw, "w").close()
            process = ExitedProcess()
            instance = demo.Demo("/unused", work, None)
            instance.load = {
                "running": True,
                "raw": raw,
                "rate": 10,
                "seconds": 2,
                "started": 100.0,
                "finished": None,
                "proc": process,
            }

            with patch.object(demo.time, "monotonic", side_effect=[112.0, 115.0, 900.0]):
                running = instance.load_counts()
                process.exit_code = 0
                finished = instance.load_counts()
                later = instance.load_counts()

            self.assertEqual(running["elapsed"], 12.0)
            self.assertEqual(finished["elapsed"], 15.0)
            self.assertFalse(finished["running"])
            self.assertEqual(later["elapsed"], 15.0)


if __name__ == "__main__":
    unittest.main()
