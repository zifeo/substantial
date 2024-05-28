import unittest

from substantial.conductor import Recorder
from substantial.types import Log, LogKind

class TestDurable(unittest.TestCase):
    def test_test(self) -> None:
        self.assertTrue(True)

    def test_logs_queued_separately(self) -> None:
        recorder = Recorder()
        handle = "workflow-id-name"
        recorder.record(handle, Log(handle, LogKind.Save, 1))
        recorder.record(handle, Log(handle, LogKind.Save, 2))
        recorder.record(handle, Log(handle, LogKind.Meta, 1))
        recorder.record(handle, Log(handle, LogKind.EventIn, ("do_stuff", ("some event payload")) ))
        self.assertEqual(len(recorder.get_recorded_runs(handle)), 2)
        self.assertEqual(len(recorder.get_recorded_events(handle)), 1)


if __name__ == '__main__':
    unittest.main()
