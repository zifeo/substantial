import subprocess
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
        self.assertEqual(len(recorder.get_recorded_runs(handle)), 2, "run logs")
        self.assertEqual(len(recorder.get_recorded_events(handle)), 1, "event logs")
    
    def test_simple_workflow(self) -> None:
        output = subprocess.run(
            [ "bash", "tests/workflows/simple/run.sh" ],
            capture_output=True
        )
        stdout = output.stdout.decode("utf-8")
        stderr = output.stdout.decode("utf-8")
        if output.returncode == 0:
            self.assertIn("LogKind.Save C B A", stdout)
        else:
            self.fail(stderr)

    def test_workflow_with_events(self) -> None:
        output = subprocess.run(
            [ "bash", "tests/workflows/event/run.sh" ],
            capture_output=True
        )
        stdout = output.stdout.decode("utf-8")
        stderr = output.stdout.decode("utf-8")
        if output.returncode == 0:
            self.assertIn("LogKind.Save Hello World B A", stdout)
        else:
            self.fail(stderr)


if __name__ == '__main__':
    unittest.main()
