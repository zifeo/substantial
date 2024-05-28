import asyncio
import subprocess
import unittest
import uvloop
import os

from tests.workflows.simple import simple_workflow

from demo.workflows import example_workflow
from substantial.conductor import Recorder, SubstantialMemoryConductor
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
            [ "bash", "tests/workflows/run.sh" ],
            capture_output=True
        )
        stdout = output.stdout.decode("utf-8")
        stderr = output.stdout.decode("utf-8")
        if output.returncode == 0:
            self.assertIn("LogKind.Save C B A", stdout)
        else:
            print("Issue encountered")
            print(stderr)
        


if __name__ == '__main__':
    unittest.main()
