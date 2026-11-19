import asyncio
import os
import tempfile
import unittest
from unittest import mock

from avocado.core.nrunner.app import BaseRunnerApp
from avocado.core.nrunner.runnable import Runnable
from avocado.core.nrunner.task import Task
from avocado.core.task.runtime import RuntimeTask
from avocado.plugins.spawners.lxc import LXCSpawner


class LXCOutput(unittest.TestCase):
    def _check_spawn(self, copy_output=None):
        with tempfile.TemporaryDirectory() as root:
            destination = os.path.join(root, "results")
            task = Task(Runnable("noop", None, output_dir=destination), "1")
            runtime_task = RuntimeTask(task)
            runtime_task.spawner_handle = "output-test-container"
            config = {"spawner.lxc.slots": []}
            if copy_output is not None:
                config["spawner.lxc.copy_output"] = copy_output
            spawner = LXCSpawner(config)
            with (
                mock.patch.object(LXCSpawner, "slots_cache", {}),
                mock.patch("avocado.plugins.spawners.lxc.lxc"),
                mock.patch.object(
                    LXCSpawner, "run_container_cmd", return_value=(1, "", "")
                ),
                mock.patch.object(
                    LXCSpawner, "run_container_cmd_async", return_value=(123, "", "")
                ) as launch,
            ):
                self.assertTrue(asyncio.run(spawner.spawn_task(runtime_task)))

            args = launch.call_args.args[1][3:]
            parsed = vars(BaseRunnerApp().parser.parse_args(args))
            copies_output = copy_output is not False
            self.assertEqual(parsed["temporary_output"], copies_output)
            self.assertEqual(
                Runnable.from_args(parsed).output_dir,
                None if copies_output else destination,
            )
            self.assertEqual(task.runnable.output_dir, destination)
            self.assertTrue(os.path.isdir(destination))

    def test_copies_output_by_default(self):
        self._check_spawn()

    def test_copies_output_when_enabled(self):
        self._check_spawn(True)

    def test_uses_mounted_output_when_copying_disabled(self):
        self._check_spawn(False)
