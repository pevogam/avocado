import asyncio
from unittest import IsolatedAsyncioTestCase

from avocado.core.exceptions import JobFailFast
from avocado.core.nrunner.runnable import Runnable
from avocado.core.nrunner.task import Task
from avocado.core.status.repo import StatusRepo
from avocado.core.task.runtime import RuntimeTask
from avocado.core.task.statemachine import TaskStateMachine, Worker

JOB_ID = "0000000000000000000000000000000000000000"


class FakeSpawner:
    def __init__(self, alive=False):
        self.alive = alive
        self.terminated = []
        self.state_machine = None

    def is_task_alive(self, _runtime_task):
        return self.alive

    async def wait_task(self, _runtime_task):
        return

    async def terminate_task(self, runtime_task):
        # Acquiring this lock proves Worker._terminate_tasks() does not hold it
        # while awaiting potentially slow spawner shutdown.
        if self.state_machine is not None:
            async with self.state_machine.lock:
                pass
        self.terminated.append(runtime_task)
        return True


def make_started_state():
    runnable = Runnable("noop", "noop")
    task = Task(runnable, identifier="1-noop", job_id=JOB_ID)
    task.setup_output_dir("/tmp/avocado-worker-state")
    runtime_task = RuntimeTask(task)
    status_repo = StatusRepo(JOB_ID)
    state_machine = TaskStateMachine([runtime_task], status_repo)
    state_machine.requested.clear()
    state_machine.started.append(runtime_task)
    return runtime_task, status_repo, state_machine


class WorkerTerminalStatus(IsolatedAsyncioTestCase):
    async def test_late_running_message_does_not_obscure_result(self):
        runtime_task, status_repo, state_machine = make_started_state()
        status_repo.process_message(
            {
                "id": "1-noop",
                "job_id": JOB_ID,
                "status": "finished",
                "result": "pass",
                "time": 1.0,
            }
        )
        status_repo.process_message(
            {
                "id": "1-noop",
                "job_id": JOB_ID,
                "status": "running",
                "time": 2.0,
            }
        )

        worker = Worker(state_machine, FakeSpawner(), terminal_message_timeout=0.01)
        await asyncio.wait_for(worker.monitor(), 0.5)

        self.assertEqual(runtime_task.result, "pass")
        self.assertEqual(state_machine.finished, [runtime_task])
        self.assertEqual(state_machine.monitored, [])

    async def test_missing_finished_message_becomes_error(self):
        runtime_task, status_repo, state_machine = make_started_state()
        worker = Worker(state_machine, FakeSpawner(), terminal_message_timeout=0.01)

        await asyncio.wait_for(worker.monitor(), 0.5)

        finished_data = status_repo.get_finished_task_data("1-noop")
        self.assertEqual(finished_data["result"], "error")
        self.assertIn(
            "runner exited without sending a finished status message",
            finished_data["fail_reason"].lower(),
        )
        self.assertNotIn("repository_status", finished_data)
        self.assertEqual(finished_data["status_event_count"], 0)
        self.assertEqual(finished_data["recent_status_events"], [])
        self.assertEqual(runtime_task.result, "error")
        self.assertEqual(state_machine.finished, [runtime_task])
        self.assertEqual(state_machine.monitored, [])
        self.assertEqual(
            [item["status"] for item in status_repo.get_all_task_data("1-noop")],
            ["started", "running", "finished"],
        )

    async def test_missing_finished_includes_runner_diagnostics(self):
        runtime_task, status_repo, state_machine = make_started_state()
        status_repo.process_message(
            {
                "id": "1-noop",
                "job_id": JOB_ID,
                "status": "running",
                "time": 1.0,
                "child_pid": 123,
                "queue_messages_received": 20,
                "queue_messages_forwarded": 19,
                "queue_writer_blocked_seconds": 6.5,
            }
        )
        runtime_task.spawner_diagnostics = {
            "spawner": "lxc",
            "container_id": "c5",
            "runner_exit_code": 0,
            "runner_stderr_tail": "status connection lost",
        }
        worker = Worker(state_machine, FakeSpawner(), terminal_message_timeout=0.01)

        with self.assertLogs("avocado.core.task.statemachine", level="ERROR") as logs:
            await asyncio.wait_for(worker.monitor(), 0.5)

        finished_data = status_repo.get_finished_task_data("1-noop")
        self.assertEqual(
            finished_data["last_runner_heartbeat"],
            {
                "time": 1.0,
                "child_pid": 123,
                "queue_messages_received": 20,
                "queue_messages_forwarded": 19,
                "queue_writer_blocked_seconds": 6.5,
            },
        )
        self.assertEqual(
            finished_data["spawner_diagnostics"]["runner_stderr_tail"],
            "status connection lost",
        )
        self.assertIn("status connection lost", logs.output[0])
        self.assertIsNone(runtime_task.spawner_diagnostics)

    async def test_normal_finished_message_discards_spawner_diagnostics(self):
        runtime_task, status_repo, state_machine = make_started_state()
        status_repo.process_message(
            {
                "id": "1-noop",
                "job_id": JOB_ID,
                "status": "finished",
                "result": "pass",
                "time": 1.0,
            }
        )
        runtime_task.spawner_diagnostics = {"runner_stderr_tail": "unused"}
        worker = Worker(state_machine, FakeSpawner(), terminal_message_timeout=0.01)

        await asyncio.wait_for(worker.monitor(), 0.5)

        self.assertIsNone(runtime_task.spawner_diagnostics)
        self.assertNotIn(
            "spawner_diagnostics",
            status_repo.get_finished_task_data("1-noop"),
        )

    async def test_finished_message_wins_timeout_race(self):
        runtime_task, status_repo, state_machine = make_started_state()

        async def finish_then_timeout(_task_id, _timeout):
            status_repo.process_message(
                {
                    "id": "1-noop",
                    "job_id": JOB_ID,
                    "status": "finished",
                    "result": "pass",
                    "time": 1.0,
                }
            )
            raise asyncio.TimeoutError

        status_repo.wait_for_task_finished = finish_then_timeout
        worker = Worker(state_machine, FakeSpawner(), terminal_message_timeout=0.01)

        await asyncio.wait_for(worker.monitor(), 0.5)

        self.assertEqual(runtime_task.result, "pass")
        self.assertEqual(state_machine.finished, [runtime_task])
        self.assertEqual(
            [item["status"] for item in status_repo.get_all_task_data("1-noop")],
            ["finished"],
        )

    async def test_finished_message_wins_while_timeout_waits_for_lock(self):
        runtime_task, status_repo, state_machine = make_started_state()
        state_machine.started.remove(runtime_task)
        state_machine.monitored.append(runtime_task)

        async def timeout(_task_id, _timeout):
            raise asyncio.TimeoutError

        status_repo.wait_for_task_finished = timeout
        worker = Worker(state_machine, FakeSpawner(), terminal_message_timeout=0.01)

        async with state_machine.lock:
            terminal_data_task = asyncio.create_task(
                worker._get_terminal_task_data(runtime_task)
            )
            await asyncio.sleep(0)
            status_repo.process_message(
                {
                    "id": "1-noop",
                    "job_id": JOB_ID,
                    "status": "finished",
                    "result": "pass",
                    "time": 1.0,
                }
            )

        terminal_data = await asyncio.wait_for(terminal_data_task, 0.5)

        self.assertEqual(terminal_data["result"], "pass")
        self.assertEqual(
            [item["status"] for item in status_repo.get_all_task_data("1-noop")],
            ["finished"],
        )

    async def test_termination_owns_task_during_terminal_grace(self):
        runtime_task, status_repo, state_machine = make_started_state()
        spawner = FakeSpawner()
        spawner.state_machine = state_machine
        monitor_worker = Worker(state_machine, spawner, terminal_message_timeout=10.0)
        monitor_task = asyncio.create_task(monitor_worker.monitor())

        for _ in range(10):
            if state_machine.monitored:
                break
            await asyncio.sleep(0)
        self.assertEqual(state_machine.monitored, [runtime_task])

        terminate_worker = Worker(state_machine, spawner)
        await asyncio.wait_for(terminate_worker.terminate_tasks_interrupted(), 0.5)
        await asyncio.wait_for(monitor_task, 0.5)

        self.assertEqual(spawner.terminated, [runtime_task])
        self.assertEqual(state_machine.monitored, [])
        self.assertEqual(
            status_repo.get_finished_task_data("1-noop")["result"], "interrupted"
        )
        self.assertEqual(
            [item["status"] for item in status_repo.get_all_task_data("1-noop")],
            ["started", "running", "finished"],
        )

    async def test_finished_message_without_result_becomes_error(self):
        runtime_task, status_repo, state_machine = make_started_state()
        status_repo.process_message(
            {
                "id": "1-noop",
                "job_id": JOB_ID,
                "status": "finished",
                "time": 1.0,
            }
        )

        worker = Worker(state_machine, FakeSpawner(), terminal_message_timeout=0.01)
        await asyncio.wait_for(worker.monitor(), 0.5)

        self.assertEqual(runtime_task.result, "error")
        self.assertEqual(state_machine.finished, [runtime_task])

    async def test_failfast_finalizes_current_task_before_raising(self):
        runtime_task, status_repo, state_machine = make_started_state()
        status_repo.process_message(
            {
                "id": "1-noop",
                "job_id": JOB_ID,
                "status": "finished",
                "result": "fail",
                "time": 1.0,
            }
        )
        worker = Worker(
            state_machine,
            FakeSpawner(),
            failfast=True,
            terminal_message_timeout=0.01,
        )

        with self.assertRaises(JobFailFast):
            await asyncio.wait_for(worker.monitor(), 0.5)

        self.assertEqual(runtime_task.result, "fail")
        self.assertEqual(state_machine.finished, [runtime_task])
        self.assertEqual(state_machine.monitored, [])
