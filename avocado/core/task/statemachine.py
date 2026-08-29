import asyncio
import collections
import logging
import multiprocessing
import time

from avocado.core.exceptions import JobFailFast
from avocado.core.task.runtime import RuntimeTaskStatus
from avocado.core.teststatus import STATUSES_NOT_OK
from avocado.core.utils import messages

LOG = logging.getLogger(__name__)


# A runner process normally sends its finished message before exiting.  Allow
# the status server a small delivery grace period, but never let a missing
# terminal message stall the state machine indefinitely.
DEFAULT_TERMINAL_MESSAGE_TIMEOUT = 300.0
# A finished message is emitted before the task runner finishes its own
# teardown.  That teardown should be short; a longer delay points at a stuck
# spawner wrapper rather than a still-running test.
DEFAULT_SPAWNER_EXIT_TIMEOUT = 30.0


class TaskStateMachine:
    """Represents all phases that a task can go through its life."""

    def __init__(self, tasks, status_repo):
        tasks = list(tasks)
        self._requested = collections.deque(tasks)
        self._status_repo = status_repo
        self._triaging = []
        self._ready = []
        self._started = []
        self._monitored = []
        self._finished = []
        self._lock = asyncio.Lock()
        self._cache_lock = asyncio.Lock()
        self._task_size = len(tasks)
        self._task_identifier_counts = collections.Counter(
            str(runtime_task.task.identifier) for runtime_task in tasks
        )

        self._tasks_by_id = {
            str(runtime_task.task.identifier): runtime_task.task
            for runtime_task in tasks
        }

    @property
    def requested(self):
        return self._requested

    @property
    def triaging(self):
        return self._triaging

    @property
    def ready(self):
        return self._ready

    @property
    def started(self):
        return self._started

    @property
    def monitored(self):
        return self._monitored

    @property
    def finished(self):
        return self._finished

    @property
    def lock(self):
        return self._lock

    @property
    def cache_lock(self):
        return self._cache_lock

    @property
    def task_size(self):
        return self._task_size

    @property
    async def complete(self):
        async with self._lock:
            pending = any([self._requested, self._triaging, self._ready, self._started])
        return not pending

    @property
    def tasks_by_id(self):
        return self._tasks_by_id

    def is_task_identifier_unique(self, runtime_task):
        """Return whether terminal status can identify this runtime task."""
        task_id = str(runtime_task.task.identifier)
        return self._task_identifier_counts[task_id] == 1

    async def add_new_task(self, runtime_task):
        async with self.lock:
            self._requested.appendleft(runtime_task)
            task_id = str(runtime_task.task.identifier)
            self._tasks_by_id[task_id] = runtime_task.task
            self._task_identifier_counts[task_id] += 1
        return

    async def abort(self, status_reason=None):
        """Abort all non-started tasks.

        This method will move all non-started tasks to finished with a specific
        reason.

        :param status_reason: string reason. Optional.
        """
        await self.abort_queue("requested", status_reason)
        await self.abort_queue("triaging", status_reason)
        await self.abort_queue("ready", status_reason)

    async def abort_queue(self, queue_name, status_reason=None):
        """Abort all tasks inside a specific queue adding a status reason.

        :param queue_name: a string with the queue name.
        :param status_reason: string reason. Optional.
        """
        to_remove = []
        async with self._lock:
            queue = getattr(self, queue_name)
            for _ in range(len(queue)):
                if queue_name == "requested":
                    runtime_task = queue.popleft()
                else:
                    runtime_task = queue.pop(0)
                to_remove.append(runtime_task)

        if to_remove:
            if status_reason:
                LOG.debug(
                    'Aborting queue "%s" by finishing %u tasks: %s',
                    queue_name,
                    len(to_remove),
                    status_reason,
                )
            else:
                LOG.debug(
                    'Aborting queue "%s" by finishing %u tasks',
                    queue_name,
                    len(to_remove),
                )

        for task in to_remove:
            await self.finish_task(task, status_reason)

    async def finish_task(self, runtime_task, status_reason=None):
        """Include a task to the finished queue with a specific reason.

        This method is assuming that you have removed (pop) the task from the
        original queue.

        :param runtime_task: A running task object.
        :param status_reason: string reason. Optional.
        """
        async with self._lock:
            self._finish_task_unlocked(runtime_task, status_reason)

    def _finish_task_unlocked(self, runtime_task, status_reason=None):
        """Finish a task while the caller holds the state-machine lock."""
        if runtime_task not in self.finished:
            if status_reason:
                runtime_task.status = status_reason
                LOG.debug(
                    'Task "%s" finished with status: %s',
                    runtime_task.task.identifier,
                    status_reason,
                )
            else:
                LOG.debug('Task "%s" finished', runtime_task.task.identifier)
            self.finished.append(runtime_task)

    async def finish_monitored_task(self, runtime_task, status_reason=None):
        """Atomically move a monitored task to the finished queue.

        A task can concurrently be removed from ``monitored`` by a worker
        terminating the job.  In that case the terminating worker owns the
        task and this returns ``False``.
        """
        async with self._lock:
            try:
                self.monitored.remove(runtime_task)
            except ValueError:
                return False
            self._finish_task_unlocked(runtime_task, status_reason)
        return True


class Worker:
    def __init__(
        self,
        state_machine,
        spawner,
        max_triaging=None,
        max_running=None,
        task_timeout=None,
        failfast=False,
        terminal_message_timeout=DEFAULT_TERMINAL_MESSAGE_TIMEOUT,
        spawner_exit_timeout=DEFAULT_SPAWNER_EXIT_TIMEOUT,
    ):
        self._state_machine = state_machine
        self._spawner = spawner
        if max_triaging is None:
            max_triaging = multiprocessing.cpu_count()
        self._max_triaging = max_triaging
        if max_running is None:
            max_running = 2 * multiprocessing.cpu_count() - 1
        self._max_running = max_running
        self._task_timeout = task_timeout
        self._failfast = failfast
        if terminal_message_timeout is None:
            terminal_message_timeout = DEFAULT_TERMINAL_MESSAGE_TIMEOUT
        if terminal_message_timeout < 0:
            raise ValueError("terminal_message_timeout must not be negative")
        self._terminal_message_timeout = terminal_message_timeout
        if spawner_exit_timeout is None:
            spawner_exit_timeout = DEFAULT_SPAWNER_EXIT_TIMEOUT
        if spawner_exit_timeout < 0:
            raise ValueError("spawner_exit_timeout must not be negative")
        self._spawner_exit_timeout = spawner_exit_timeout
        LOG.debug("%s has been initialized", self)

    def __repr__(self):
        fmt = (
            '<Worker spawner="{}" max_triaging={} max_running={} '
            "task_timeout={} terminal_message_timeout={} "
            "spawner_exit_timeout={}>"
        )
        return fmt.format(
            self._spawner,
            self._max_triaging,
            self._max_running,
            self._task_timeout,
            self._terminal_message_timeout,
            self._spawner_exit_timeout,
        )

    async def _send_finished_tasks_message(self, terminate_tasks, reason):
        """Sends messages related to tasks being terminated to status repository.

        On normal conditions, the "avocado-runner-*" will produce messages
        finishing each task.  But, under some conditions (such as timeouts,
        interruptions requested by users, etc), it's necessary to do this on
        the runner's behalf.

        When a task is terminated, it is necessary to send a "finish" message
        with the correct fail reason to the status repository, which will close
        logging.  This method will also send a "log" message with the reason
        (timeout, user interruption, etc).

        :param terminate_tasks: runtime_tasks which were terminated and need
                                to have messages sent on their behalf
        :type terminate_tasks: list
        :param reason: a description of what caused the task interruption (timeout, user
                       requested interruption, etc).
        :type reason: str
        """
        for terminated_task in terminate_tasks:
            task_id = str(terminated_task.task.identifier)
            job_id = terminated_task.task.job_id
            log_message = messages.LogMessage.get(
                f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} | Test interrupted: {reason}",
                id=task_id,
                job_id=job_id,
            )
            finish_message = messages.FinishedMessage.get(
                "interrupted", f"Test interrupted: {reason}", id=task_id, job_id=job_id
            )
            current_status = self._state_machine._status_repo.get_task_status(task_id)
            if current_status != "finished":
                if current_status is None:
                    start_message = messages.StartedMessage.get(
                        output_dir=terminated_task.task.runnable.output_dir,
                        id=task_id,
                        job_id=job_id,
                    )
                    self._state_machine._status_repo.process_message(start_message)
                self._state_machine._status_repo.process_message(log_message)
                self._state_machine._status_repo.process_message(finish_message)

    async def _get_terminal_task_data(self, runtime_task):
        """Return terminal task data, synthesizing an error when it is lost."""
        task_id = str(runtime_task.task.identifier)
        status_repo = self._state_machine._status_repo
        try:
            return await status_repo.wait_for_task_finished(
                task_id, self._terminal_message_timeout
            )
        except asyncio.TimeoutError:
            # A job-wide termination worker may have removed the task while
            # this worker was waiting.  In that case it owns terminal status
            # generation, and must not be raced by a synthetic ERROR here.
            async with self._state_machine.lock:
                if runtime_task not in self._state_machine.monitored:
                    return None

                # asyncio.wait_for() may decide to time out immediately
                # before the status callback publishes a real finished
                # message.  The lock acquisition above is the only await in
                # this recovery path, so rechecking here makes the decision
                # and synthetic insertion atomic on this event loop.
                finished_data = status_repo.get_finished_task_data(task_id)
                if finished_data is not None:
                    return finished_data

                task_data = status_repo.get_all_task_data(task_id) or []
                recent_event_fields = (
                    "status",
                    "type",
                    "time",
                    "result",
                    "child_pid",
                    "queue_messages_received",
                    "queue_messages_forwarded",
                    "queue_writer_locked",
                    "queue_writer_blocked_seconds",
                )
                recent_events = [
                    {key: event.get(key) for key in recent_event_fields if key in event}
                    for event in task_data[-10:]
                ]
                heartbeat_fields = recent_event_fields[4:]
                last_runner_heartbeat = next(
                    (
                        {
                            key: event.get(key)
                            for key in ("time", *heartbeat_fields)
                            if key in event
                        }
                        for event in reversed(task_data)
                        if any(key in event for key in heartbeat_fields)
                    ),
                    None,
                )
                # Keep this compatible with RuntimeTask instances created by
                # extensions (or restored state) that predate this optional
                # diagnostic field.  Missing diagnostics must not turn the
                # terminal-status recovery itself into a job crash.
                spawner_diagnostics = getattr(runtime_task, "spawner_diagnostics", None)
                reason = (
                    "Task runner exited without sending a finished status message "
                    f"within {self._terminal_message_timeout:g} seconds"
                )
                repository_status = status_repo.get_task_status(task_id)
                job_id = runtime_task.task.job_id
                log_message = messages.LogMessage.get(
                    f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} | "
                    f"Runner error: {reason}",
                    id=task_id,
                    job_id=job_id,
                )
                finish_message = messages.FinishedMessage.get(
                    "error",
                    reason,
                    id=task_id,
                    job_id=job_id,
                    repository_status=repository_status,
                    status_event_count=len(task_data),
                    recent_status_events=recent_events,
                    last_runner_heartbeat=last_runner_heartbeat,
                    spawner_diagnostics=spawner_diagnostics,
                )
                if status_repo.get_task_status(task_id) is None:
                    start_message = messages.StartedMessage.get(
                        output_dir=runtime_task.task.runnable.output_dir,
                        id=task_id,
                        job_id=job_id,
                    )
                    status_repo.process_message(start_message)
                status_repo.process_message(log_message)
                status_repo.process_message(finish_message)
            LOG.error(
                'Task "%s": %s (repository status=%r, event count=%d, '
                "recent events=%r, last runner heartbeat=%r, "
                "spawner diagnostics=%r)",
                task_id,
                reason,
                repository_status,
                len(task_data),
                recent_events,
                last_runner_heartbeat,
                spawner_diagnostics,
            )
            return status_repo.get_finished_task_data(task_id)

    async def bootstrap(self):
        """Reads from requested, moves into triaging."""
        try:
            async with self._state_machine.lock:
                if len(self._state_machine.triaging) < self._max_triaging:
                    runtime_task = self._state_machine.requested.popleft()
                    self._state_machine.triaging.append(runtime_task)
                    LOG.debug(
                        'Task "%s": requested -> triaging', runtime_task.task.identifier
                    )
                else:
                    return
        except IndexError:
            return

    async def triage(self):
        """Reads from triaging, moves into either: ready or finished."""

        try:
            async with self._state_machine.lock:
                runtime_task = self._state_machine.triaging.pop(0)
        except IndexError:
            return

        # a task waiting requirements already checked its requirements
        if runtime_task.status != RuntimeTaskStatus.WAIT_DEPENDENCIES:
            # check for requirements a task may have
            requirements_ok = await self._spawner.check_task_requirements(runtime_task)
            if requirements_ok:
                LOG.debug(
                    'Task "%s": requirements OK (will proceed to check dependencies)',
                    runtime_task.task.identifier,
                )
            else:
                await self._state_machine.finish_task(
                    runtime_task, RuntimeTaskStatus.FAIL_TRIAGE
                )
                return

        # handle task dependencies
        if runtime_task.dependencies:
            # check of all the dependency tasks finished
            if not runtime_task.are_dependencies_finished():
                async with self._state_machine.lock:
                    self._state_machine.triaging.append(runtime_task)
                    runtime_task.status = RuntimeTaskStatus.WAIT_DEPENDENCIES
                await asyncio.sleep(0.1)
                return

            # dependencies finished, let's check if they finished
            # successfully, so we can move on with the parent task
            dependencies_ok = runtime_task.can_run()
            if not dependencies_ok:
                LOG.debug(
                    'Task "%s" has failed dependencies', runtime_task.task.identifier
                )
                task_id = str(runtime_task.task.identifier)
                job_id = runtime_task.task.job_id
                reason = "Dependency was not fulfilled."
                start_message = messages.StartedMessage.get(
                    output_dir=runtime_task.task.runnable.output_dir,
                    id=task_id,
                    job_id=job_id,
                )
                log_message = messages.LogMessage.get(
                    f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} | {reason}",
                    id=task_id,
                    job_id=job_id,
                )
                finish_message = messages.FinishedMessage.get(
                    "skip", reason, id=task_id, job_id=job_id
                )
                self._state_machine._status_repo.process_message(start_message)
                self._state_machine._status_repo.process_message(log_message)
                self._state_machine._status_repo.process_message(finish_message)
                runtime_task.result = "fail"
                await self._state_machine.finish_task(
                    runtime_task, RuntimeTaskStatus.FAIL_TRIAGE
                )
                return
        if runtime_task.task.category != "test":
            # save or retrieve task from cache
            if runtime_task.is_cacheable:
                async with self._state_machine.cache_lock:
                    is_task_in_cache = await self._spawner.is_requirement_in_cache(
                        runtime_task
                    )
                    if is_task_in_cache is None:
                        async with self._state_machine.lock:
                            self._state_machine.triaging.append(runtime_task)
                            runtime_task.status = RuntimeTaskStatus.WAIT
                            await asyncio.sleep(0.1)
                        return

                    if is_task_in_cache:
                        task_id = str(runtime_task.task.identifier)
                        job_id = runtime_task.task.job_id
                        start_message = messages.StartedMessage.get(
                            output_dir=runtime_task.task.runnable.output_dir,
                            id=task_id,
                            job_id=job_id,
                        )
                        log_message = messages.LogMessage.get(
                            f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} | "
                            f"Dependency fulfilled from cache.",
                            id=task_id,
                            job_id=job_id,
                        )
                        finish_message = messages.FinishedMessage.get(
                            "pass", id=task_id, job_id=job_id
                        )
                        self._state_machine._status_repo.process_message(start_message)
                        self._state_machine._status_repo.process_message(log_message)
                        self._state_machine._status_repo.process_message(finish_message)
                        await self._state_machine.finish_task(
                            runtime_task, RuntimeTaskStatus.IN_CACHE
                        )
                        runtime_task.result = "pass"
                        return

                    await self._spawner.save_requirement_in_cache(runtime_task)

        # the task is ready to run
        async with self._state_machine.lock:
            self._state_machine.ready.append(runtime_task)

    async def _cancel_spawn_task(self, spawn_task, runtime_task):
        """Cancel a spawner operation without waiting forever for cleanup."""
        if not spawn_task.done():
            spawn_task.cancel()
        done, _ = await asyncio.wait((spawn_task,), timeout=self._spawner_exit_timeout)
        if done:
            # Consume cancellation or a cleanup exception so it is not reported
            # later as an unhandled task exception.
            await asyncio.gather(*done, return_exceptions=True)
            return

        LOG.error(
            'Task "%s": spawner ignored cancellation for another %g seconds; '
            "leaving its cleanup task detached",
            runtime_task.task.identifier,
            self._spawner_exit_timeout,
        )

        def consume_exception(task):
            if not task.cancelled():
                task.exception()

        spawn_task.add_done_callback(consume_exception)

    async def _spawn_task(self, runtime_task):
        """Wait for spawning, a terminal status, or the execution deadline.

        Some spawners run the task command synchronously and only return after
        it exits.  A terminal status therefore proves that spawning succeeded,
        even if a wrapper remains stuck after the actual runner has finished.

        :returns: ``True`` or ``False`` for the spawner result, or ``None``
                  when a spawn-phase execution timeout finalized the task.
        """
        task_id = str(runtime_task.task.identifier)
        status_repo = self._state_machine._status_repo
        spawn_task = asyncio.create_task(self._spawner.spawn_task(runtime_task))
        terminal_task = None
        wait_tasks = {spawn_task}
        if self._state_machine.is_task_identifier_unique(runtime_task):
            terminal_task = asyncio.create_task(
                status_repo.wait_for_task_finished(task_id, None)
            )
            wait_tasks.add(terminal_task)
        else:
            LOG.warning(
                'Task "%s": terminal-status spawn shortcut disabled because '
                "the identifier is not unique within the job",
                task_id,
            )
        timeout = None
        if runtime_task.execution_timeout is not None:
            timeout = max(0, runtime_task.execution_timeout - time.monotonic())

        try:
            done, _ = await asyncio.wait(
                wait_tasks,
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if spawn_task in done:
                return await spawn_task

            if terminal_task is not None and terminal_task in done:
                # Consume and validate the waiter result before giving the
                # wrapper a short opportunity to finish its normal teardown.
                terminal_data = await terminal_task
                wrapper_done, _ = await asyncio.wait(
                    (spawn_task,), timeout=self._spawner_exit_timeout
                )
                if wrapper_done:
                    try:
                        spawn_result = await spawn_task
                    except Exception:  # pylint: disable=W0718
                        LOG.exception(
                            'Task "%s": spawner failed after terminal status %r',
                            runtime_task.task.identifier,
                            terminal_data.get("result"),
                        )
                        return True
                    if not spawn_result:
                        LOG.warning(
                            'Task "%s": spawner reported failure after terminal '
                            "status %r; treating the delivered status as authoritative",
                            runtime_task.task.identifier,
                            terminal_data.get("result"),
                        )
                    return True

                diagnostics = dict(
                    getattr(runtime_task, "spawner_diagnostics", None) or {}
                )
                diagnostics.update(
                    {
                        "terminal_status_received": True,
                        "terminal_result": terminal_data.get("result"),
                        "spawner_exit_timeout": self._spawner_exit_timeout,
                    }
                )
                runtime_task.spawner_diagnostics = diagnostics
                LOG.error(
                    'Task "%s": terminal status %r was received, but the spawner '
                    "did not return within %g seconds; cancelling the stuck "
                    "wrapper",
                    runtime_task.task.identifier,
                    terminal_data.get("result"),
                    self._spawner_exit_timeout,
                )
                await self._cancel_spawn_task(spawn_task, runtime_task)
                return True

            LOG.error(
                'Task "%s": spawner did not return before the %g-second task '
                "execution timeout",
                runtime_task.task.identifier,
                self._task_timeout,
            )
            await self._cancel_spawn_task(spawn_task, runtime_task)
            await self._terminate_task(runtime_task, RuntimeTaskStatus.TIMEOUT)
            await self._send_finished_tasks_message(
                [runtime_task], "Timeout reached while spawning task"
            )
            terminal_data = status_repo.get_finished_task_data(task_id)
            if terminal_data is not None:
                runtime_task.result = terminal_data["result"]
            await self._state_machine.finish_task(
                runtime_task, RuntimeTaskStatus.TIMEOUT
            )
            return None
        except asyncio.CancelledError:
            await self._cancel_spawn_task(spawn_task, runtime_task)
            raise
        finally:
            if terminal_task is not None:
                if not terminal_task.done():
                    terminal_task.cancel()
                await asyncio.gather(terminal_task, return_exceptions=True)

    async def start(self):
        """Reads from ready, moves into either: started or finished."""
        try:
            async with self._state_machine.lock:
                runtime_task = self._state_machine.ready.pop(0)
        except IndexError:
            return

        # enforce a rate limit on the number of started (currently
        # running) tasks.  this is a global limit, but the spawners
        # can also be queried with regards to their capacity to handle
        # new tasks
        should_wait = False
        async with self._state_machine.lock:
            if len(self._state_machine.started) >= self._max_running:
                self._state_machine.ready.insert(0, runtime_task)
                runtime_task.status = RuntimeTaskStatus.WAIT
                should_wait = True
        if should_wait:
            await asyncio.sleep(0.1)
            return

        LOG.debug(
            'Task "%s": about to be spawned with "%s"',
            runtime_task.task.identifier,
            self._spawner,
        )
        if self._task_timeout is not None:
            # Include spawning in the execution deadline.  Container spawners
            # can otherwise block before monitor() installs the timeout.
            runtime_task.execution_timeout = time.monotonic() + self._task_timeout
        start_ok = await self._spawn_task(runtime_task)
        if start_ok is None:
            return
        if start_ok:
            LOG.debug('Task "%s": spawned successfully', runtime_task.task.identifier)
            runtime_task.status = RuntimeTaskStatus.STARTED
            async with self._state_machine.lock:
                self._state_machine.started.append(runtime_task)
        else:
            await self._state_machine.finish_task(
                runtime_task, RuntimeTaskStatus.FAIL_START
            )

    async def monitor(self):
        """Reads from started, moves into finished."""
        async with self._state_machine.lock:
            try:
                runtime_task = self._state_machine.started.pop(0)
            except IndexError:
                return
            # Keep the transition atomic and the task represented until its
            # terminal status has been reconciled.  In particular, do not
            # leave a task in no queue while waiting for a potentially delayed
            # finished message.
            self._state_machine.monitored.append(runtime_task)

        terminal_data = None
        if self._state_machine.is_task_identifier_unique(runtime_task):
            terminal_data = self._state_machine._status_repo.get_finished_task_data(
                str(runtime_task.task.identifier)
            )
        if terminal_data is not None:
            LOG.debug(
                'Task "%s" reached monitor phase with terminal status already '
                "available",
                runtime_task.task.identifier,
            )
        elif self._spawner.is_task_alive(runtime_task):
            LOG.debug(
                'Task "%s" is alive at monitor phase', runtime_task.task.identifier
            )
            try:
                if runtime_task.execution_timeout is None:
                    remaining = None
                else:
                    remaining = runtime_task.execution_timeout - time.monotonic()
                await asyncio.wait_for(self._spawner.wait_task(runtime_task), remaining)
            except asyncio.TimeoutError:
                await self._terminate_task(runtime_task, RuntimeTaskStatus.TIMEOUT)
                await self._send_finished_tasks_message(
                    [runtime_task], "Timeout reached"
                )
        else:
            LOG.debug(
                'Task "%s" was very short lived, this may be '
                "completely normal depending on the task itself. "
                "Please check the task logs",
                runtime_task.task.identifier,
            )

        # A late running/log message must not obscure an earlier terminal
        # result.  If the runner exited without sending one, bound the grace
        # period and turn that runner protocol failure into a clear ERROR.
        latest_task_data = terminal_data or await self._get_terminal_task_data(
            runtime_task
        )
        # Spawner output tails are retained solely for terminal-loss
        # diagnostics.  Do not keep them for every completed task until the
        # entire job object is released.
        runtime_task.spawner_diagnostics = None
        if latest_task_data is None:
            # A terminating worker took ownership while terminal status was
            # being reconciled.
            return
        if runtime_task.task.category != "test":
            async with self._state_machine.cache_lock:
                await self._spawner.update_requirement_cache(
                    runtime_task, latest_task_data["result"].upper()
                )
        runtime_task.result = latest_task_data["result"]
        finalized = await self._state_machine.finish_monitored_task(
            runtime_task, RuntimeTaskStatus.FINISHED
        )
        if not finalized:
            # A terminating worker already removed this task.
            return
        result_stats = set(
            key.upper() for key in self._state_machine._status_repo.result_stats.keys()
        )
        if self._failfast and not result_stats.isdisjoint(STATUSES_NOT_OK):
            await self._state_machine.abort(RuntimeTaskStatus.FAILFAST)
            raise JobFailFast("Interrupting job (failfast).")

    async def _terminate_task(self, runtime_task, task_status):
        runtime_task.status = task_status
        terminate_result = await self._spawner.terminate_task(runtime_task)
        if not terminate_result:
            LOG.error('Could not terminate task "%s"', runtime_task.task.identifier)

    async def _terminate_tasks(self, task_status):
        await self._state_machine.abort(task_status)
        terminated = []
        while True:
            runtime_task = None
            async with self._state_machine.lock:
                try:
                    runtime_task = self._state_machine.monitored.pop(0)
                except IndexError:
                    if (
                        len(self._state_machine.finished) + len(terminated)
                        >= self._state_machine.task_size
                    ):
                        break
            if runtime_task is None:
                # Other workers may still be moving started tasks to the
                # monitored queue.  Do not busy-spin or hold the global state
                # lock while waiting for them.
                await asyncio.sleep(0.01)
                continue
            # Spawner shutdown can be slow.  It must not hold the global state
            # lock, which is needed by the workers being terminated.
            await self._terminate_task(runtime_task, task_status)
            terminated.append(runtime_task)
        return terminated

    async def terminate_tasks_timeout(self):
        """Terminate all running tasks with a timeout message."""
        task_status = RuntimeTaskStatus.TIMEOUT
        terminated = await self._terminate_tasks(task_status)
        await self._send_finished_tasks_message(terminated, "Timeout reached")

    async def terminate_tasks_interrupted(self):
        """Terminate all running tasks with an interrupted message."""
        task_status = RuntimeTaskStatus.INTERRUPTED
        terminated = await self._terminate_tasks(task_status)
        await self._send_finished_tasks_message(terminated, "Interrupted by user")

    async def run(self):
        """Pushes Tasks forward and makes them do something with their lives."""
        while True:
            is_complete = await self._state_machine.complete
            if is_complete:
                break
            await self.bootstrap()
            await self.triage()
            await self.start()
            await self.monitor()
