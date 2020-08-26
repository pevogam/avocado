import asyncio
import os
import tempfile
import shutil
import stat

try:
    import lxc
    LXC_AVAILABLE = True
except ImportError:
    LXC_AVAILABLE = False

from avocado.core.plugin_interfaces import Init, Spawner
from avocado.core.settings import settings
from avocado.core.output import LOG_JOB
from avocado.core.spawners.common import SpawnerMixin, SpawnMethod


class LXCStreamsFile(object):

    def __init__(self):
        self.fd_out = None
        self.path_out = None
        self.fd_out = None
        self.path_out = None

    def fileno(self):
        return self.fd

    def read(self):
        with open(self.path, "r") as fp:
            return fp.read()

    def __enter__(self):
        self.fd, self.path = tempfile.mkstemp()
        return self

    def __exit__(self, *args):
        os.remove(self.path)


class LXCSpawnerInit(Init):

    description = 'LXC (container) based spawner initialization'

    def initialize(self):
        section = 'spawner.lxc'

        help_msg = 'Distro for the LXC container'
        settings.register_option(
            section=section,
            key='dist',
            help_msg=help_msg,
            default='fedora')

        help_msg = 'Release of the LXC container (depends on the choice of distro)'
        settings.register_option(
            section=section,
            key='release',
            help_msg=help_msg,
            default='32')

        help_msg = 'Architecture of the LXC container'
        settings.register_option(
            section=section,
            key='arch',
            help_msg=help_msg,
            default='i386')


class LXCSpawner(Spawner, SpawnerMixin):

    description = 'LXC (container) based spawner'
    METHODS = [SpawnMethod.STANDALONE_EXECUTABLE]

    @staticmethod
    def run_container_cmd(container, command):
        with LXCStreamsFile() as tmp_out, LXCStreamsFile() as tmp_err:
            exitcode = container.attach_wait(lxc.attach_run_command, command, stdout=tmp_out, stderr=tmp_err)
            return exitcode, tmp_out.read(), tmp_err.read()

    @staticmethod
    async def run_container_cmd_async(container, command):
        with LXCStreamsFile() as tmp_out, LXCStreamsFile() as tmp_err:
            pid = container.attach(lxc.attach_run_command, command, stdout=tmp_out, stderr=tmp_err)
            loop = asyncio.get_event_loop()
            _, exitcode = await loop.run_in_executor(None, os.waitpid, pid, os.WUNTRACED)
            return exitcode, tmp_out.read(), tmp_err.read()

    @staticmethod
    def is_task_alive(runtime_task):
        if runtime_task.spawner_handle is None:
            return False

        if not LXC_AVAILABLE:
            msg = 'LXC python bindings not available on the system'
            runtime_task.status = msg
            # we shouldn't reach this point
            raise RuntimeError("LXC dependency is missing")

        container = lxc.Container(runtime_task.spawner_handle)
        if not container.defined:
            return False
        if not container.running:
            return False

        status, _, _ = LXCSpawner.run_container_cmd(container, ["pgrep", "avocado-runner"])
        return status == 0

    async def spawn_task(self, runtime_task):
        task = runtime_task.task
        entry_point_cmd = '/root/avocado-runner'
        entry_point_args = task.get_command_args()
        entry_point_args.insert(0, "task-run")
        entry_point_args.insert(0, entry_point_cmd)

        config = settings.as_dict()
        dist = config.get('spawner.lxc.distro')
        release = config.get('spawner.lxc.release')
        arch = config.get('spawner.lxc.arch')

        if not LXC_AVAILABLE:
            msg = 'LXC python bindings not available on the system'
            runtime_task.status = msg
            return False

        # TODO: need dynamical container name
        container_id = "c34"
        c = lxc.Container(container_id)
        if not c.defined:
            # Create the container rootfs
            if not c.create("download", lxc.LXC_CREATE_QUIET, {"dist": dist,
                                                               "release": release,
                                                               "arch": arch}):
                LOG_JOB.error("Failed to create the container rootfs")
                return False

        # Deploy test data to the container
        # TODO: Currently limited to avocado-runner, we'll expand on that
        # when the runner requirements system is in place
        this_path = os.path.abspath(__file__)
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(this_path)))
        avocado_runner_path = os.path.join(base_path, 'core', 'nrunner.py')
        destination_runner_path = os.path.join(c.get_config_item("lxc.rootfs.path"),
                                               entry_point_cmd.lstrip("/"))
        try:
            shutil.copy2(avocado_runner_path, destination_runner_path)
            os.chmod(destination_runner_path, mode=(stat.S_IRUSR | stat.S_IXUSR |
                                                    stat.S_IRGRP | stat.S_IXGRP |
                                                    stat.S_IROTH | stat.S_IXOTH))
        except (FileNotFoundError, PermissionError):
            return False

        # Start the container
        if not c.running:
            if not c.start():
                LOG_JOB.error("Failed to start the container")
                return False

        # Wait for connectivity
        # TODO: The current networking is not good enough to connect to the status server
        if not c.get_ips(timeout=30):
            LOG_JOB.error("Failed to connect to the container")
            return False

        # Query some information
        LOG_JOB.info(f"Container state: {c.state}")
        LOG_JOB.info(f"Container PID: {c.init_pid}")

        exitcode, output, err = await LXCSpawner.run_container_cmd_async(c, entry_point_args)
        LOG_JOB.info(f"Command exited with code {exitcode} ({err}) and output {output}")

        # TODO: decide whether to always shutdown and destroy the LXC container
        # c.state == "RUNNING"
        # for c in lxc.list_containers(as_object=True): ...

        # Stop the container
        #if not c.shutdown(30):
        #    LOG_JOB.warning("Failed to cleanly shutdown the container, forcing.")
        #    if not c.stop():
        #        LOG_JOB.error("Failed to kill the container")
        #        return False

        # Destroy the container
        #if not c.destroy():
        #    LOG_JOB.error("Failed to destroy the container.")
        #    return False

        runtime_task.spawner_handle = container_id
        return True

    @staticmethod
    async def wait_task(runtime_task):
        while True:
            if not LXCSpawner.is_task_alive(runtime_task):
                return
            await asyncio.sleep(0.1)

    @staticmethod
    async def check_task_requirements(runtime_task):
        runnable_requirements = runtime_task.task.runnable.requirements
        if not runnable_requirements:
            return True
        # TODO: implement requirements for an LXC container, e.g. a given state
        return True
