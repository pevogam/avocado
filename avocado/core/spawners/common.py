import enum

from mmap import mmap
from mmap import ACCESS_READ

from ...core.data_dir import get_job_results_dir
from ...utils.astring import string_to_safe_path
from .exceptions import SpawnerException

from pathlib import Path


class SpawnMethod(enum.Enum):
    """The method employed to spawn a runnable or task."""
    #: Spawns by running executing Python code, that is, having access to
    #: a runnable or task instance, it calls its run() method.
    PYTHON_CLASS = object()
    #: Spawns by running a command, that is having either a path to an
    #: executable or a list of arguments, it calls a function that will
    #: execute that command (such as with os.system())
    STANDALONE_EXECUTABLE = object()
    #: Spawns with any method available, that is, it doesn't declare or
    #: require a specific spawn method
    ANY = object()


class BaseSpawner:
    """Defines an interface to be followed by all implementations."""

    METHODS = []

    def bytes_from_file(self, filename):
        """Read bytes from a files in binary mode.

        This is a helpful method to read *local* files bytes efficiently.

        If the spawner that you are implementing needs access to local file,
        feel free to use this method.
        """
        # This could be optimized in the future.
        with open(filename, 'rb', 0) as fp:
            with mmap(fp.fileno(), 0, access=ACCESS_READ) as stream:
                yield stream.read()

    def stream_output(self, job_id, task_id):
        """Returns output files streams in binary mode from a task.

        This method will find for output files generated by a task and will
        return a generator with tuples, each one containing a filename and
        bytes.

        You need to provide in your spawner a `stream_output()` method if this
        one is not suitable for your spawner. i.e: if the spawner is trying to
        access a remote output file.
        """
        results_dir = get_job_results_dir(job_id)
        task_id = string_to_safe_path(task_id)
        data_pointer = '{}/test-results/{}/data'.format(results_dir, task_id)
        src = open(data_pointer, 'r').readline().rstrip()
        try:
            for path in Path(src).expanduser().iterdir():
                if path.is_file() and path.stat().st_size != 0:
                    for stream in self.bytes_from_file(str(path)):
                        yield (path.name, stream)
        except FileNotFoundError as e:
            raise SpawnerException("Task not found: {}".format(e))

    @staticmethod
    def is_task_alive(task):
        raise NotImplementedError("You need to implement this method.")

    def spawn_task(self, task):
        raise NotImplementedError("You need to implement this method.")
