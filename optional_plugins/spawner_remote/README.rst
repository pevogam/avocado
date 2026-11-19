=====================
Remote Spawner Plugin
=====================

This plugin makes use of remote `aexpect
<https://github.com/avocado-framework/aexpect/>`__ sessions (to remote
hosts or equivalently remote containers behind remote hosts forwarded
via specific ports) as slots to schedule test runs on.

It mainly draws inspiration and mimics slot and other
code from the LXC spawner with the exception of extra configurable
test timeout that is enforced by aexpect as a dependency and fully
specific to this type of spawner.

To install the Remote Spawner plugin from pip, use::

    $ sudo pip install avocado-framework-plugin-spawner-remote

After it is installed, add a slot (e.g. "board") to your avocado
config file::

    [spawner.remote]
    slots = ['board']

Then you need a JSON file of the same name as the slot. Its contents
are the command line parameters of aexpect's ``remote_login`` function
of module ``remote``, e.g.::

    {
       "client": "telnet",
       "host": "192.168.64.2",
       "port": "23",
       "username": "root",
       "password": "",
       "prompt": "#"
    }

Final important detail: the remote site also needs avocado
installed.

By default, task output is collected in a unique temporary directory on the
remote site.
Before reporting completion, the runner sends its files through the status
connection into the job's ``test-results`` directory, then removes the
temporary directory.  Shared storage and a separate SSH file-transfer
connection are not required.  Both sites must have an Avocado version
supporting ``task-run --temporary-output``.  Forced termination can prevent
file collection; detected collection or connection failures preserve the
temporary directory for recovery.

If the job results directory is mounted at the same path on the remote site,
restore direct writes to that shared directory by disabling copying::

    [spawner.remote]
    copy_output = False
