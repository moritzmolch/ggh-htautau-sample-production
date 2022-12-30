import os
import signal
import six
import subprocess
import time
from law.util import perf_counter


def interruptable_and_readable_popen(*args, **kwargs):
    """interruptable_and_readable_popen(*args, interrupt_callback=None, kill_timeout=None, **kwargs)
    Aims to merge the functionality of :py:function:`interrutpable_open` and
    :py:function:`readable_open`.
    *interrupt_callback* can be a function, accepting the process instance as an argument, that is
    called immediately after a *KeyboardInterrupt* occurs. After that, a SIGTERM signal is send to
    the subprocess to allow it to gracefully shutdown.
    When *kill_timeout* is set, and the process is still alive after that period (in seconds), a
    SIGKILL signal is sent to force the process termination.
    All other *args* and *kwargs* are forwarded to the :py:class:`Popen` constructor.
    """

    # get kwargs not being passed to Popen
    interrupt_callback = kwargs.pop("interrupt_callback", None)
    kill_timeout = kwargs.pop("kill_timeout", None)

    # enforce pipes: stdout is redirected to PIPE, stderr is redirected to STDOUT leading to a merged stream
    kwargs["stdout"] = subprocess.PIPE
    kwargs["stderr"] = subprocess.STDOUT

    # start the subprocess in a new process group
    kwargs["preexec_fn"] = os.setsid

    # create the process
    p = subprocess.Popen(*args, **kwargs)

    # generator function for printing out the lines
    if six.PY2:

        def line_gen():
            for line in iter(lambda: p.stdout.readline(), ""):
                yield line.rstrip()

    elif six.PY3:

        def line_gen():
            for line in iter(lambda: p.stdout.readline(), b""):
                if six.PY3:
                    line = line.decode("utf-8")
                yield line.rstrip()

    # communicate to the process and and handle interrupts
    try:
        out, err = p.communicate()
    except KeyboardInterrupt:
        # allow the interrupt_callback to perform a custom process termination
        if callable(interrupt_callback):
            interrupt_callback(p)

        # when the process is still alive, send SIGTERM to gracefully terminate it
        pgid = os.getpgid(p.pid)
        if p.poll() is None:
            os.killpg(pgid, signal.SIGTERM)

        # when a kill_timeout is set, and the process is still running after that period,
        # send SIGKILL to force its termination
        if kill_timeout is not None:
            target_time = perf_counter() + kill_timeout
            while target_time > perf_counter():
                time.sleep(0.05)
                if p.poll() is not None:
                    # the process terminated, exit the loop
                    break
            else:
                # check the status again to avoid race conditions
                if p.poll() is None:
                    os.killpg(pgid, signal.SIGKILL)

        # transparently reraise
        raise

    if six.PY3:
        if out is not None:
            out = out.decode("utf-8")
        if err is not None:
            err = err.decode("utf-8")

    return p.returncode, out, err, line_gen()
