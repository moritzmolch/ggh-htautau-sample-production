import law.util
import re


def _build_cms_driver_command(
    fragment=None,
    kwargs=None,
    args=None,
):
    # convert parameters to concrete types
    _fragment = fragment if fragment is not None else ""
    _args = list(args) if args is not None else []
    _kwargs = dict(kwargs) if kwargs is not None else {}

    # check if required arguments have been passed
    if _fragment == "" and _kwargs.get("filein", "") == "":
        raise ValueError(
            "If fragment is empty the argument 'filein' in the 'kwargs' mapping has to be set."
        )

    # construct the cmsDriver command
    cmd = ["cmsDriver.py"]
    if _fragment != "":
        cmd.append(_fragment)
    for k, v in _kwargs.items():
        cmd.append("--{key:s}".format(key=k))
        cmd.append(str(v))
    for arg in _args:
        cmd.append("--{arg:s}".format(arg=arg))

    return cmd


def cms_driver(
    fragment=None,
    kwargs=None,
    args=None,
    popen_kwargs=None,
    yield_output=False,
):
    cmd = law.util.quote_cmd(_build_cms_driver_command(fragment=fragment, kwargs=kwargs, args=args))
    fn = law.util.readable_popen if yield_output else law.util.interruptable_popen
    _popen_kwargs = dict(popen_kwargs) if popen_kwargs else {}
    return fn(cmd, shell=True, executable="/bin/bash", **_popen_kwargs)


def cms_run(
    config_file,
    popen_kwargs=None,
    yield_output=False,
):
    cmd = law.util.quote_cmd(["cmsRun", config_file])
    fn = law.util.readable_popen if yield_output else law.util.interruptable_popen
    _popen_kwargs = dict(popen_kwargs) if popen_kwargs else {}
    return fn(cmd, shell=True, executable="/bin/bash", **_popen_kwargs)


def _parse_cms_run_event(line):
    if not isinstance(line, str):
        return None
    m = re.match(r"^Begin\sprocessing\sthe\s(\d+)\w{2,2}\srecord\..+$", line.strip())
    if m is None:
        return None
    return int(m.group(1))
