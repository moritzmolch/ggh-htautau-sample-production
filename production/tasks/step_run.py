import law
import os
import subprocess
import tempfile

from production.tasks.step_configuration import AODSIMConfiguration, MINIAODSIMConfiguration, NANOAODSIMConfiguration
from production.framework import Task


class AODSIMRun(Task, law.LocalWorkflow):

    def create_branch_map(self):
        config_branch_map = AODSIMConfiguration.req(self)
        branches = []
        for branch, branch_data in config_branch_map.items():
            for job_index in branch.jobs:
                branches.append[{
                    "aodsim_config_branch": branch,
                    "name": branch_data["name"],
                    "job_index": job_index,
                }]
        return {k: v for k, v in enumerate(branches)}

    def workflow_requires(self):
        reqs = {"aodsim_config": AODSIMConfiguration.req(self)}
        return reqs

    def requires(self):
        reqs = {"aodsim_config": AODSIMConfiguration(branch=self.branch_data["aodsim_config_branch"])}
        return reqs

    def output(self):
        return law.local_target(
            "{basename:s}_aodsim_{i:d}.root".format(basename=self.branch_data["name"], i=self.branch_data["job_index"])
        )

    def run(self):

        # get the configuration with the correct job index
        job_index = self.branch_data["job_index"]
        _input = self.input()["aodsim_config"][job_index]

        # get the output and create parent directory if it does not exist
        _output = self.output()[0]
        if not _output.parent.exists():
            _output.parent.touch()

        with tempfile.TemporaryDirectory(dir=os.getcwd()) as tmpdir:
            _input.copy_to_local(tmpdir)

            # run the production
            cmd = ["cmsRun", os.path.basename(_input)]
            ret_code, out, err = law.util.interruptable_popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=tmpdir,
                env=os.environ
            )
            local_output_path = os.path.join(tmpdir, os.path.basename(_output.path))
            if ret_code != 0:
                raise RuntimeError(
                    "Command {cmd} failed with exit code {ret_code:d}".format(cmd=cmd, ret_code=ret_code)
                    + "Output: {out:s}".format(out=out)
                    + "Error: {err:s}".format(err=err)
                )
            elif not os.path.exists(local_output_path):
                raise RuntimeError("Output file {output:s} does not exist".format(output=local_output_path))

            _output.copy_from_local(os.path.join(tmpdir, os.path.basename(_output.path)))
