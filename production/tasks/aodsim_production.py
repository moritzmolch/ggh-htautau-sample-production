import datetime
import time
from jinja2 import Template
import law
import luigi
import os
import re
import subprocess

from production.tasks.base import BaseTask, CMSDriverTask


class FragmentGeneration(BaseTask):

    cmssw_path = luigi.Parameter()
    fragment_template_path = luigi.Parameter()

    higgs_mass = luigi.FloatParameter()

    def local_path(self, *path):
        parts = (self.cmssw_path,) + path
        return os.path.join(*parts)

    def output(self):
        higgs_mass_str = (
            str(int(self.higgs_mass))
            if int(self.higgs_mass) == self.higgs_mass
            else str(self.higgs_mass).replace(".", "p")
        )
        return self.local_target(
            "src",
            "Configuration",
            "GenProduction",
            "python",
            "GluGluHToTauTau_MH{higgs_mass:s}_pythia8_TuneCP5_cff.py".format(higgs_mass=higgs_mass_str),
        )

    def run(self):
        # get the output
        _output = self.output()
        self.logger.info("creating fragment at {output:s}".format(output=_output.path))

        # load template and replace placeholders
        _fragment_template = law.LocalFileTarget(self.fragment_template_path)
        with _fragment_template.open(mode="r") as f:
            template = Template(f.read())
        content = template.render(higgs_mass=self.higgs_mass)

        # write the fragment content to the output target
        _output.dump(content, formatter="text")
        self.logger.info("successfully saved output to {output:s}".format(output=_output.path))


class CompileCMSSWWithFragments(BaseTask, law.SandboxTask):

    cmssw_path = luigi.Parameter()

    sandbox = "bash::${PROD_BASE}/sandboxes/cmssw_default.sh"

    def requires(self):
        reqs = []
        for higgs_mass in range(50, 250, 1):
            reqs.append(FragmentGeneration.req(self, higgs_mass=higgs_mass, cmssw_path=self.cmssw_path))
        for higgs_mass in range(250, 805, 5):
            reqs.append(FragmentGeneration.req(self, higgs_mass=higgs_mass, cmssw_path=self.cmssw_path))
        return reqs

    def run(self):
        self.logger.info("Compiling CMSSW release {cmssw_path:s} with fragments".format(cmssw_path=self.cmssw_path))
        # compile CMSSW in the src directory of the CMSSW directory
        cmd = ["scramv1", "build"]
        self.logger.info("Run command {cmd:s}".format(cmd=law.util.quote_cmd(cmd)))
        ret_code, out, err = law.util.interruptable_popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=os.path.join(self.cmssw_path, "src"), env=self.env)
        if ret_code != 0:
            raise RuntimeError(
                "Command {cmd} failed with exit code {ret_code:d}".format(cmd=cmd, ret_code=ret_code)
                + "Output: {out:s}".format(out=out)
                + "Error: {err:s}".format(err=err)
            )
        self.logger.info("Successfully compiled CMSSW release {cmssw_path:s}".format(cmssw_path=self.cmssw_path))


class AODSIMConfigurationTemplate(BaseTask, CMSDriverTask):

    higgs_mass = luigi.FloatParameter()
    step_name = "aodsim"

    cms_driver_filein = law.NO_STR
    cms_driver_fileout = "{{ cms_driver_fileout }}"

    def requires(self):
        reqs = {}
        reqs["fragment"] = FragmentGeneration.req(self, higgs_mass=self.higgs_mass)
        return reqs

    def output(self):
        higgs_mass_str = (
            str(int(self.higgs_mass))
            if int(self.higgs_mass) == self.higgs_mass
            else str(self.higgs_mass).replace(".", "p")
        )
        return self.local_target(
            self.__class__.__name__,
            "GluGluHToTauTau_MH{higgs_mass:s}_pythia8_TuneCP5_{step:s}_cfg.py.j2".format(
                higgs_mass=higgs_mass_str,
                step=self.step_name,
            ),
        )

    def run(self):
        # get the output
        _output = self.output()
        self.logger.info(
            "creating cmsDriver.py AODSIM configuration template at {output:s}".format(output=_output.path)
        )

        # get fragment path relative to the CMSSW root directory and overwrite the corresponding instance attribute
        _fragment = self.input()["fragment"]
        m = re.match("^(.*CMSSW_\d+_\d+_\d+(_[\w\d]+)?(/src)?)/(.*)$", _fragment.path)
        if m is None:
            raise RuntimeError("Fragment path {path:s} has not the expected pattern".format(_fragment.path))
        self.cms_driver_fragment = os.path.relpath(_fragment.path, start=m.group(1))

        # create the configuration
        tmp_config = law.LocalFileTarget(is_tmp=True, tmp_dir=_output.parent)
        tmp_python_filename = tmp_config.basename
        cmd = self.build_command(python_filename=tmp_python_filename)
        self.logger.info("running command {cmd:s}".format(cmd=law.util.quote_cmd(cmd)))
        self.run_command(cmd, tmp_config)

        # load configuration content in order to inject some template placeholders
        with tmp_config.open(mode="r") as f:
            content = f.read()

        # inject template placeholders for number of events
        content = content.replace("-n -1", "-n {{ number_of_events }}")
        content = content.replace(
            "input = cms.untracked.int32(-1)", "input = cms.untracked.int32({{ number_of_events }})"
        )
        content = content.replace("nevts:-1", "nevts:{{ number_of_events }}")

        # inject template placeholders for python filename
        content = content.replace(tmp_python_filename, "{{ python_filename }}")

        # write the config to the output target
        _output.dump(content, formatter="text")
        self.logger.info("successfully saved output to {output:s}".format(output=_output.path))


class AODSIMConfiguration(BaseTask):

    cms_driver_python_filename = luigi.Parameter()
    cms_driver_fileout = luigi.Parameter()

    higgs_mass = luigi.IntParameter()
    job_index = luigi.IntParameter()
    number_of_events = luigi.IntParameter()

    step_name = "aodsim"

    def requires(self):
        reqs = {}
        reqs["config_template"] = AODSIMConfigurationTemplate.req(self, higgs_mass=self.higgs_mass)
        return reqs

    def output(self):
        return self.local_target(self.__class__.__name__, self.cms_driver_python_filename)

    def run(self):
        # get the output
        _output = self.output()

        # load the configuration template and replace placeholders for configuration filename, production output file
        # and number of events
        _config_template = self.input()["config_template"]
        template = Template(_config_template.load(formatter="text"))
        with _config_template.open(mode="r") as f:
            template = Template(f.read())
        content = template.render(
            pyhon_filename=_output.basename,
            cms_driver_fileout=self.cms_driver_fileout,
            number_of_events=self.number_of_events,
        )

        # add luminosity block modifier to the end of the configuration file
        content += "\n\nprocess.source.firstLuminosityBlock = cms.untracked.uint32(1 + {job_index:d})".format(
            job_index=self.job_index
        )

        # write useable configuration to the output target
        _output.dump(content, formatter="text")


class AODSIMProduction(BaseTask, law.SandboxTask, law.LocalWorkflow):

    step_name = "aodsim"

    sandbox = "bash::${PROD_BASE}/sandboxes/cmssw_default.sh"

    def create_branch_map(self):
        branch_map = []
        for higgs_mass in range(50, 250, 1):
            for job_index in range(5):
                branch_map.append(
                    {
                        "higgs_mass": higgs_mass,
                        "job_index": job_index,
                        "number_of_events": 10, # 2000,
                    }
                )
        for higgs_mass in range(250, 805, 5):
            for job_index in range(5, 15):
                branch_map.append(
                    {
                        "higgs_mass": higgs_mass,
                        "job_index": job_index,
                        "number_of_events": 10, # 4000,
                    }
                )
        return {k: v for k, v in enumerate(branch_map)}

    @property
    def config_filename(self):
        return "GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}_{job_index:d}_cfg.py".format(
            higgs_mass=self.branch_data["higgs_mass"], step=self.step_name, job_index=self.branch_data["job_index"]
        )

    @property
    def output_filename(self):
        return "GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}_{job_index:d}.root".format(
            higgs_mass=self.branch_data["higgs_mass"], step=self.step_name, job_index=self.branch_data["job_index"]
        )

    def requires(self):
        reqs = {}
        reqs["config"] = AODSIMConfiguration.req(
            self,
            higgs_mass=self.branch_data["higgs_mass"],
            job_index=self.branch_data["job_index"],
            number_of_events=self.branch_data["number_of_events"],
            cms_driver_python_filename=self.config_filename,
            cms_driver_fileout="file:{filename:s}".format(filename=self.output_filename),
        )
        return reqs

    def output(self):
        return self.local_target(self.__class__.__name__, self.output_filename)

    def run(self):
        # get the output
        _output = self.output()
        self.logger.info("Producing {output:s}".format(output=_output.path))

        # get the config file
        _config = self.input()["config"]

        # run the production in a temporary directory, copy input files before starting the production
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True, tmp_dir=_output.parent)
        tmp_dir.touch()
        tmp_config = law.LocalFileTarget(os.path.join(tmp_dir.path, _config.basename))
        tmp_output = law.LocalFileTarget(os.path.join(tmp_dir.path, _output.basename))
        tmp_config.copy_from_local(_config)

        # run the production
        cmd = ["cmsRun", tmp_config.basename]
        self.logger.info("Run command {cmd:s}".format(cmd=law.util.quote_cmd(cmd)))
        ret_code, out, err = law.util.interruptable_popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=tmp_config.parent, env=os.environ
        )
        if ret_code != 0:
            raise RuntimeError(
                "Command {cmd} failed with exit code {ret_code:d}".format(cmd=cmd, ret_code=ret_code)
                + "Output: {out:s}".format(out=out)
                + "Error: {err:s}".format(err=err)
            )
        elif not tmp_output.exists():
            raise RuntimeError("Output file {output:s} does not exist".format(output=tmp_output.path))

        # write produced dataset to the output target
        _output.copy_from_local(tmp_output)
        self.logger.info("Successfully produced {output:s}".format(output=_output.path))

