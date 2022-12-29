import time
from jinja2 import Template
import law
import luigi
import os
import re
import subprocess

from production.tasks.base import BaseTask, CMSDriverTask


class FragmentGeneration(BaseTask):

    cmssw_path = luigi.PathParameter(exists=True)
    fragment_template_path = luigi.PathParameter(exists=True)

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
        _output = self.output()
        _fragment_template = law.LocalFileTarget(self.fragment_template)
        with _fragment_template.open(mode="r") as f:
            template = Template(f.read())
        content = template.render(higgs_mass=self.higgs_mass)
        _output.dump(content, formatter="text")


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

        # get fragment path relative to the CMSSW root directory
        _fragment = self.input()["fragment"]
        m = re.match("^(.*CMSSW_\d+_\d+_\d+(_[\w\d]+)?(/src)?)/(.*)$", _fragment.abspath)
        if m is None:
            raise RuntimeError("Fragment path {path:s} has not the expected pattern".format(_fragment.abspath))
        rel_fragment_path = os.path.relpath(_fragment.abspath, start=m.group(1))

        # create the configuration
        tmp_config = law.LocalFileTarget(is_tmp=True)
        self.run_command(
            tmp_config, fragment=rel_fragment_path, filein=self.cms_driver_filein, fileout=self.cms_driver_fileout
        )
        tmp_config.dump("This is a test file", formatter="text")

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
        content = content.replace(tmp_config.basename, "{{ python_filename }}")

        # write the config to the output target
        _output.dump(content, formatter="text")


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


class AODSIMProduction(BaseTask, law.LocalWorkflow):

    step_name = "aodsim"

    def create_branch_map(self):
        branch_map = []
        for higgs_mass in range(50, 250, 1):
            for job_index in range(5):
                branch_map.append(
                    {
                        "higgs_mass": higgs_mass,
                        "job_index": job_index,
                        "number_of_events": 2000,
                    }
                )
        for higgs_mass in range(250, 805, 5):
            for job_index in range(5, 15):
                branch_map.append(
                    {
                        "higgs_mass": higgs_mass,
                        "job_index": job_index,
                        "number_of_events": 4000,
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

        # get the config file
        _config = self.input()["config"]

        # run the production in a temporary directory, copy input files before starting the production
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True, tmp_dir=os.getcwd())
        tmp_dir.touch()
        tmp_config = law.LocalFileTarget(os.path.join(tmp_dir.abspath, _config.basename))
        tmp_output = law.LocalFileTarget(os.path.join(tmp_dir.abspath, _output.basename))
        tmp_config.copy_from_local(_config)

        # run the production
        cmd = ["cmsRun", tmp_config.basename]
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
