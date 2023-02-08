from jinja2 import Template
import law
import luigi
import os
import re
import subprocess

from production.tasks.base import AnalysisTask, BaseTask, ProcessTask, DatasetTask, HTCondorWorkflow
from production.util.cms import cms_driver, cms_run

law.contrib.load("tasks", "wlcg")


class FragmentGeneration(ProcessTask, law.LocalWorkflow):
    cmssw_path = os.path.expandvars("${PROD_CMSSW_BASE}")
    fragment_template = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "templates",
            "GluGluHToTauTau_MHXXX_pythia8_TuneCP5_cff.py.j2",
        )
    )

    exclude_params_req_get = {"workflow"}
    prefer_params_cli = {"workflow"}

    def create_branch_map(self):
        branch_map = super(FragmentGeneration, self).create_branch_map()
        return branch_map

    def output(self):
        _process_inst = self.branch_data["process_inst"]
        return self.local_target(
            "src",
            "Configuration",
            "GenProduction",
            "python",
            "{filename_prefix:s}_cff.py".format(
                filename_prefix=_process_inst.get_aux("filename_prefix")
            ),
            store=self.cmssw_path,
        )

    def run(self):
        # get the output and the process instance of this branch
        _output = self.output()
        _process_inst = self.branch_data["process_inst"]

        # load template and replace placeholders
        _fragment_template = law.LocalFileTarget(self.fragment_template)
        with _fragment_template.open(mode="r") as f:
            template = Template(f.read())
        content = template.render(higgs_mass=float(_process_inst.get_aux("higgs_mass")))

        # write the fragment content to the output target
        _output.dump(content, formatter="text")
        self.publish_message(
            "successfully created fragment for process {process:s}".format(
                process=_process_inst.name
            )
        )


class CompileCMSSW(BaseTask, law.contrib.tasks.RunOnceTask):
    cmssw_path = os.path.expandvars("${PROD_CMSSW_BASE}")

    def run(self):
        cmd = ["scram", "build"]
        cwd = os.path.join(self.cmssw_path, "src")
        ret_code, _, _ = law.util.interruptable_popen(
            cmd,
            cwd=cwd,
            env=os.environ,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if ret_code != 0:
            raise Exception(
                "CMSSW compilation failed with exit code {ret_code}".format(ret_code=ret_code)
            )


class AODSIMConfigurationTemplate(DatasetTask):
    config = "mc_ul18_fastsim_aodsim"

    fileout_placeholder = "{{ cms_driver_fileout }}"

    def create_branch_map(self):
        # we do not need to structure this task as a workflow, overwrite branch map of parent task
        return []

    def requires(self):
        reqs = {}
        _dataset_inst = self.dataset_inst
        reqs["fragment"] = FragmentGeneration.req(
            self, process=_dataset_inst.processes.get_first().name, branch=0
        )
        return reqs

    def output(self):
        _dataset_inst = self.dataset_inst
        return self.local_target(
            self.__class__.__name__,
            "{filename_prefix:s}_cfg.py.j2".format(
                filename_prefix=_dataset_inst.get_aux("filename_prefix"),
            ),
        )

    def run(self):
        # get the output and the branch data
        _output = self.output()
        _config_inst = self.config_inst
        _dataset_inst = self.dataset_inst

        # get the input fragment
        _input_fragment = self.input()["fragment"]
        m = re.match(r"^(.*CMSSW_\d+_\d+_\d+(_[\w\d]+)?(/src)?)/(.*)$", _input_fragment.path)
        if m is None:
            raise RuntimeError(
                "Fragment path {path:s} has not the expected pattern".format(
                    path=_input_fragment.path
                )
            )

        # prepare arguments for the cmsDriver command
        fragment = os.path.relpath(_input_fragment.path, start=m.group(1))
        tmp_python_filename = _output.basename.replace(".j2", "")
        fileout = self.fileout_placeholder

        cms_driver_kwargs = _config_inst.get_aux("cms_driver_kwargs")
        cms_driver_kwargs["python_filename"] = tmp_python_filename
        cms_driver_kwargs["fileout"] = fileout
        if "n" in cms_driver_kwargs:
            cms_driver_kwargs.pop("n")
        cms_driver_kwargs["number"] = "-1"

        cms_driver_args = _config_inst.get_aux("cms_driver_args")
        if "no_exec" not in cms_driver_args:
            cms_driver_args.append("no_exec")

        # run the command in a temporary directory
        tmp_dir = law.LocalDirectoryTarget(
            is_tmp=True, tmp_dir=os.path.expandvars("${PROD_TMPDIR}")
        )
        tmp_dir.touch()
        tmp_python_file = law.LocalFileTarget(os.path.join(tmp_dir.path, tmp_python_filename))
        popen_kwargs = {
            "env": os.environ,
            "cwd": tmp_dir.path,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
        }
        ret_code, out, err = cms_driver(
            fragment=fragment,
            kwargs=cms_driver_kwargs,
            args=cms_driver_args,
            popen_kwargs=popen_kwargs,
            yield_output=False,
        )

        if ret_code != 0:
            self.logger.error(
                "cmsDriver command failed\n"
                + "Output\n"
                + "======\n"
                + out
                + "\n\n"
                + "Error\n"
                + "=====\n"
                + err
            )
            raise RuntimeError("cmsDriver command failed")

        # load configuration content in order to inject some template placeholders
        with tmp_python_file.open(mode="r") as f:
            content = f.read()

        # inject template placeholders for number of events
        content = content.replace("--number -1", "--number {{ number_of_events }}")
        content = content.replace(
            "input = cms.untracked.int32(-1)", "input = cms.untracked.int32({{ number_of_events }})"
        )
        content = content.replace("nevts:-1", "nevts:{{ number_of_events }}")

        # inject template placeholders for python filename
        content = content.replace(tmp_python_filename, "{{ python_filename }}")

        # write the config template to the output target
        _output.dump(content, formatter="text")
        self.publish_message(
            "successfully saved run config template for dataset {dataset:s}".format(
                dataset=_dataset_inst.name
            )
        )


class AODSIMConfiguration(DatasetTask, law.LocalWorkflow):
    config = "mc_ul18_fastsim_aodsim"

    exclude_params_req_get = {"workflow"}
    prefer_params_cli = {"workflow"}

    def create_branch_map(self):
        branch_map = super(AODSIMConfiguration, self).create_branch_map()
        return branch_map

    def workflow_requires(self):
        reqs = {}
        reqs["config_template"] = AODSIMConfigurationTemplate.req(self, dataset=self.dataset)
        return reqs

    def requires(self):
        reqs = {}
        reqs["config_template"] = AODSIMConfigurationTemplate.req(self, dataset=self.dataset)
        return reqs

    def output(self):
        filename = os.path.basename(self.branch_data["keys"][0]).replace(".root", "_cfg.py")
        target = self.local_target(self.__class__.__name__, filename)
        return target

    def run(self):
        # get the output and branch data
        _output = self.output()
        _dataset_inst = self.branch_data["dataset_inst"]
        _keys = self.branch_data["keys"]
        _file_index = self.branch_data["file_index"]
        _n_events = self.branch_data["n_events"]

        # prepare arguments for the placeholders
        python_filename = _output.basename
        fileout = "file:{filename:s}".format(filename=os.path.basename(_keys[0]))

        # load the configuration template and replace placeholders for configuration filename,
        # production output file and number of events
        _input_config_template = self.input()["config_template"]
        template = Template(_input_config_template.load(formatter="text"))
        with _input_config_template.open(mode="r") as f:
            template = Template(f.read())
        content = template.render(
            python_filename=python_filename,
            cms_driver_fileout=fileout,
            number_of_events=_n_events,
        )

        # add luminosity block modifier to the end of the configuration file
        content += (
            "\n\nprocess.source.firstLuminosityBlock = cms.untracked.uint32(1 + "
            + "{file_index:d})".format(file_index=_file_index)
        )

        # write useable config to the output target
        _output.dump(content, formatter="text")
        self.publish_message(
            "successfully saved run config for dataset {dataset:s}, file {file_index:d}".format(
                dataset=_dataset_inst.name, file_index=_file_index
            )
        )


class AODSIMProduction(DatasetTask, HTCondorWorkflow, law.LocalWorkflow):
    config = "mc_ul18_fastsim_aodsim"

    def create_branch_map(self):
        branch_map = super(AODSIMProduction, self).create_branch_map()
        return branch_map

    def workflow_requires(self):
        reqs = {}
        reqs["config"] = AODSIMConfiguration.req(self, dataset=self.dataset, branches=self.branches)
        return reqs

    def requires(self):
        reqs = {}
        reqs["config"] = AODSIMConfiguration.req(self, dataset=self.dataset, branch=self.branch)
        return reqs

    def output(self):
        _keys = self.branch_data["keys"]
        parts = [p for p in _keys[0].split("/") if p.strip() != ""]
        target = self.remote_target(*parts)
        return target

    def run(self):
        # get the output and the branch data
        _output = self.output()
        _dataset_inst = self.branch_data["dataset_inst"]
        _file_index = self.branch_data["file_index"]

        # get the config file
        _input_config = self.input()["config"]

        # run the production in a temporary directory, copy input files before starting the
        # production
        tmp_dir = law.LocalDirectoryTarget(
            is_tmp=True, tmp_dir=os.path.expandvars("${PROD_TMPDIR}")
        )
        tmp_dir.touch()
        tmp_config = law.LocalFileTarget(os.path.join(tmp_dir.path, _input_config.basename))
        tmp_output = law.LocalFileTarget(os.path.join(tmp_dir.path, _output.basename))
        tmp_config.copy_from_local(_input_config)

        # run the production
        popen_kwargs = {"cwd": tmp_dir.path, "env": os.environ}
        p, lines = cms_run(tmp_config.basename, popen_kwargs=popen_kwargs, yield_output=True)
        for line in lines:
            print(line)
            if p.poll() is not None:
                break
        if p.returncode != 0:
            self.logger.error("cmsRun command failed, for command output see above")
            raise RuntimeError("cmsRun command failed")

        # write produced file to the output target
        _output.copy_from_local(tmp_output)
        self.publish_message(
            "successfully produced dataset {dataset:s}, file {file_index:d}".format(
                dataset=_dataset_inst.name, file_index=_file_index
            )
        )


class AnalysisAODSIMConfiguration(AnalysisTask, law.LocalWorkflow):
    config = "mc_ul18_fastsim_aodsim"

    exclude_params_req_get = {"workflow"}
    prefer_params_cli = {"workflow"}

    def create_branch_map(self):
        # create a map that associates each output file with a branch of the workflow
        branch_map = {}
        i = 0
        for _dataset_inst in self.config_inst.datasets:
            for file_index in range(_dataset_inst.n_files):
                branch_map[i] = {
                    "dataset_inst": _dataset_inst,
                    "file_index": file_index,
                    "keys": [_dataset_inst.keys[file_index]],
                    "n_events": _dataset_inst.get_aux("n_events_per_file"),
                }
                i += 1
        return branch_map

    def workflow_requires(self):
        reqs = {}
        for branch, branch_data in self.get_branch_map().items():
            _dataset_inst = branch_data["dataset_inst"]
            reqs[
                "config_template__branch_{branch:d}".format(branch=branch)
            ] = AODSIMConfigurationTemplate.req(self, dataset=_dataset_inst.name)
        return reqs

    def requires(self):
        reqs = {}
        _dataset_inst = self.branch_data["dataset_inst"]
        reqs["config_template"] = AODSIMConfigurationTemplate.req(self, dataset=_dataset_inst.name)
        return reqs

    def output(self):
        filename = os.path.basename(self.branch_data["keys"][0]).replace(".root", "_cfg.py")
        target = self.local_target(self.__class__.__name__, filename)
        return target

    def run(self):
        # get the output and branch data
        _output = self.output()
        _dataset_inst = self.branch_data["dataset_inst"]
        _keys = self.branch_data["keys"]
        _file_index = self.branch_data["file_index"]
        _n_events = self.branch_data["n_events"]

        # prepare arguments for the placeholders
        python_filename = _output.basename
        fileout = "file:{filename:s}".format(filename=os.path.basename(_keys[0]))

        # load the configuration template and replace placeholders for configuration filename,
        # production output file and number of events
        _input_config_template = self.input()["config_template"]
        template = Template(_input_config_template.load(formatter="text"))
        with _input_config_template.open(mode="r") as f:
            template = Template(f.read())
        content = template.render(
            python_filename=python_filename,
            cms_driver_fileout=fileout,
            number_of_events=_n_events,
        )

        # add luminosity block modifier to the end of the configuration file
        content += (
            "\n\nprocess.source.firstLuminosityBlock = cms.untracked.uint32(1 + "
            + "{file_index:d})".format(file_index=_file_index)
        )

        # write useable config to the output target
        _output.dump(content, formatter="text")
        self.logger.info(
            "successfully saved run config for dataset {dataset:s}, file {file_index:d}".format(
                dataset=_dataset_inst.name, file_index=_file_index
            )
        )


class AnalysisAODSIMProduction(AnalysisTask, HTCondorWorkflow, law.LocalWorkflow):
    config = "mc_ul18_fastsim_aodsim"

    def create_branch_map(self):
        # create a map that associates each output file with a branch of the workflow
        branch_map = {}
        i = 0
        for _dataset_inst in self.config_inst.datasets:
            for file_index in range(_dataset_inst.n_files):
                branch_map[i] = {
                    "dataset_inst": _dataset_inst,
                    "file_index": file_index,
                    "keys": [_dataset_inst.keys[file_index]],
                    "n_events": _dataset_inst.get_aux("n_events_per_file"),
                }
                i += 1
        return branch_map

    def workflow_requires(self):
        reqs = {}
        reqs["config"] = AnalysisAODSIMConfiguration.req(self, branches=self.branches)
        return reqs

    def requires(self):
        reqs = {}
        reqs["config"] = AnalysisAODSIMConfiguration.req(self, branch=self.branch)
        return reqs

    def output(self):
        _keys = self.branch_data["keys"]
        parts = [p for p in _keys[0].split("/") if p.strip() != ""]
        target = self.remote_target(*parts)
        return target

    def run(self):
        # get the output and the branch data
        _output = self.output()
        _dataset_inst = self.branch_data["dataset_inst"]
        _file_index = self.branch_data["file_index"]

        # get the config file
        _input_config = self.input()["config"]

        # run the production in a temporary directory, copy input files before starting the
        # production
        tmp_dir = law.LocalDirectoryTarget(
            is_tmp=True, tmp_dir=os.path.expandvars("${PROD_TMPDIR}")
        )
        tmp_dir.touch()
        tmp_config = law.LocalFileTarget(os.path.join(tmp_dir.path, _input_config.basename))
        tmp_output = law.LocalFileTarget(os.path.join(tmp_dir.path, _output.basename))
        tmp_config.copy_from_local(_input_config)

        # print information about production
        self.publish_message(
            "Producing dataset {dataset:s}, file {file_index:s}\n".format(
                dataset=_dataset_inst.name, file_index=_file_index
            )
            + ">> configuration:       {config:s}\n".format(config=tmp_config.basename)
            + ">> input dataset file:  {input:s}\n".format(input="-")
            + ">> output dataset file: {output:s}".format(output=tmp_output.basename)
        )

        # run the production
        popen_kwargs = {
            "cwd": tmp_dir.path,
            "env": os.environ,
        }
        p, lines = cms_run(tmp_config.basename, popen_kwargs=popen_kwargs, yield_output=True)
        for line in lines:
            print(line)
            if p.poll() is not None:
                break
        if p.returncode != 0:
            self.logger.error("cmsRun command failed")
            raise RuntimeError("cmsRun command failed")

        # write produced file to the output target
        _output.copy_from_local(tmp_output)
        self.publish_message(
            "successfully produced dataset {dataset:s}, file {file_index:d}".format(
                dataset=_dataset_inst.name, file_index=_file_index
            )
        )
