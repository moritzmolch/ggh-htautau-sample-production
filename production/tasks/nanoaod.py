from jinja2 import Template
import law
import order as od
import os
import subprocess

from production.tasks.miniaod import MINIAODProduction
from production.tasks.base import AnalysisTask, DatasetTask, HTCondorWorkflow
from production.util.cms import cms_driver, cms_run

law.contrib.load("tasks", "wlcg")


class NANOAODConfigurationTemplate(DatasetTask):
    config = "mc_ul18_fastsim_nanoaod"

    filein_placeholder = "{{ cms_driver_filein }}"
    fileout_placeholder = "{{ cms_driver_fileout }}"

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

        # prepare arguments for the cmsDriver command
        tmp_python_filename = _output.basename.replace(".j2", "")
        filein = self.filein_placeholder
        fileout = self.fileout_placeholder

        cms_driver_kwargs = _config_inst.get_aux("cms_driver_kwargs")
        cms_driver_kwargs["python_filename"] = tmp_python_filename
        cms_driver_kwargs["filein"] = filein
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
            fragment=None,
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

        # inject template placeholders for python filename
        content = content.replace(tmp_python_filename, "{{ python_filename }}")

        # write the config template to the output target
        _output.dump(content, formatter="text")
        self.publish_message(
            "successfully saved run config template for dataset {dataset:s}".format(
                dataset=_dataset_inst.name
            )
        )


class NANOAODConfiguration(DatasetTask, law.LocalWorkflow):
    config = "mc_ul18_fastsim_nanoaod"

    exclude_params_req_get = {"workflow"}
    prefer_params_cli = {"workflow"}

    def create_branch_map(self):
        # a generic branch map that associates each file of the dataset with a branch
        _config_previous_inst = self.analysis_inst.get_config(
            self.config_inst.get_aux("config_previous")
        )
        _dataset_previous_inst = _config_previous_inst.get_dataset(
            self.dataset_inst.name(
                self.config_inst.get_aux("step"), _config_previous_inst.get_aux("step")
            )
        )
        branch_map = {}
        for i in range(self.dataset_inst.n_files):
            branch_map[i] = {
                "dataset_inst": self.dataset_inst,
                "dataset_previous_inst": _dataset_previous_inst,
                "file_index": i,
                "key": self.dataset_inst.keys[i],
                "key_previous": _dataset_previous_inst.keys[i],
                "n_events": self.dataset_inst.get_aux("n_events_per_file"),
            }
        return branch_map

    def workflow_requires(self):
        reqs = {}
        reqs["config_template"] = NANOAODConfigurationTemplate.req(self, dataset=self.dataset)
        return reqs

    def requires(self):
        reqs = {}
        reqs["config_template"] = NANOAODConfigurationTemplate.req(self, dataset=self.dataset)
        return reqs

    def output(self):
        filename = os.path.basename(self.branch_data["key"]).replace(".root", "_cfg.py")
        target = self.local_target(self.__class__.__name__, filename)
        return target

    def run(self):
        # get the output and branch data
        _output = self.output()
        _dataset_inst = self.branch_data["dataset_inst"]
        _file_index = self.branch_data["file_index"]
        _key = self.branch_data["key"]
        _key_previous = self.branch_data["key_previous"]

        # prepare arguments for the placeholders
        python_filename = _output.basename
        filein = "file:{filename:s}".format(filename=os.path.basename(_key_previous))
        fileout = "file:{filename:s}".format(filename=os.path.basename(_key))

        # load the configuration template and replace placeholders for configuration filename,
        # production output file and number of events
        _input_config_template = self.input()["config_template"]
        template = Template(_input_config_template.load(formatter="text"))
        with _input_config_template.open(mode="r") as f:
            template = Template(f.read())
        content = template.render(
            python_filename=python_filename,
            cms_driver_filein=filein,
            cms_driver_fileout=fileout,
        )

        # write useable config to the output target
        _output.dump(content, formatter="text")
        self.publish_message(
            "successfully saved run config for dataset {dataset:s}, file {file_index:d}".format(
                dataset=_dataset_inst.name, file_index=_file_index
            )
        )


class NANOAODProduction(AnalysisTask, HTCondorWorkflow, law.LocalWorkflow):
    config = "mc_ul18_fastsim_nanoaod"

    def create_branch_map(self):
        # a generic branch map that associates each file of each dataset of the analysis with a
        # branch
        _config_previous_inst = self.analysis_inst.get_config(
            self.config_inst.get_aux("config_previous")
        )
        _dataset_previous_inst = _config_previous_inst.get_dataset(
            self.dataset_inst.name(
                self.config_inst.get_aux("step"), _config_previous_inst.get_aux("step")
            )
        )
        branch_map = {}
        for i in range(self.dataset_inst.n_files):
            branch_map[i] = {
                "dataset_inst": self.dataset_inst,
                "dataset_previous_inst": _dataset_previous_inst,
                "file_index": i,
                "key": self.dataset_inst.keys[i],
                "key_previous": _dataset_previous_inst.keys[i],
                "n_events": self.dataset_inst.get_aux("n_events_per_file"),
            }
        return branch_map

    def workflow_requires(self):
        datasets = od.UniqueObjectIndex(od.Dataset)
        for branch_data in self.get_branch_map().values():
            datasets.add(branch_data["dataset_inst"], overwrite=True)
        reqs = {}
        for i, _dataset_inst in enumerate(datasets):
            reqs["config_{i:d}".format(i=i)] = NANOAODConfiguration.req(
                self, dataset=_dataset_inst.name
            )
        reqs["aodsim"] = MINIAODProduction.req(self, branches=self.branches)
        return reqs

    def requires(self):
        _dataset_inst = self.branch_data["dataset_inst"]
        reqs = {}
        reqs["config"] = NANOAODConfiguration.req(self, dataset=_dataset_inst.name)
        reqs["aodsim"] = MINIAODProduction.req(self, branch=self.branch)
        return reqs

    def output(self):
        _key = self.branch_data["key"]
        parts = [p for p in _key.split("/") if p.strip() != ""]
        target = self.remote_target(*parts)
        return target

    def run(self):
        # get the output and the branch data
        _output = self.output()
        _dataset_inst = self.branch_data["dataset_inst"]
        _file_index = self.branch_data["file_index"]

        # get the config file and the input AODSIM dataset
        _input_config = self.input()["config"]
        _input_aodsim = self.input()["aodsim"]

        # run the production in a temporary directory, copy input files before starting the
        # production
        tmp_dir = law.LocalDirectoryTarget(
            is_tmp=True, tmp_dir=os.path.expandvars("${PROD_TMPDIR}")
        )
        tmp_dir.touch()
        tmp_config = law.LocalFileTarget(os.path.join(tmp_dir.path, _input_config.basename))
        tmp_input_aodsim = law.LocalFileTarget(os.path.join(tmp_dir.path, _input_aodsim.basename))
        tmp_output = law.LocalFileTarget(os.path.join(tmp_dir.path, _output.basename))
        tmp_config.copy_from_local(_input_config)
        _input_aodsim.copy_to_local(tmp_input_aodsim)

        # print information about production
        self.publish_message(
            "Producing dataset {dataset:s}, file {file_index:s}\n".format(
                dataset=_dataset_inst.name, file_index=_file_index
            )
            + ">> configuration:       {config:s}\n".format(config=tmp_config.basename)
            + ">> input dataset file:  {input:s}\n".format(input=tmp_input_aodsim.basename)
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
