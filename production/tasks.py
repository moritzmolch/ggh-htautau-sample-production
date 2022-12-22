import law
import os
import csv
import luigi

from production.utils import HTTDataset
from production.framework import Task, CMSDriverTask


class CreateFragment(Task, law.LocalWorkflow):

    cmssw_path = luigi.PathParameter(exists=True)
    fragment_template_path = luigi.PathParameter(exists=True)
    mass_grid_definition_path = luigi.PathParameter(exists=True)

    def create_branch_map(self):
        branch_map = {}
        with open(self.mass_grid_definition_path, "r") as f:
            csv_reader = csv.reader(f, delimiter=",")
            branch = 0
            for row in csv_reader:
                if row[0].strip().startswith("#"):
                    continue
                higgs_mass, higgs_width, number_of_events = (
                    float(row[0]),
                    float(row[1]),
                    int(row[2]),
                )
                branch_map[branch] = HTTDataset(higgs_mass, higgs_width, number_of_events, branch=branch)
                branch += 1
        return branch_map

    def output(self):
        return law.LocalFileTarget(
            os.path.join(
                self.cmssw_path,
                "src",
                "Configuration",
                "GenProduction",
                "python",
                self.branch_data.get_fragment_filename(),
            )
        )

    def run(self):
        _output = self.output()
        if not _output.parent.exists():
            _output.parent.touch()
        dataset = self.branch_data
        with open(self.fragment_template_path, "r") as f:
            fragment_content = f.read()
        if dataset.has_modified_higgs_width:
            process_parameters_block = (
                "processParameters = cms.vstring(\n"
                + "            'HiggsSM:gg2H = on',\n"
                + "            '25:onMode = off',\n"
                + "            '25:onIfMatch = 15 -15',\n"
                + "            '25:m0 = {0:f}',\n".format(dataset.higgs_mass)
                + "            '25:mWidth = {0:f}',\n".format(dataset.higgs_width)
                + "            '25:doForceWidth = on',\n"
                + "        ),"
            )
        else:
            process_parameters_block = (
                "processParameters = cms.vstring(\n"
                + "            'HiggsSM:gg2H = on',\n"
                + "            '25:onMode = off',\n"
                + "            '25:onIfMatch = 15 -15',\n"
                + "            '25:m0 = {0:f}',\n".format(dataset.higgs_mass)
                + "        ),"
            )
        fragment_content = fragment_content.replace("{{ process_parameters_block }}", process_parameters_block)
        _output.dump(fragment_content, formatter="text")


class CreateAODSIMConfigTemplate(Task, CMSDriverTask, law.LocalWorkflow):

    def create_branch_map(self):
        branch_map = CreateFragment.req(self).get_branch_map()
        return branch_map

    def workflow_requires(self):
        return {
            "CreateFragment": CreateFragment.req(self),
        }

    def requires(self):
        return {
            "CreateFragment": CreateFragment.req(self),
        }

    def output(self):
        return self.local_target(
            self.branch_data.get_step_config_filename("aodsim"),
        )

    def cmssw_fragment_path(self):
        fragment_path = self.input()["CreateFragment"].path
        return os.path.relpath(fragment_path, start=os.path.join(self.cmssw_path, "src"))

    def root_input_filename(self):
        return None

    def root_output_filename(self):
        return self.branch_data.get_step_root_filename("aodsim")


class CreateMINIAODSIMConfigTemplate(Task, CMSDriverTask, law.LocalWorkflow):

    def create_branch_map(self):
        branch_map = CreateFragment.req(self).get_branch_map()
        return branch_map

    def output(self):
        return self.local_target(
            self.branch_data.get_step_config_filename("miniaod"),
        )

    def cmssw_fragment_path(self):
        return None

    def root_input_filename(self):
        return self.branch_data.get_step_root_filename("aodsim")

    def root_output_filename(self):
        return self.branch_data.get_step_root_filename("miniaod")


class CreateNANOAODSIMConfigTemplate(Task, CMSDriverTask, law.LocalWorkflow):

    def create_branch_map(self):
        branch_map = CreateFragment.req(self).get_branch_map()
        return branch_map

    def output(self):
        return self.local_target(
            self.branch_data.get_step_config_filename("nanoaod"),
        )

    def cmssw_fragment_path(self):
        return None

    def root_input_filename(self):
        return self.branch_data.get_step_root_filename("miniaod")

    def root_output_filename(self):
        return self.branch_data.get_step_root_filename("nanoaod")


class SplitAODSIMConfigs(Task, law.LocalWorkflow):

    def create_branch_map(self):
        aodsim_branch_map = CreateAODSIMConfigTemplate.req(self).get_branch_map()
        branch_map = {}
        branch = 0
        for parent_branch, branch_data in aodsim_branch_map.items():
            htt_files = branch_data.files
            for htt_file in htt_files:
                htt_file.branch = branch
                branch_map[branch] = htt_file
                branch += 1
        return branch_map

    def workflow_requires(self):
        return {
            "CreateAODSIMConfigTemplate": CreateAODSIMConfigTemplate.req(self),
        }

    def requires(self):
        return {
            "CreateAODSIMConfigTemplate": CreateAODSIMConfigTemplate.req(self, branch=self.branch_data.dataset.branch),
        }

    def output(self):
        return self.local_target(
            self.branch_data.get_step_config_filename("aodsim")
        )

    def run(self):

        # check if parent directory of output exists
        _output = self.output()
        if not _output.parent.exists():
            _output.parent.touch()

        # load config template
        _input = self.input()["CreateAODSIMConfigTemplate"]
        template = _input.load(formatter="text")

        # replace filenames
        template = template.replace(
            self.branch_data.dataset.get_step_config_filename("aodsim"),
            self.branch_data.get_step_config_filename("aodsim"),
        )
        template = template.replace(
            self.branch_data.dataset.get_step_root_filename("aodsim"),
            self.branch_data.get_step_root_filename("aodsim"),
        )

        # replace number of events
        n_events = self.branch_data.number_of_events
        template = template.replace("-n -1", "-n {n:d}".format(n=n_events))
        template = template.replace(
            "input = cms.untracked.int32(-1)",
            "input = cms.untracked.int32({n:d})".format(n=n_events),
        )
        template = template.replace("nevts:-1", "nevts:{n:d}".format(n=n_events))

        # add luminosity block modifier
        template += "\n\nprocess.source.firstLuminosityBlock = cms.untracked.uint32(1 + {lumi_block:d})".format(
                lumi_block=self.branch_data.index
        )

        # write modified configuration to destination
        _output.dump(template, formatter="text")


class SplitMINIAODSIMConfigs(Task, law.LocalWorkflow):

    def create_branch_map(self):
        miniaodsim_branch_map = CreateMINIAODSIMConfigTemplate.req(self).get_branch_map()
        branch_map = {}
        branch = 0
        for parent_branch, branch_data in miniaodsim_branch_map.items():
            htt_files = branch_data.files
            for htt_file in htt_files:
                htt_file.branch = branch
                branch_map[branch] = htt_file
                branch += 1
        return branch_map

    def workflow_requires(self):
        return {
            "CreateMINIAODSIMConfigTemplate": CreateMINIAODSIMConfigTemplate.req(self),
        }

    def requires(self):
        return {
            "CreateMINIAODSIMConfigTemplate": CreateMINIAODSIMConfigTemplate.req(self, branch=self.branch_data.dataset.branch),
        }

    def output(self):
        return self.local_target(
            self.branch_data.get_step_config_filename("miniaod")
        )

    def run(self):

        # check if parent directory of output exists
        _output = self.output()
        if not _output.parent.exists():
            _output.parent.touch()

        # load config template
        _input = self.input()["CreateMINIAODSIMConfigTemplate"]
        template = _input.load(formatter="text")

        # replace filenames
        template = template.replace(
            self.branch_data.dataset.get_step_config_filename("miniaod"),
            self.branch_data.get_step_config_filename("miniaod"),
        )
        template = template.replace(
            self.branch_data.dataset.get_step_root_filename("miniaod"),
            self.branch_data.get_step_root_filename("miniaod"),
        )

        # write modified configuration to destination
        _output.dump(template, formatter="text")


class SplitNANOAODSIMConfigs(Task, law.LocalWorkflow):

    def create_branch_map(self):
        nanoaodsim_branch_map = CreateNANOAODSIMConfigTemplate.req(self).get_branch_map()
        branch_map = {}
        branch = 0
        for parent_branch, branch_data in nanoaodsim_branch_map.items():
            htt_files = branch_data.files
            for htt_file in htt_files:
                htt_file.branch = branch
                branch_map[branch] = htt_file
                branch += 1
        return branch_map

    def workflow_requires(self):
        return {
            "CreateNANOAODSIMConfigTemplate": CreateNANOAODSIMConfigTemplate.req(self),
        }

    def requires(self):
        return {
            "CreateNANOAODSIMConfigTemplate": CreateNANOAODSIMConfigTemplate.req(self, branch=self.branch_data.dataset.branch),
        }

    def output(self):
        return self.local_target(
            self.branch_data.get_step_config_filename("nanoaod")
        )

    def run(self):

        # check if parent directory of output exists
        _output = self.output()
        if not _output.parent.exists():
            _output.parent.touch()

        # load config template
        _input = self.input()["CreateNANOAODSIMConfigTemplate"]
        template = _input.load(formatter="text")

        # replace filenames
        template = template.replace(
            self.branch_data.dataset.get_step_config_filename("nanoaod"),
            self.branch_data.get_step_config_filename("nanoaod"),
        )
        template = template.replace(
            self.branch_data.dataset.get_step_root_filename("nanoaod"),
            self.branch_data.get_step_root_filename("nanoaod"),
        )

        # write modified configuration to destination
        _output.dump(template, formatter="text")
