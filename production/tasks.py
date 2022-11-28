import csv
import law
import luigi
from luigi.util import inherits
import numpy as np
from production.framework import ProductionConfig, CMSSWTask, ConfigTask, ProductionTask
import os


class CreateFragments(ConfigTask, law.LocalWorkflow):

    name = "fragments"

    fragment_template_path = luigi.PathParameter(exists=True)
    mass_grid_definition_path = luigi.PathParameter(exists=True)

    def create_branch_map(self):
        higgs_mass, higgs_width, number_of_events = np.loadtxt(self.mass_grid_definition_path, comments="#", delimiter=",", unpack=True)
        branch_map = {}
        for i in range(len(higgs_mass)):
            branch_map[i] = {
                "higgs_mass": int(higgs_mass[i]),
                "higgs_width": float(higgs_width[i]),
                "number_of_events": int(number_of_events[i]),
            }
        return branch_map

    def output(self):
        filename = "GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_cff.py".format(
            higgs_mass=self.branch_data["higgs_mass"]
        )
        return self.local_target(filename)

    def run(self):
        higgs_mass = self.branch_data["higgs_mass"]
        higgs_width = self.branch_data["higgs_width"]
        with open(self.fragment_template_path, "r") as f:
            fragment_template = f.read()

        if higgs_width < 0.:
            process_parameters_block = "        processParameters = cms.vstring(\n" + \
                                       "            'HiggsSM:gg2H = on',\n" + \
                                       "            '25:onMode = off',\n" + \
                                       "            '25:onIfMatch = 15 -15',\n" + \
                                       "            '25:m0 = {higgs_mass:f}',\n".format(higgs_mass=higgs_mass) + \
                                       "            ),"
        else:
            process_parameters_block = "        processParameters = cms.vstring(\n" + \
                                       "            'HiggsSM:gg2H = on',\n" + \
                                       "            '25:onMode = off',\n" + \
                                       "            '25:onIfMatch = 15 -15',\n" + \
                                       "            '25:m0 = {higgs_mass:f}',\n".format(higgs_mass=higgs_mass) + \
                                       "            '25:mWidth = {higgs_width:f}',\n".format(higgs_width=higgs_width) + \
                                       "            '25:doForceWidth = on',\n" + \
                                       "            ),"
        fragment_template = fragment_template.replace("{{ process_parameters_block }}", process_parameters_block)
        self.output().dump(fragment_template, formatter="text")


class CopyToCMSSW(CMSSWTask, law.LocalWorkflow):

    name = "copy_to_cmssw"

    def create_branch_map(self):
        return CreateFragments.req(self).get_branch_map()

    def workflow_requires(self):
        return {
            "CreateFragments": CreateFragments.req(self, branches=self.branches),
        }

    def requires(self):
        return {
            "CreateFragments": CreateFragments.req(self, branch=self.branch),
        }

    def output(self):
        fragment_dir = os.path.join("src", "Configuration", "GenProduction", "python")
        filename = "GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_cff.py".format(
            higgs_mass=self.branch_data["higgs_mass"]
        )
        return self.local_target(os.path.join(fragment_dir, filename))

    def run(self):
        self.input()["CreateFragments"].copy_to_local(self.output().path)



#class CreateAODSimConfig(ConfigTask, law.LocalWorkflow):
