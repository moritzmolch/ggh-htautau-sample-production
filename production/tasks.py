import law
import luigi
import numpy as np
import os
from production.framework import CMSSWTask, ConfigTask
import subprocess
import tempfile


class CreateFragments(ConfigTask, law.LocalWorkflow):

    name = "fragments"

    fragment_template_path = luigi.PathParameter(exists=True)
    mass_grid_definition_path = luigi.PathParameter(exists=True)

    def create_branch_map(self):
        higgs_mass, higgs_width, number_of_events = np.loadtxt(self.mass_grid_definition_path, comments="#", delimiter=",", unpack=True)
        branch_map = {}
        for i in range(len(higgs_mass)):
            sample_basename = "GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5".format(higgs_mass=int(higgs_mass[i]))
            branch_map[i] = {
                "higgs_mass": float(higgs_mass[i]),
                "higgs_width": float(higgs_width[i]),
                "number_of_events": int(number_of_events[i]),
                "sample_basename": sample_basename,
            }
        return branch_map

    def output(self):
        filename = self.branch_data["sample_basename"] + "_cff.py"
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
        filename = self.branch_data["sample_basename"] + "_cff.py"
        return self.local_target(os.path.join(fragment_dir, filename))

    def run(self):
        self.input()["CreateFragments"].copy_to_local(self.output().path)


class CreateAODSimConfig(ConfigTask, law.SandboxTask, law.LocalWorkflow):

    name = "aodsim_config"
    sandbox = "bash::${PROD_BASE_PATH}/sandboxes/cmssw_env.sh"

    beamspot = "Realistic25ns13TeVEarly2018Collision"
    era = "Run2_2018_FastSim"
    conditions = "106X_upgrade2018_realistic_v16_L1v1"

    step = "GEN,SIM,RECOBEFMIX,DIGI,DATAMIX,L1,DIGI2RAW,L1Reco,RECO"
    datatier = "AODSIM"
    eventcontent = "AODSIM"

    proc_modifiers = "premix_stage2"
    datamix = "PreMix"
    pileup_input = "dbs:/Neutrino_E-10_gun/RunIIFall17FSPrePremix-PUFSUL18CP5_106X_upgrade2018_realistic_v16-v1/PREMIX"

    add_monitoring = True    
    use_random_service_helper = True
    use_fast_simulation = True
    is_mc_dataset = True

    def create_branch_map(self):
        return CopyToCMSSW.req(self).get_branch_map()

    def workflow_requires(self):
        return {
            "CopyToCMSSW": CopyToCMSSW.req(self, branches=self.branches),
        }

    def requires(self):
        return {
            "CopyToCMSSW": CopyToCMSSW.req(self, branch=self.branch),
        }

    def output(self):
        return self.local_target(self.branch_data["sample_basename"] + "_aodsim_cfg.py")

    def build_command(self, fragment_path, python_filename, output_filename, number_of_events):
        # fragment path
        arguments = [fragment_path, ]

        # python filename
        arguments.append(("--python_filename", python_filename))
        arguments.append(("--fileout", "file:" + output_filename))

        # customization
        if self.add_monitoring:
            arguments.append(("--customise", "Configuration/DataProcessing/Utils.addMonitoring"))
        if self.use_random_service_helper:
            arguments.append((
                "--customise_commands",
                "from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper;" +
                "randSvc = RandomNumberServiceHelper(process.RandomNumberGeneratorService);" +
                "randSvc.populate()"
            ))

        # data-taking conditions
        arguments.append(("--beamspot", self.beamspot))
        arguments.append(("--era", self.era))
        arguments.append(("--conditions", self.conditions))

        # step and event format definition
        arguments.append(("--step", self.step))
        arguments.append(("--datatier", self.datatier))
        arguments.append(("--eventcontent", self.eventcontent))

        # premixing and pileup
        arguments.append(("--procModifiers", self.proc_modifiers))
        arguments.append(("--datamix", self.datamix))
        arguments.append(("--pileup_input", self.pileup_input))

        # switches
        if self.use_fast_simulation:
            arguments.append(("--fast", ))
        if self.is_mc_dataset:
            arguments.append(("--mc", ))
        arguments.append(("--no_exec", ))

        # number of events
        arguments.append(("-n", "{n:d}".format(n=number_of_events)))

        # unravel the command
        return ["cmsDriver.py", ] + [substring for arg in arguments for substring in arg]

    def run(self):
        fragment_path = law.util.rel_path(self.input()["CopyToCMSSW"].path, self.cmssw_path)
        output = self.output()
        python_filename = os.path.basename(output.path)
        output_filename = self.branch_data["sample_basename"] + "_aodsim.root"
        if not output.parent.exists():
            output.parent.touch()
        with tempfile.TemporaryDirectory(dir=output.parent.path) as tmpdir:
            cmd = self.build_command(fragment_path, python_filename, output_filename, self.branch_data["number_of_events"])
            print(cmd)
            #ret_code, out, err = law.util.interruptable_popen(
            #    cmd, cwd=tmpdir, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            #)
            #if ret_code != 0:
            #    raise RuntimeError(
            #        "Command {cmd} failed with exit code {ret_code:d}".format(cmd=cmd, ret_code=ret_code) +
            #        "Output: {out:s}".format(out=out) +
            #        "Error: {err:s}".format(err=err)
            #    )
            #output.copy_from_local(os.path.join(tmpdir, python_filename))
