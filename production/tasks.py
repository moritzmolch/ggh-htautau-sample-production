import law
import os
import csv
import luigi
import tempfile
import subprocess

law.contrib.load("cms")


class HTTDataset(object):

    def __init__(self, higgs_mass, higgs_width, number_of_events):
        self.higgs_mass = higgs_mass
        self.higgs_width = None
        self.has_manual_width = False if self.higgs_width == -1 else True
        if self.has_manual_width:
            self.higgs_width = higgs_width
        if int(self.higgs_mass) == self.higgs_mass:
            self.basename = "GluGluHToTauTau_MH{0:d}_pythia8_TuneCP5".format(int(self.higgs_mass))
        else:
            self.basename = "GluGluHToTauTau_MH{0:s}_pythia8_TuneCP5".format(str(self.higgs_mass).replace(".", "p"))
        self.number_of_events = number_of_events


class ProductionTask(law.Task):

    def __init__(self, *args, **kwargs):
        super(ProductionTask, self).__init__(*args, **kwargs)


class CreateFragment(ProductionTask, law.LocalWorkflow):

    cmssw_path = luigi.PathParameter(exists=True)
    fragment_template_path = luigi.PathParameter(exists=True)
    mass_grid_definition_path = luigi.PathParameter(exists=True)

    def create_branch_map(self):
        branch_map = {}
        with open(self.mass_grid_definition_path, "r") as f:
            csv_reader = csv.reader(f, delimiter=",")
            i = 0
            for row in csv_reader:
                if row[0].strip().startswith("#"):
                    continue
                branch_map[i] = HTTDataset(
                    higgs_mass=float(row[0]), higgs_width=float(row[1]), number_of_events=int(row[2])
                )
                i += 1
        return branch_map

    def output(self):
        return law.LocalFileTarget(os.path.join(
            self.cmssw_path, "src", "Configuration", "GenProduction", "python", self.branch_data.basename + "_cff.py"
        ))

    def run(self):
        _output = self.output()
        if not _output.parent.exists():
            _output.parent.touch()
        dataset = self.branch_data
        with open(self.fragment_template_path, "r") as f:
            fragment_content = f.read()
        if dataset.has_manual_width:
            process_parameters_block = "processParameters = cms.vstring(\n" + \
                                       "            'HiggsSM:gg2H = on',\n" + \
                                       "            '25:onMode = off',\n" + \
                                       "            '25:onIfMatch = 15 -15',\n" + \
                                       "            '25:m0 = {0:f}',\n".format(dataset.higgs_mass) + \
                                       "        ),"
        else:
            process_parameters_block = "processParameters = cms.vstring(\n" + \
                                       "            'HiggsSM:gg2H = on',\n" + \
                                       "            '25:onMode = off',\n" + \
                                       "            '25:onIfMatch = 15 -15',\n" + \
                                       "            '25:m0 = {0:f}',\n".format(dataset.higgs_mass) + \
                                       "            '25:mWidth = {0:f}',\n".format(dataset.higgs_width) + \
                                       "            '25:doForceWidth = on',\n" + \
                                       "        ),"
        fragment_content = fragment_content.replace("{{ process_parameters_block }}", process_parameters_block)
        with _output.open("w") as f:
            f.write(fragment_content)


class ConfigAODSIM(ProductionTask, law.LocalWorkflow):

    prod_config_path = luigi.PathParameter(exists=True)
    cmssw_path = luigi.PathParameter(exists=True)
    step_name = "aodsim"

    cms_driver_beamspot = "Realistic25ns13TeVEarly2018Collision"
    cms_driver_era = "Run2_2018_FastSim"
    cms_driver_conditions = "106X_upgrade2018_realistic_v16_L1v1"

    cms_driver_step = "GEN,SIM,RECOBEFMIX,DIGI,DATAMIX,L1,DIGI2RAW,L1Reco,RECO"
    cms_driver_datatier = "AODSIM"
    cms_driver_eventcontent = "AODSIM"

    cms_driver_proc_modifiers = "premix_stage2"
    cms_driver_datamix = "PreMix"
    cms_driver_pileup_input = "dbs:/Neutrino_E-10_gun/RunIIFall17FSPrePremix-PUFSUL18CP5_106X_upgrade2018_realistic_v16-v1/PREMIX"

    cms_driver_add_monitoring = True
    cms_driver_use_random_service_helper = True
    cms_driver_use_fast_simulation = True
    cms_driver_is_mc_dataset = True

    def create_branch_map(self):
        return CreateFragment.req(self).get_branch_map()

    def workflow_requires(self):
        return {
            "CreateFragment": CreateFragment.req(self),
        }

    def requires(self):
        return {
            "CreateFragment": CreateFragment.req(self),
        }

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self.prod_config_path, self.branch_data.basename + "_aodsim_cfg.py")
        )

    def get_relative_fragment_path(self):
        fragment_path = self.input()["CreateFragment"].path
        return os.path.relpath(fragment_path, start=os.path.join(self.cmssw_path, "src"))

    def get_root_input_filename(self):
        return None

    def get_root_output_filename(self):
        return self.branch_data.basename + "_aodsim.root"

    def build_command(self):
        # base command and input file/fragment
        cmd = ["python3", "-m", "cmsDriver.py", ]
        arguments = []
        if self.get_root_input_filename() is None and self.get_relative_fragment_path() is not None:
            cmd.append(self.get_relative_fragment_path())
        elif self.get_root_input_filename() is not None and self.get_relative_fragment_path() is None:
            arguments.append(("--filein", "file:" + self.get_root_input_filename()))
        else:
            raise RuntimeError("Exactly one of fragment path and root input file has to be set.")

        # python filename and output file
        arguments.append(("--python_filename", os.path.basename(self.output().path)))
        arguments.append(("--fileout", "file:" + self.get_root_output_filename()))

        # customization
        if self.cms_driver_add_monitoring:
            arguments.append(("--customise", "Configuration/DataProcessing/Utils.addMonitoring"))
        if self.cms_driver_use_random_service_helper:
            arguments.append((
                "--customise_commands",
                "from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper;" +
                "randSvc = RandomNumberServiceHelper(process.RandomNumberGeneratorService);" +
                "randSvc.populate()"
            ))

        # data-taking conditions
        arguments.append(("--beamspot", self.cms_driver_beamspot))
        arguments.append(("--era", self.cms_driver_era))
        arguments.append(("--conditions", self.cms_driver_conditions))

        # step and event format definition
        arguments.append(("--step", self.cms_driver_step))
        arguments.append(("--datatier", self.cms_driver_datatier))
        arguments.append(("--eventcontent", self.cms_driver_eventcontent))

        # premixing and pileup
        arguments.append(("--procModifiers", self.cms_driver_proc_modifiers))
        arguments.append(("--datamix", self.cms_driver_datamix))
        arguments.append(("--pileup_input", self.cms_driver_pileup_input))

        # switches
        if self.cms_driver_use_fast_simulation:
            arguments.append(("--fast", ))
        if self.cms_driver_is_mc_dataset:
            arguments.append(("--mc", ))
        arguments.append(("--no_exec", ))

        # number of events
        arguments.append(("-n", "-1"))

        # unravel the command
        return cmd + [substring for arg in arguments for substring in arg]

    def run(self):
        _output = self.output()
        if not _output.parent.exists():
            _output.parent.touch()
        with tempfile.TemporaryDirectory(dir=_output.parent.path) as tmpdir:
            cmd = self.build_command()
            ret_code, out, err = law.util.interruptable_popen(
                cmd, cwd=tmpdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=os.environ
            )
            if ret_code != 0:
                raise RuntimeError(
                    "Command {cmd} failed with exit code {ret_code:d}".format(cmd=cmd, ret_code=ret_code) +
                    "Output: {out:s}".format(out=out) +
                    "Error: {err:s}".format(err=err)
                )
            python_filename = os.path.basename(_output.path)
            _output.copy_from_local(os.path.join(tmpdir, python_filename))


class ConfigMINIAOD(ProductionTask, law.LocalWorkflow):

    prod_config_path = luigi.PathParameter()
    cmssw_path = luigi.PathParameter(exists=True)
    step_name = "miniaod"

    def create_branch_map(self):
        return ConfigAODSIM.req(self)

    def workflow_requires(self):
        return {
            "ConfigAODSIM": ConfigAODSIM.req(self),
        }

    def requires(self):
        return {
            "ConfigAODSIM": ConfigAODSIM.req(self),
        }

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self.prod_config_path, self.branch_data.basename + "_miniaod_cfg.py")
        )

    def get_root_input_filename(self):
        return self.ConfigAODSIM.req(self, branch=self.branch).get_root_output_filename()

    def get_root_output_filename(self):
        return self.branch_data.basename + "_miniaod.root"


class ConfigNANOAOD(ProductionTask, law.LocalWorkflow):

    prod_config_path = luigi.PathParameter()
    cmssw_path = luigi.PathParameter(exists=True)
    step_name = "nanoaod"

    def create_branch_map(self):
        return ConfigMINIAOD.req(self)

    def requires(self):
        return {
            "ConfigMINIAOD": ConfigMINIAOD.req(self),
        }

    def output(self):
        return law.LocalFileTarget(
            os.path.join(self.prod_config_path, self.branch_data.basename + "_nanoaod_cfg.py")
        )

    def get_root_input_filename(self):
        return self.ConfigMINIAOD.req(self, branch=self.branch).get_root_output_filename()

    def get_root_output_filename(self):
        return self.branch_data.basename + "_nanoaod.root"


class RunAODSIM(ProductionTask, law.LocalWorkflow):

    step_name = "aodsim"

    def requires(self):
        return {
            "ConfigAODSIM": ConfigAODSIM.req(self),
        }

    def output(self):
        return law.LocalFileTarget(os.path.join(self.prod_data_path, "aodsim", "sample.root"))


class RunMINIAOD(ProductionTask, law.LocalWorkflow):

    step_name = "miniaod"

    def requires(self):
        return {
            "ConfigMINIAOD": ConfigMINIAOD.req(self),
            "RunAODSIM": RunAODSIM.req(self),
        }

    def output(self):
        return law.LocalFileTarget(os.path.join(self.prod_data_path, "miniaod", "sample.root"))


class RunNANOAOD(ProductionTask, law.LocalWorkflow):

    step_name = "nanoaod"

    def requires(self):
        return {
            "ConfigNANOAOD": ConfigNANOAOD.req(self),
            "RunMINIAOD": RunMINIAOD.req(self),
        }

    def output(self):
        return law.LocalFileTarget(os.path.join(self.prod_data_path, "nanoaod", "sample.root"))
