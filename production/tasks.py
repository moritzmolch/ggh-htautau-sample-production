import law
import os
import csv
import luigi
import tempfile
import subprocess

from production.framework import ProductionTask


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
                higgs_mass, higgs_width, number_of_events = (
                    float(row[0]),
                    float(row[1]),
                    int(row[2]),
                )
                higgs_mass_str = (
                    str(int(higgs_mass))
                    if higgs_mass == int(higgs_mass)
                    else str(higgs_mass).replace(".", "p")
                )
                basename = "GluGluHToTauTau_MH{higgs_mass:s}_pythia8_TuneCP5".format(
                    higgs_mass=higgs_mass_str
                )
                branch_map[i] = {
                    "basename": basename,
                    "fragment_filename": basename + "_cff.py",
                    "higgs_mass": higgs_mass,
                    "higgs_width": higgs_width,
                    "number_of_events": number_of_events,
                }
                i += 1
        return branch_map

    def output(self):
        return law.LocalFileTarget(
            os.path.join(
                self.cmssw_path,
                "src",
                "Configuration",
                "GenProduction",
                "python",
                self.branch_data["fragment_filename"],
            )
        )

    def run(self):
        _output = self.output()
        if not _output.parent.exists():
            _output.parent.touch()
        dataset = self.branch_data
        with open(self.fragment_template_path, "r") as f:
            fragment_content = f.read()
        if dataset["higgs_width"] == -1:
            process_parameters_block = (
                "processParameters = cms.vstring(\n"
                + "            'HiggsSM:gg2H = on',\n"
                + "            '25:onMode = off',\n"
                + "            '25:onIfMatch = 15 -15',\n"
                + "            '25:m0 = {0:f}',\n".format(dataset["higgs_mass"])
                + "        ),"
            )
        else:
            process_parameters_block = (
                "processParameters = cms.vstring(\n"
                + "            'HiggsSM:gg2H = on',\n"
                + "            '25:onMode = off',\n"
                + "            '25:onIfMatch = 15 -15',\n"
                + "            '25:m0 = {0:f}',\n".format(dataset["higgs_mass"])
                + "            '25:mWidth = {0:f}',\n".format(dataset["higgs_width"])
                + "            '25:doForceWidth = on',\n"
                + "        ),"
            )
        fragment_content = fragment_content.replace(
            "{{ process_parameters_block }}", process_parameters_block
        )
        with _output.open("w") as f:
            f.write(fragment_content)


class CreateAODSIMConfigTemplate(ProductionTask, law.LocalWorkflow):

    prod_config_path = luigi.PathParameter(exists=True)
    cmssw_path = luigi.PathParameter(exists=True)
    step = "aodsim"

    cms_driver_beamspot = "Realistic25ns13TeVEarly2018Collision"
    cms_driver_era = "Run2_2018_FastSim"
    cms_driver_conditions = "106X_upgrade2018_realistic_v16_L1v1"

    cms_driver_step = "GEN,SIM,RECOBEFMIX,DIGI,DATAMIX,L1,DIGI2RAW,L1Reco,RECO"
    cms_driver_datatier = "AODSIM"
    cms_driver_eventcontent = "AODSIM"

    cms_driver_proc_modifiers = "premix_stage2"
    cms_driver_datamix = "PreMix"
    cms_driver_pileup_input = (
        "dbs:/Neutrino_E-10_gun/RunIIFall17FSPrePremix-PUFSUL18CP5_106X_upgrade2018_realistic"
        + "_v16-v1/PREMIX"
    )

    cms_driver_add_monitoring = True
    cms_driver_use_random_service_helper = True
    cms_driver_use_fast_simulation = True
    cms_driver_is_mc_dataset = True

    def create_branch_map(self):
        branch_map = CreateFragment.req(self).get_branch_map()
        for branch in branch_map:
            basename = branch_map[branch]["basename"]
            aodsim_basename = "{basename:s}_{step:s}".format(
                basename=basename,
                step=self.step,
            )
            branch_map[branch]["aodsim_basename"] = aodsim_basename
            branch_map[branch]["aodsim_cfg_filename"] = aodsim_basename + "_cfg.py"
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
        return law.LocalFileTarget(
            os.path.join(
                self.prod_config_path,
                self.__class__.__name__,
                self.branch_data["aodsim_cfg_filename"],
            )
        )

    def get_relative_fragment_path(self):
        fragment_path = self.input()["CreateFragment"].path
        return os.path.relpath(
            fragment_path, start=os.path.join(self.cmssw_path, "src")
        )

    def get_root_input_filename(self):
        return None

    def get_root_output_filename(self):
        return  "{aodsim_basename:s}.root".format(aodsim_basename=self.branch_data["aodsim_basename"])

    def build_command(self):
        # base command and input file/fragment
        cmd = [
            "python3",
            "-m",
            "cmsDriver.py",
        ]
        arguments = []
        if (
            self.get_root_input_filename() is None
            and self.get_relative_fragment_path() is not None
        ):
            cmd.append(self.get_relative_fragment_path())
        elif (
            self.get_root_input_filename() is not None
            and self.get_relative_fragment_path() is None
        ):
            arguments.append(("--filein", "file:" + self.get_root_input_filename()))
        else:
            raise RuntimeError(
                "Exactly one of fragment path and root input file has to be set."
            )

        # python filename and output file
        arguments.append(("--python_filename", os.path.basename(self.output().path)))
        arguments.append(("--fileout", "file:" + self.get_root_output_filename()))

        # customization
        if self.cms_driver_add_monitoring:
            arguments.append(
                ("--customise", "Configuration/DataProcessing/Utils.addMonitoring")
            )
        if self.cms_driver_use_random_service_helper:
            arguments.append(
                (
                    "--customise_commands",
                    "from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper;"
                    + "randSvc = RandomNumberServiceHelper(process.RandomNumberGeneratorService);"
                    + "randSvc.populate()",
                )
            )

        # data-taking conditions
        arguments.append(("--beamspot", self.cms_driver_beamspot))
        arguments.append(("--era", self.cms_driver_era))
        arguments.append(("--conditions", self.cms_driver_conditions))

        # step and event format definition
        arguments.append(("--step", self.cms_driver_step))
        arguments.append(("--datatier", self.cms_driver_datatier))
        arguments.append(("--eventcontent", self.cms_driver_eventcontent))

        # premixing and pileup
        #arguments.append(("--procModifiers", self.cms_driver_proc_modifiers))
        #arguments.append(("--datamix", self.cms_driver_datamix))
        #arguments.append(("--pileup_input", self.cms_driver_pileup_input))

        # switches
        if self.cms_driver_use_fast_simulation:
            arguments.append(("--fast",))
        if self.cms_driver_is_mc_dataset:
            arguments.append(("--mc",))
        arguments.append(("--no_exec",))

        # number of events
        arguments.append(("-n", "-1"))

        # ravel the command
        return cmd + [substring for arg in arguments for substring in arg]

    def run(self):
        _output = self.output()
        if not _output.parent.exists():
            _output.parent.touch()
        with tempfile.TemporaryDirectory(dir=_output.parent.path) as tmpdir:
            cmd = self.build_command()
            print(f"Execute {cmd}")
            ret_code, out, err = law.util.interruptable_popen(
                cmd,
                cwd=tmpdir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=os.environ,
            )
            if ret_code != 0:
                raise RuntimeError(
                    "Command {cmd} failed with exit code {ret_code:d}".format(
                        cmd=cmd, ret_code=ret_code
                    )
                    + "Output: {out:s}".format(out=out)
                    + "Error: {err:s}".format(err=err)
                )
            python_filename = os.path.basename(_output.path)
            _output.copy_from_local(os.path.join(tmpdir, python_filename))


class SplitAODSIMConfigs(ProductionTask, law.LocalWorkflow):

    prod_config_path = luigi.PathParameter(exists=True)
    step_name = "aodsim"

    def create_branch_map(self):
        aodsim_branch_map = CreateAODSIMConfigTemplate.req(self).get_branch_map()
        branch_map = {}
        branch = 0
        for branch_data in aodsim_branch_map.values():
            n_events = branch_data["number_of_events"]
            i_start, i_stop = 0
            i = 0
            while i_start < n_events:
                if i_start < 10000:
                    i_stop = min(i_start + 2000, n_events)
                else:
                    i_stop = min(i_start + 4000, n_events)
                n_events_job = i_stop - i_start
                branch_map[branch] = branch_data
                branch_map["job_index"] = i
                branch_map[
                    "job_aodsim_basename"
                ] = "{aodsim_basename:s}_{index:d}".format(
                    aodsim_basename=branch_map["aodsim_basename"], index=i
                )
                branch_map[
                    "job_aodsim_config_filename"
                ] = "{job_aodsim_basename:s}_cfg.py".format(
                    job_aodsim_basename=branch_map["job_aodsim_basename"]
                )
                branch_map["job_number_of_events"] = n_events_job
                i_start = i_stop
                i += 1
                branch += 1
        return branch_map

    def workflow_requires(self):
        return {
            "CreateAODSIMConfigTemplate": CreateAODSIMConfigTemplate.req(self),
        }

    def requires(self):
        return {
            "CreateAODSIMConfigTemplate": CreateAODSIMConfigTemplate.req(self),
        }

    def output(self):
        return law.LocalFileTarget(
            os.path.join(
                self.prod_config_path,
                self.__class__.__name__,
                self.branch_data["job_aodsim_config_filename"],
            )
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
            self.branch_data["aodsim_basename"], self.branch_data["job_aodsim_basename"]
        )

        # replace number of events
        n_events = self.branch_data["job_number_of_events"]
        template = template.replace("-n -1", "-n {n:d}".format(n=n_events))
        template = template.replace(
            "input = cms.untracked.int32(-1)",
            "input = cms.untracked.int32({n:d})".format(n=n_events),
        )
        template = template.replace("nevts:-1", "nevts:{n:d}".format(n=n_events))

        # add luminosity block modifier
        template = template.replace(
            "# Customisation from command line",
            "# Customisation from command line\n\n"
            + "process.source.firstLuminosityBlock = cms.untracked.uint32({lumi_block:d})".format(
                lumi_block=self.branch_data["job_index"] + 1
            ),
        )

        # write modified configuration to destination
        _output.dump(template, formatter="text")
