import law
import os
import luigi
import subprocess
from abc import ABCMeta, abstractmethod




class Task(law.Task):

    local_data_path = luigi.Parameter()
    cmssw_path = luigi.Parameter()


    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

    def local_path(self, *path):
        parts = (self.local_data_path, self.__class__.__name__) + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(path))


class CMSDriverTask(law.Task):

    __metaclass__ = ABCMeta

    cms_driver_era = luigi.Parameter(description="Tag for the data-taking period")
    cms_driver_conditions = luigi.Parameter(
        description="Tag for the data-taking conditions that affect alignment and calibration"
    )
    cms_driver_beamspot = luigi.Parameter(description="Tag for the beamspot scenario")

    cms_driver_step = luigi.Parameter(
        description="Simulation steps that are performed during one part of the simulation chain"
    )
    cms_driver_datatier = luigi.Parameter(description="Data tier of the chain step")
    cms_driver_eventcontent = luigi.Parameter(
        description="Format in which the events are stord in the ROOT output file"
    )

    cms_driver_proc_modifiers = luigi.Parameter(
        default=law.NO_STR, description="Tag for processing modifications, e.g. for premixing or during the miniAOD step"
    )
    cms_driver_datamix = luigi.Parameter(default=law.NO_STR, description="Type of pileup mixing")
    cms_driver_pileup_input = luigi.Parameter(default=law.NO_STR, description="Input file for pileup mixing")

    cms_driver_geometry = luigi.Parameter(default=law.NO_STR, description="Selection of the detector geometry description")

    cms_driver_fast = luigi.BoolParameter(
        default=False, description="Run the CMS Fast Simulation instead of the full detector simulation with GEANT"
    )
    cms_driver_mc = luigi.BoolParameter(default=False, description="Declare processing of Monte Carlo data")

    cms_driver_add_monitoring = luigi.BoolParameter(
        default=False, description="Activate monitoring tools when running the configuration"
    )
    cms_driver_use_random_service_helper = luigi.BoolParameter(
        default=False, description="Provide random seed to event generators"
    )
    cms_driver_run_unscheduled = luigi.BoolParameter(default=False, description="Run the production unscheduled")

    cms_driver_number_of_events = luigi.IntParameter(default=-1, description="Number of events")

    @abstractmethod
    def output(self):
        pass

    @abstractmethod
    def cmssw_fragment_path(self) -> str | None:
        pass

    @abstractmethod
    def root_input_filename(self) -> str | None:
        pass

    @abstractmethod
    def root_output_filename(self) -> str:
        pass

    def has_valid_command_inputs(self):
        if self.cmssw_fragment_path() is None and isinstance(self.root_input_filename(), str):
            return True
        if isinstance(self.cmssw_fragment_path(), str) and self.root_input_filename() is None:
            return True
        return False

    def build_command(self):
        if not self.has_valid_command_inputs():
            raise RuntimeError(
                "Command cannot be constructed: Fragment path or root input file have to be defined. The 'or' has "
                + "to be interpreted as 'XOR' ('exclusive or') here."
            )

        # basic cms driver command
        cmd = [
            "cmsDriver.py",
        ]

        # set the input of the production step (fragment or ROOT input file)
        if self.get_relative_fragment_path() is not None:
            cmd.append(self.get_relative_fragment_path())
        if self.get_root_input_filename() is not None:
            cmd += ["--filein", "file:" + self.get_root_input_filename()]

        # set the python filename and the name of the output file
        cmd += ["--python_filename", os.path.basename(self.output().path)]
        cmd += ["--fileout", "file:" + self.get_root_output_filename()]

        # add customization flags (monotoring and random seed)
        # customization
        if self.cms_driver_add_monitoring:
            cmd += ["--customise", "Configuration/DataProcessing/Utils.addMonitoring"]
        if self.cms_driver_use_random_service_helper:
            cmd += [
                "--customise_commands",
                "from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper;randSvc = "
                + "RandomNumberServiceHelper(process.RandomNumberGeneratorService);randSvc.populate()",
            ]

        # data-taking conditions (required)
        cmd += ["--era", self.cms_driver_era]
        cmd += ["--conditions", self.cms_driver_conditions]
        cmd += ["--beamspot", self.cms_driver_beamspot]

        # step and event format definition (required)
        cmd += ["--step", self.cms_driver_step]
        cmd += ["--datatier", self.cms_driver_datatier]
        cmd += ["--eventcontent", self.cms_driver_eventcontent]

        # process modification tag (optional)
        if self.cms_driver_proc_modifiers != law.NO_STR:
            cmd += ["--procModifiers", self.cms_driver_proc_modifiers]

        # premixing and pileup (optional)
        if self.cms_driver_datamix != law.NO_STR:
            cmd += ["--datamix", self.cms_driver_datamix]
        if self.cms_driver_pileup_input != law.NO_STR:
            cmd += ["--pileup_input", self.cms_driver_pileup_input]

        # detector geometry (optional)
        if self.cms_driver_geometry != law.NO_STR:
            cmd += ["--geometry", self.cms_driver_geometry]

        # fast simulation of the CMS detector (optional)
        if self.cms_driver_fast:
            cmd.append("--fast")

        # generation and processing of Monte Carlo data (optional)
        if self.cms_driver_is_mc_dataset:
            cmd.append("--mc")

        # run unscheduled production
        if self.cms_driver_run_unscheduled:
            cmd.append("--runUnscheduled")

        # do not execute the simulation while creating the configuration
        # This flag is always set here as the production should not be started in the configuration task.
        cmd.append("--no_exec")

        # set number of events
        cmd += ["-n", str(self.cms_driver_number_of_events)]

        return cmd

    def run_command(self, command, output):
        # generate the configuration file
        ret_code, out, err = law.util.interruptable_popen(
            command,
            cwd=output.parent.path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=os.environ,
        )

        if ret_code != 0:
            raise RuntimeError(
                "Command {cmd} failed with exit code {ret_code:d}".format(cmd=command, ret_code=ret_code)
                + "Output: {out:s}".format(out=out)
                + "Error: {err:s}".format(err=err)
            )
        elif not output.exists():
            raise RuntimeError("Output file {output:s} does not exist".format(output=output.path))

    def run(self):

        # check if parent directory exists, create it if this is not the case
        _output = self.output()
        if not _output.parent.exists():
            _output.parent.touch()

        # build and execute the configuration generation command
        cmd = self.build_command()
        self.run_command(cmd, _output.parent.path)
