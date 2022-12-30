import law
import luigi
import os
import subprocess


class BaseTask(law.Task):

    store = os.path.expandvars("${PROD_BASE}/data")

    def __init__(self, *args, **kwargs):
        super(BaseTask, self).__init__(*args, **kwargs)

    def local_path(self, *path):
        parts = (self.store,) + path
        return os.path.join(*parts)

    def local_target(self, *path, **kwargs):
        target_class = law.LocalDirectoryTarget if kwargs.pop("dir", False) else law.LocalFileTarget
        return target_class(self.local_path(*path))


class CMSDriverTask(law.SandboxTask):

    sandbox = "bash::${PROD_BASE}/sandboxes/cmssw_default.sh"

    # cms_driver_python_filename = luigi.Parameter(
    #    description="name of the python configuration file",
    # )

    cms_driver_fragment = luigi.Parameter(
        default=law.NO_STR,
        description="name of the fragment that serves as template for the configuration of the root production task",
    )
    cms_driver_filein = luigi.Parameter(
        default=law.NO_STR,
        description="input ROOT file; can have a protocol prefix, e.g. 'file:root_input_file.root'",
    )
    cms_driver_fileout = luigi.Parameter(
        description="input ROOT file; can have a protocol prefix, e.g. 'file:root_input_file.root'",
    )

    cms_driver_era = luigi.Parameter(
        description="Tag for the data-taking period",
    )
    cms_driver_conditions = luigi.Parameter(
        description="Tag for the data-taking conditions that affect alignment and calibration",
    )
    cms_driver_beamspot = luigi.Parameter(
        description="Tag for the beamspot scenario",
    )

    cms_driver_step = luigi.Parameter(
        description="Simulation steps that are performed during one part of the simulation chain",
    )
    cms_driver_datatier = luigi.Parameter(
        description="Data tier of the chain step",
    )
    cms_driver_eventcontent = luigi.Parameter(
        description="Format in which the events are stord in the ROOT output file"
    )

    cms_driver_proc_modifiers = luigi.Parameter(
        default=law.NO_STR,
        description="Tag for processing modifications, e.g. for premixing or during the miniAOD step",
    )
    cms_driver_datamix = luigi.Parameter(
        default=law.NO_STR,
        description="Type of pileup mixing",
    )
    cms_driver_pileup_input = luigi.Parameter(
        default=law.NO_STR,
        description="Input file for pileup mixing",
    )

    cms_driver_geometry = luigi.Parameter(
        default=law.NO_STR,
        description="Selection of the detector geometry description",
    )

    cms_driver_fast = luigi.BoolParameter(
        default=False,
        description="Run the CMS Fast Simulation instead of the full detector simulation with GEANT",
    )
    cms_driver_mc = luigi.BoolParameter(
        default=False,
        description="Declare processing of Monte Carlo data",
    )

    cms_driver_add_monitoring = luigi.BoolParameter(
        default=False,
        description="Activate monitoring tools when running the configuration",
    )
    cms_driver_use_random_service_helper = luigi.BoolParameter(
        default=False,
        description="Provide random seed to event generators",
    )
    cms_driver_run_unscheduled = luigi.BoolParameter(
        default=False,
        description="Run the production unscheduled",
    )

    cms_driver_number_of_events = luigi.IntParameter(
        default=-1,
        description="Number of events",
    )

    def build_command(self, python_filename):
        command = ["cmsDriver.py"]
        # check if either fragment or input file are set
        if self.cms_driver_fragment == law.NO_STR and self.cms_driver_filein == law.NO_STR:
            raise ValueError("Command cannot be constructed: Either 'fragment' or 'filein' must be set.")

        # the cms driver command
        cmd = [
            "cmsDriver.py",
        ]

        # if fragment is defined add the fragment as positional argument
        if self.cms_driver_fragment != law.NO_STR:
            cmd.append(self.cms_driver_fragment)

        # set the name of the configuration file
        cmd += ["--python_filename", python_filename]

        # set the input file for the production
        if self.cms_driver_filein != law.NO_STR:
            cmd += ["--filein", self.cms_driver_filein]

        # set the output file of the production
        cmd += ["--fileout", self.cms_driver_fileout]

        # add customization flags (monotoring and random seed)
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
        if self.cms_driver_mc:
            cmd.append("--mc")

        # run unscheduled production
        if self.cms_driver_run_unscheduled:
            cmd.append("--runUnscheduled")

        # do not execute the simulation while creating the configuration
        # this flag is always set here as the production should not be started in the configuration task
        cmd.append("--no_exec")

        # set number of events
        cmd += ["-n", str(self.cms_driver_number_of_events)]

        return cmd

    def run_command(self, command, output):
        self.logger.debug(f"sandbox environment: {self.env}")

        # generate the configuration file
        ret_code, out, err = law.util.interruptable_popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env, cwd=output.dirname
        )
        if ret_code != 0:
            raise RuntimeError(
                "Command {cmd} failed with exit code {ret_code:d}".format(cmd=command, ret_code=ret_code)
                + "Output: {out:s}".format(out=out)
                + "Error: {err:s}".format(err=err)
            )
        elif not output.exists():
            raise RuntimeError("Output file {output:s} does not exist".format(output=output.path))
