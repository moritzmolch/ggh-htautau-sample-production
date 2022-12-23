import law
import re
import os
import luigi
import subprocess
from abc import ABCMeta, abstractmethod
from typing import Union


law.contrib.load("wlcg")
#law.contrib.load("htcondor")
#law.contrib.load("git")


class Task(law.Task):

    wlcg_path = luigi.Parameter()
    local_data_path = luigi.Parameter()
    cmssw_path = luigi.Parameter()

    _wlcg_file_systems = {}

    output_collection_cls = law.SiblingFileCollection

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

    @classmethod
    def get_wlcg_file_system(cls, wlcg_path):
        if wlcg_path not in cls._wlcg_file_systems:
            cls._wlcg_file_systems[wlcg_path] = law.wlcg.WLCGFileSystem(None, base=wlcg_path)
        return cls._wlcg_file_systems[wlcg_path]

    def remote_path(self, *path):
        return os.path.join(*path)

    def remote_target(self, path):
        return law.wlcg.WLCGFileTarget(self.remote_path(*path), self.get_wlcg_file_system(self.wlcg_path))

    def remote_targets(self, paths):
        targets = []
        for path in paths:
            targets.append(law.WLCGFileTarget(self.remote_path(path), self.get_wlcg_file_system(self.wlcg_path)))
        return targets

    def local_path(self, *path):
        parts = (self.local_data_path, self.__class__.__name__, ) + path
        return os.path.join(*parts)

    def local_target(self, path):
        return law.LocalFileTarget(self.local_path(*path))

    def local_targets(self, paths):
        targets = []
        for path in paths:
            targets.append(law.LocalFileTarget(self.local_path(path)))
        return targets


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
        default=law.NO_STR,
        description="Tag for processing modifications, e.g. for premixing or during the miniAOD step"
    )
    cms_driver_datamix = luigi.Parameter(default=law.NO_STR, description="Type of pileup mixing")
    cms_driver_pileup_input = luigi.Parameter(default=law.NO_STR, description="Input file for pileup mixing")

    cms_driver_geometry = luigi.Parameter(
        default=law.NO_STR,
        description="Selection of the detector geometry description"
    )

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
    def cmssw_fragment_path(self) -> Union[str, None]:
        pass

    @abstractmethod
    def root_input_filename(self) -> Union[str, None]:
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

    def build_command(self, output):
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
        if self.cmssw_fragment_path() is not None:
            cmd.append(self.cmssw_fragment_path())
        if self.root_input_filename() is not None:
            cmd += ["--filein", self.root_input_filename()]

        # set the python filename and the name of the output file
        cmd += ["--python_filename", os.path.basename(output.path)]
        cmd += ["--fileout", self.root_output_filename()]

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
        if self.cms_driver_mc:
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

    def run_command(self, output):
        cmd = self.build_command(output)
        
        # generate the configuration file
        ret_code, out, err = law.util.interruptable_popen(
            cmd,
            cwd=output.parent.path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=os.environ,
        )

        if ret_code != 0:
            raise RuntimeError(
                "Command {cmd} failed with exit code {ret_code:d}".format(cmd=cmd, ret_code=ret_code)
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
        self.run_command(_output)


"""
class HTCondorJobManager(law.contrib.htcondor.HTCondorJobManager):

    status_line_cre = re.compile(r"^(\d+\.\d+)" + 4 * r"\s+[^\s]+" + r"\s+([UIRXSCHE<>])\s+.*$")

    bootstrap_file = law.PathParameter(exists=True)

    @classmethod
    def get_htcondor_version(cls):
        return (8, 6, 5)
"""

"""
class HTCondorWorkflow(law.contrib.htcondor.HTCondorWorkflow):

    prod_data_path = luigi.PathParameter(exists=True)
    bootstrap_file = luigi.PathParameter(exists=True)

    htcondor_universe = luigi.Parameter()
    htcondor_docker_image = luigi.Parameter()

    htcondor_request_cpus = luigi.IntParameter(default=1)
    htcondor_request_gpus = luigi.IntParameter(default=0)
    htcondor_request_memory = law.BytesParameter(default=2000, unit="MB")
    htcondor_request_disk = law.BytesParameter(default=200000, unit="KB")
    htcondor_request_walltime = law.DurationParameter(default=86400, unit="s")

    htcondor_remote_job = luigi.BoolParameter(default=False)
    htcondor_requirements = luigi.Parameter(default=law.NO_STR)

    htcondor_accounting_group = luigi.Parameter(default=law.NO_STR)
    htcondor_x509userproxy = law.wlcg.get_voms_proxy_file()

    def htcondor_workflow_requires(self):
        reqs = HTCondorWorkflow.htcondor_workflow_requires(self)
        reqs["repo"] = BundleProductionRepository.req(self, replicas=3)
        reqs["cmssw"] = BundleCMSSW.req(self, replicas=3)
        return reqs

    def htcondor_create_job_manager(self, **kwargs):
        kwargs = law.util.merge_dicts(self.htcondor_job_manager_defaults, kwargs)
        return HTCondorJobManager(**kwargs)

    def htcondor_output_directory(self):
        return law.LocalDirectoryTarget(os.path.join(self.prod_data_path, "condor_files"))

    def htcondor_create_job_file_factory(self):
        factory = super(HTCondorWorkflow, self).htcondor_create_job_file_factory()
        factory.is_tmp = False
        return factory

    def htcondor_bootstrap_file(self):
        return law.util.rel_path(__file__, self.bootstrap_file)

    def htcondor_job_config(self, config, job_num, branches):

        # custom config content
        config.custom_content = []
        config.custom_content.append(("universe", self.htcondor_universe))
        config.custom_content.append(("docker_image", self.htcondor_docker_image))
        config.custom_content.append(("request_cpus", self.htcondor_request_cpus))
        config.custom_content.append(("request_gpus", self.htcondor_request_gpus))
        config.custom_content.append(("request_memory", self.htcondor_request_memory))
        config.custom_content.append(("request_disk", self.htcondor_request_disk))
        config.custom_content.append(("+request_walltime", self.htcondor_request_walltime))
        config.custom_content.append(("+remote_job", self.htcondor_remote_job))
        config.custom_content.append(("requirements", self.htcondor_requirements))
        config.custom_content.append(("accounting_group", self.htcondor_accounting_group))
        config.custom_content.append(("x509userproxy", self.htcondor_x509userproxy))
        return config

    def htcondor_use_local_scheduler(self):
        return True


class BundleCMSSW(law.cms.BundleCMSSW, law.tasks.TransferLocalFile):

    prod_bundle_path = law.PathParameter(exists=True)
    replicas = luigi.Parameter()

    task_namespace = None

    exclude = "^src/tmp"

    def get_cmssw_path(self):
        return os.environ["PROD_CMSSW_BASE"]

    def single_output(self):
        filename = "{cmssw_version:s}-{checksum:s}.tar.gz".format(
            cmssw_version=os.path.basename(self.get_cmssw_path()),
            checksum=self.checksum,
        )
        return self.LocalFileTarget(os.path.join(self.prod_bundle_path, filename))

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def output(self):
        return law.tasks.TransferLocalFile.output(self)

    @law.decorator.safe_output
    def run(self):
        bundle = law.LocalFileTarget(is_tmp=True)
        self.bundle(bundle)
        self.publish_message(
            "bundled CMSSW archive, size is {:.2f} {}".format(*law.util.human_bytes(bundle.stat().st_size))
        )
        self.transfer(bundle)


class BundleProductionRepository(law.git.BundleGitRepository, law.tasks.TransferLocalFile):

    prod_bundle_path = law.PathParameter(exists=True)

    task_namespace = None

    exclude_files = [".law", ".mypy", "config", "data", "venv", "*.pyc"]

    def get_repo_path(self):
        return os.environ["PROD_BASE_PATH"]

    def single_output(self):
        filename = "{repo_basename:s}-{checksum:s}.tar.gz".format(
            repo_basename=os.path.basename(self.get_repo_path()), checksum=self.checksum
        )
        return self.LocalFileTarget(os.path.join(self.prod_bundle_path, filename))

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def output(self):
        return law.tasks.TransferLocalFile.output(self)

    @law.decorator.safe_output
    def run(self):
        bundle = law.LocalFileTarget(is_tmp=True)
        self.bundle(bundle)
        self.publish_message(
            "bundled repository archive, size is {:.2f} {}".format(*law.util.human_bytes(bundle.stat().st_size))
        )
        self.transfer(bundle)
"""
