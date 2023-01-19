import law
import luigi
import os
import subprocess


law.contrib.load("cms", "git", "htcondor", "tasks", "wlcg")


class BaseTask(law.Task):

    store = os.path.expandvars("${PROD_BASE}/data")

    def __init__(self, *args, **kwargs):
        super(BaseTask, self).__init__(*args, **kwargs)

    def local_path(self, *path):
        parts = (self.store, ) + path
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


class BundleCMSSW(BaseTask, law.contrib.tasks.TransferLocalFile, law.contrib.cms.BundleCMSSW):

    store = os.path.expandvars("${PROD_BUNDLE_BASE}")
    replicas = luigi.IntParameter(default=10, description="number of replica archives to generate; default is 10")

    cmssw_checksumming = False
    exclude = "^src/tmp"
    task_namespace = None

    def get_cmssw_path(self):
        return os.path.expandvars("${PROD_CMSSW_PATH}")

    def single_output(self):
        return self.local_target("{0:s}.tgz".format(os.path.basename(self.get_cmssw_path())))

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def output(self):
        return law.contrib.tasks.TransferLocalFile.output(self)
    
    @law.decorator.safe_output
    def run(self):

        # bundle repository
        bundle = law.LocalFileTarget(is_tmp="tgz", tmp_dir=os.path.expandvars("${PROD_BASE}/tmp"))
        self.bundle(bundle)

        # log the size
        self.publish_message("bundled CMSSW archive, size is {:.2f} {}".format(
            *law.util.human_bytes(bundle.stat().st_size)))

        # transfer replica archives
        self.transfer(bundle)


class BundleProductionRepository(BaseTask, law.contrib.tasks.TransferLocalFile, law.contrib.git.BundleGitRepository):

    store = os.path.expandvars("${PROD_BUNDLE_BASE}")
    replicas = luigi.IntParameter(default=10, description="number of replica archives to generate; default is 10")

    exclude_files = [".law", "bundle", "config", "jobs", "software", "tmp", "luigi.cfg.old", "*~", "*.pyc"]
    task_namespace = None

    def get_repo_path(self):
        return os.path.expandvars("${PROD_BASE}")

    def single_output(self):
        return self.local_target("{0:s}.tgz".format(os.path.basename(self.get_repo_path())))

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def output(self):
        return law.contrib.tasks.TransferLocalFile.output(self)
    
    @law.decorator.safe_output
    def run(self):

        # bundle repository
        bundle = law.LocalFileTarget(is_tmp="tgz", tmp_dir=os.path.expandvars("${PROD_BASE}/tmp"))
        self.bundle(bundle)

        # log the size
        self.publish_message("bundled repository archive, size is {:.2f} {}".format(
            *law.util.human_bytes(bundle.stat().st_size)))

        # transfer replica archives
        self.transfer(bundle)


class BundleConda(BaseTask, law.contrib.tasks.TransferLocalFile):

    store = os.path.expandvars("${PROD_BUNDLE_BASE}")
    replicas = luigi.IntParameter(default=10, description="number of replica archives to generate; default is 10")

    task_namespace = None

    def get_conda_path(self):
        return os.path.expandvars("${PROD_CONDA_BASE}")

    def single_output(self):
        return self.local_target("{0:s}.tgz".format(os.path.basename(self.get_conda_path())))

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def output(self):
        return law.contrib.tasks.TransferLocalFile.output(self)

    @law.decorator.safe_output
    def run(self):

        # bundle repository with conda-pack
        bundle = law.LocalFileTarget(is_tmp="tgz", tmp_dir=os.path.expandvars("${PROD_BASE}/tmp"))
        cmd = ["conda-pack", "--prefix", self.get_conda_path(), "--output", bundle.path]
        ret_code, out, err = law.util.interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ)
        if ret_code != 0:
            raise Exception(
                "conda-pack failed with exit code {0:d}".format(ret_code)
                + "\nOutput:\n{0:s}".format(out)
                + "\nError:\n{0:s}".format(err)
            )

        # log the size
        self.publish_message("bundled conda archive, size is {:.2f} {}".format(
            *law.util.human_bytes(bundle.stat().st_size)))

        # transfer replica archives
        self.transfer(bundle)


class HTCondorWorkflow(law.contrib.htcondor.HTCondorWorkflow):

    htcondor_universe = luigi.Parameter(default="docker", significant=False)
    htcondor_docker_image = luigi.Parameter(default="mschnepf/slc7-condocker", significant=False)
    htcondor_requirements = luigi.Parameter(default=law.NO_STR, significant=False)

    htcondor_request_cpus = luigi.IntParameter(default=1, significant=False)
    htcondor_request_gpus = luigi.IntParameter(default=0, significant=False)
    htcondor_request_memory = luigi.Parameter(default=2000, significant=False, description="(in MB)")
    htcondor_request_walltime = luigi.IntParameter(default=3600, significant=False, description="(in s)")
    htcondor_request_disk = luigi.Parameter(default=200000, significant=False, description="(in KB)")
    htcondor_remote_job = luigi.BoolParameter(default=False, significant=False)

    htcondor_accounting_group = luigi.Parameter(default="cms.higgs", significant=False)
    htcondor_run_as_owner = luigi.BoolParameter(default=True, significant=False)
    htcondor_x509userproxy = law.contrib.wlcg.get_voms_proxy_file()

    exclude_params_branch = {
        "htcondor_universe",
        "htcondor_docker_image",
        "htcondor_requirements",
        "htcondor_request_cpus",
        "htcondor_request_gpus",
        "htcondor_request_memory",
        "htcondor_request_walltime",
        "htcondor_request_disk",
        "htcondor_remote_job",
        "htcondor_accounting_group",
        "htcondor_run_as_owner",
        "htcondor_x509userproxy",
    }

    def htcondor_workflow_requires(self):
        reqs = law.contrib.htcondor.HTCondorWorkflow.htcondor_workflow_requires(self)
        reqs["repo"] = BundleProductionRepository.req(self, replicas=3)
        reqs["cmssw"] = BundleCMSSW.req(self, replicas=3)
        reqs["conda"] = BundleConda.req(self, replicas=3)
        return reqs

    def htcondor_output_directory(self):
        return law.LocalDirectoryTarget(os.path.expandvars("${PROD_JOBS_BASE}"))

    def htcondor_create_job_file_factory(self, **kwargs):
        kwargs = law.util.merge_dicts(self.htcondor_job_file_factory_defaults, {"universe": self.htcondor_universe}, kwargs)
        factory = super(HTCondorWorkflow, self).htcondor_create_job_file_factory(**kwargs)
        factory.is_tmp = False
        return factory

    def htcondor_bootstrap_file(self):
        return os.path.expandvars("${PROD_BASE}/production/tasks/remote_bootstrap.sh") 

    def htcondor_job_config(self, config, job_num, branches):

        # append law's wlcg tool script to the collection of input files
        # needed for setting up the software environment
        config.input_files["wlcg_tools"] = law.util.law_src_path("contrib/wlcg/scripts/law_wlcg_tools.sh")

        ## contents of the HTCondor submission file

        # job environment: docker image and requirements
        #config.custom_content.append(("universe", self.htcondor_universe))
        config.custom_content.append(("docker_image", self.htcondor_docker_image))
        if self.htcondor_requirements != law.NO_STR:
            config.custom_content.append(("requirements", self.htcondor_requirements))

        # log files; enforce that STDOUT and STDERR are not streamed to the submission machine
        # while the job is running
        # (log files might automatically be set by HTCondorJobFileFactory)
        log_dir = os.path.join(self.htcondor_output_directory().path, "logs")
        config.custom_content.append(("log", os.path.join(log_dir, "log_{0:d}_{1:d}To{2:d}.txt".format(job_num, branches[0], branches[-1]))))
        config.custom_content.append(("output", os.path.join(log_dir, "output_{0:d}_{1:d}To{2:d}.txt".format(job_num, branches[0], branches[-1]))))
        config.custom_content.append(("error", os.path.join(log_dir, "error_{0:d}_{1:d}To{2:d}.txt".format(job_num, branches[0], branches[-1]))))
        config.custom_content.append(("stream_output", False))
        config.custom_content.append(("stream_error", False))

        # resources and runtime
        config.custom_content.append(("request_cpus", self.htcondor_request_cpus))
        if self.htcondor_request_gpus > 0:
            config.custom_content.append(("request_gpus", self.htcondor_request_gpus))
        config.custom_content.append(("request_memory", self.htcondor_request_memory))
        config.custom_content.append(("request_disk", self.htcondor_request_disk))
        config.custom_content.append(("+request_walltime", self.htcondor_request_walltime))
        if self.htcondor_remote_job:
            config.custom_content.append(("+remote_job", self.htcondor_remote_job)) 

        # user information: accounting group and VOMS proxy
        config.custom_content.append(("accounting_group", self.htcondor_accounting_group))
        if self.htcondor_run_as_owner:
            config.custom_content.append(("run_as_owner", self.htcondor_run_as_owner))
        config.custom_content.append(("x509userproxy", self.htcondor_x509userproxy))

        # get the URIs of the bundles
        reqs = self.htcondor_workflow_requires()
        def get_bundle_info(task):
            uris = task.output().dir.uri(cmd="filecopy", return_all=True)
            pattern = os.path.basename(task.get_file_pattern())
            return ",".join(uris), pattern

        # render variables in bootstrap script
        config.render_variables["user"] = os.environ["USER"]

        config.render_variables["prod_conda_base"] = os.path.relpath(os.environ["PROD_CONDA_BASE"], os.environ["PROD_BASE"])
        uris, pattern = get_bundle_info(reqs["repo"])
        config.render_variables["prod_repo_uris"] = uris
        config.render_variables["prod_repo_pattern"] = pattern

        uris, pattern = get_bundle_info(reqs["conda"])
        config.render_variables["prod_conda_uris"] = uris
        config.render_variables["prod_conda_pattern"] = pattern

        uris, pattern = get_bundle_info(reqs["cmssw"])
        config.render_variables["prod_cmssw_uris"] = uris
        config.render_variables["prod_cmssw_pattern"] = pattern

        return config

    def htcondor_use_local_scheduler(self):
        # always use a local scheduler in remote jobs
        return True
