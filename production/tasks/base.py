import law
import luigi
import os

from production.config import ggh_htautau_production


law.contrib.load("cms", "git", "htcondor", "tasks", "wlcg")


class BaseTask(law.Task):
    default_store = os.path.expandvars("${PROD_DATA_STORE}")

    output_collection_cls = law.SiblingFileCollection

    def __init__(self, *args, **kwargs):
        super(BaseTask, self).__init__(*args, **kwargs)

    def local_path(self, *path, **kwargs):
        store = kwargs.pop("store", self.default_store)
        parts = (store,) + path
        return os.path.join(*parts)

    def local_target(self, *path, **kwargs):
        target_class = law.LocalDirectoryTarget if kwargs.pop("dir", False) else law.LocalFileTarget
        return target_class(self.local_path(*path, **kwargs))

    def remote_path(self, *path):
        return os.path.join(*path)

    def remote_target(self, *path, **kwargs):
        target_class = (
            law.wlcg.WLCGDirectoryTarget if kwargs.pop("dir", False) else law.wlcg.WLCGFileTarget
        )
        return target_class(self.remote_path(*path))


class AnalysisTask(BaseTask):
    config = luigi.Parameter(default="mc_ul18_fastsim_aodsim")

    def __init__(self, *args, **kwargs):
        super(AnalysisTask, self).__init__(*args, **kwargs)
        self.analysis_inst = ggh_htautau_production
        self.config_inst = self.analysis_inst.get_config(self.config)


class ProcessTask(AnalysisTask):
    process = luigi.Parameter(default="ggh_htautau")

    def __init__(self, *args, **kwargs):
        super(ProcessTask, self).__init__(*args, **kwargs)
        self.process_inst = self.config_inst.get_process(self.process)

    def create_branch_map(self):
        # a generic branch map that associates each subprocess of the defined process with a branch
        # if the process has no subprocesses a branch map with a single entry that contains the
        # process is returned
        processes = self.process_inst.get_leaf_processes()
        branch_map = {}
        for i, p in enumerate(processes):
            branch_map[i] = {"process_inst": p}
        if len(branch_map) == 0:
            branch_map[0] = {"process_inst": self.process_inst}
        return branch_map


class DatasetTask(AnalysisTask):
    dataset = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(DatasetTask, self).__init__(*args, **kwargs)
        self.dataset_inst = self.config_inst.get_dataset(self.dataset)


class BundleCMSSW(BaseTask, law.tasks.TransferLocalFile, law.contrib.cms.BundleCMSSW):
    default_store = os.path.expandvars("${PROD_BUNDLE_STORE}")
    replicas = luigi.IntParameter(
        default=10, description="number of replica archives to generate; default is 10"
    )

    cmssw_checksumming = False
    exclude = "^src/tmp"
    task_namespace = None

    def get_cmssw_path(self):
        return os.path.expandvars("${PROD_CMSSW_BASE}")

    def single_output(self):
        return self.local_target("{0:s}.tgz".format(os.path.basename(self.get_cmssw_path())))

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def output(self):
        return law.tasks.TransferLocalFile.output(self)

    @law.decorator.safe_output
    def run(self):
        # bundle repository
        bundle = law.LocalFileTarget(is_tmp="tgz", tmp_dir=os.path.expandvars("${PROD_TMPDIR}"))
        self.bundle(bundle)

        # log the size
        self.publish_message(
            "bundled CMSSW archive, size is {:.2f} {}".format(
                *law.util.human_bytes(bundle.stat().st_size)
            )
        )

        # transfer replica archives
        self.transfer(bundle)


class BundleProductionRepository(
    BaseTask, law.tasks.TransferLocalFile, law.git.BundleGitRepository
):
    default_store = os.path.expandvars("${PROD_BUNDLE_STORE}")
    replicas = luigi.IntParameter(
        default=10, description="number of replica archives to generate; default is 10"
    )

    task_namespace = None
    exclude_files = [
        ".law",
        "_config",
        "bundle",
        "data",
        "jobs",
        "software",
        "tmp",
        "venv",
    ]

    def get_repo_path(self):
        return os.path.expandvars("${PROD_BASE}")

    def single_output(self):
        return self.local_target("{0:s}.tgz".format(os.path.basename(self.get_repo_path())))

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def output(self):
        return law.tasks.TransferLocalFile.output(self)

    @law.decorator.safe_output
    def run(self):
        # bundle repository
        bundle = law.LocalFileTarget(is_tmp="tgz", tmp_dir=os.path.expandvars("${PROD_TMPDIR}"))
        self.bundle(bundle)

        # log the size
        self.publish_message(
            "bundled repository archive, size is {:.2f} {}".format(
                *law.util.human_bytes(bundle.stat().st_size)
            )
        )

        # transfer replica archives
        self.transfer(bundle)


class HTCondorWorkflow(law.htcondor.HTCondorWorkflow):
    htcondor_universe = luigi.Parameter(default="docker", significant=False)
    htcondor_docker_image = luigi.Parameter(default="mschnepf/slc7-condocker", significant=False)
    htcondor_requirements = luigi.Parameter(
        default="(Target.ProvidesIO =?= True) && (Target.ProvidesCPU =?= True) &&"
        + " (Target.ProvidesEKPResources =?= True)",
        significant=False,
    )

    htcondor_request_cpus = luigi.IntParameter(default=1, significant=False)
    htcondor_request_gpus = luigi.IntParameter(default=0, significant=False)
    htcondor_request_memory = luigi.Parameter(
        default=5000, significant=False, description="(in MB)"
    )
    htcondor_request_walltime = luigi.IntParameter(
        default=86400, significant=False, description="(in s)"
    )
    htcondor_request_disk = luigi.Parameter(
        default=10000000, significant=False, description="(in KB)"
    )
    htcondor_remote_job = luigi.BoolParameter(default=False, significant=False)

    htcondor_accounting_group = luigi.Parameter(default="cms.higgs", significant=False)
    htcondor_run_as_owner = luigi.BoolParameter(default=True, significant=False)
    htcondor_x509userproxy = law.wlcg.get_voms_proxy_file()

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
        reqs = law.htcondor.HTCondorWorkflow.htcondor_workflow_requires(self)
        reqs["repo"] = BundleProductionRepository.req(self, replicas=3)
        reqs["cmssw"] = BundleCMSSW.req(self, replicas=3)
        return reqs

    def htcondor_output_directory(self):
        return law.LocalDirectoryTarget(os.path.expandvars("${PROD_JOBS_STORE}"))

    def htcondor_create_job_file_factory(self, **kwargs):
        kwargs = law.util.merge_dicts(
            self.htcondor_job_file_factory_defaults, {"universe": self.htcondor_universe}, kwargs
        )
        factory = super(HTCondorWorkflow, self).htcondor_create_job_file_factory(**kwargs)
        factory.is_tmp = False
        return factory

    def htcondor_bootstrap_file(self):
        return os.path.expandvars("${PROD_BASE}/production/tasks/remote_bootstrap.sh")

    def htcondor_job_config(self, config, job_num, branches):
        # create directory for log files if it doesn't exist
        log_dir = os.path.join(self.htcondor_output_directory().path, "logs")
        log_target = law.LocalDirectoryTarget(log_dir)
        if not log_target.exists():
            log_target.touch()

        # append law's wlcg tool script to the collection of input files
        # needed for setting up the software environment
        config.input_files["wlcg_tools"] = law.util.law_src_path(
            "contrib/wlcg/scripts/law_wlcg_tools.sh"
        )

        # contents of the HTCondor submission file

        # job environment: docker image and requirements
        # config.custom_content.append(("universe", self.htcondor_universe))
        config.custom_content.append(("docker_image", self.htcondor_docker_image))
        if self.htcondor_requirements != law.NO_STR:
            config.custom_content.append(("requirements", self.htcondor_requirements))

        # set paths for log files
        # enforce that STDOUT and STDERR are not streamed to the submission machine
        # while the job is running
        config.custom_content.append(
            (
                "log",
                os.path.join(
                    log_dir, "log_{0:d}_{1:d}To{2:d}.txt".format(job_num, branches[0], branches[-1])
                ),
            )
        )
        config.custom_content.append(
            (
                "output",
                os.path.join(
                    log_dir,
                    "output_{0:d}_{1:d}To{2:d}.txt".format(job_num, branches[0], branches[-1]),
                ),
            )
        )
        config.custom_content.append(
            (
                "error",
                os.path.join(
                    log_dir,
                    "error_{0:d}_{1:d}To{2:d}.txt".format(job_num, branches[0], branches[-1]),
                ),
            )
        )
        config.custom_content.append(("stream_output", False))
        config.custom_content.append(("stream_error", False))

        # resources and runtime
        config.custom_content.append(("request_cpus", self.htcondor_request_cpus))
        if self.htcondor_request_gpus > 0:
            config.custom_content.append(("request_gpus", self.htcondor_request_gpus))
        config.custom_content.append(("request_memory", self.htcondor_request_memory))
        config.custom_content.append(("request_disk", self.htcondor_request_disk))
        config.custom_content.append(("+RequestWalltime", self.htcondor_request_walltime))
        if self.htcondor_remote_job:
            config.custom_content.append(("+RemoteJob", self.htcondor_remote_job))

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

        uris, pattern = get_bundle_info(reqs["repo"])
        config.render_variables["prod_repo_uris"] = uris
        config.render_variables["prod_repo_pattern"] = pattern

        uris, pattern = get_bundle_info(reqs["cmssw"])
        config.render_variables["prod_cmssw_uris"] = uris
        config.render_variables["prod_cmssw_pattern"] = pattern

        return config

    def htcondor_use_local_scheduler(self):
        # always use a local scheduler in remote jobs
        return True
