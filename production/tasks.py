import law
import os
import csv
import luigi

from production.framework import Task, CMSDriverTask


class ConfigAODSIM(Task, CMSDriverTask, law.LocalWorkflow):

    def create_branch_map(self):
        branch_map = CreateFragment.req(self).get_branch_map()
        for branch, branch_data in branch_map.items():
            higgs_mass = branch_data["higgs_mass"]
            number_of_events = branch_data["number_of_events"]
            job_number_of_events = 0
            if higgs_mass < 250:
                job_number_of_events = 2000
            else:
                job_number_of_events = 4000
            number_of_jobs = 0
            while number_of_jobs * job_number_of_events < number_of_events:
                number_of_jobs += 1
            jobs = {k: v for k, v in enumerate([job_number_of_events for i in range(number_of_jobs)])}
            branch_map[branch]["jobs"] = jobs
        return branch_map

    def workflow_requires(self):
        reqs = super(ConfigAODSIM, self).workflow_requires()
        reqs["fragment"] = CreateFragment.req(self, branches=self.branches)
        return reqs

    def requires(self):
        reqs = {"fragment": CreateFragment.req(self, branch=self.branch)}
        return reqs

    def output(self):
        return self.local_targets([
            "{basename:s}_aodsim_{index:d}_cfg.py".format(basename=self.branch_data["name"], index=i)
            for i in sorted(list(self.branch_data["jobs"]))
        ])

    def cmssw_fragment_path(self):
        fragment_path = self.input()["fragment"].path
        return os.path.relpath(fragment_path, start=os.path.join(self.cmssw_path, "src"))

    def root_input_filename(self):
        return None

    def root_output_filename(self):
        return "file:{{ root_output_filename }}"

    def run(self):

        # get the first output and create parent directory if it does not exist
        _output = self.output()[0]
        if not _output.parent.exists():
            _output.parent.touch()

        # create the template config
        tmp_config = law.LocalFileTarget(_output.path + ".tpl")
        self.run_command(tmp_config)

        # use template config to create config files for jobs and replace placeholders for filenames and number of
        # events
        config = tmp_config.load(formatter="text")
        tmp_config_filename = os.path.basename(tmp_config.path)
        for i in sorted(list(self.branch_data["jobs"])):
            job_config = config
            _output = self.output()[i]
            job_number_of_events = self.branch_data["jobs"][i]

            # replace the name of the python config file
            config_filename = os.path.basename(_output.path)
            job_config = job_config.replace(tmp_config_filename, config_filename)

            # replace the name of the ROOT output file
            root_output_filename = "{basename:s}_aodsim_{index:d}.root".format(
                basename=self.branch_data["name"], index=i
            )
            job_config = job_config.replace("{{ root_output_filename }}", root_output_filename)

            # set the number of events
            job_config = job_config.replace("-n -1", "-n {n:d}".format(n=job_number_of_events))
            job_config = job_config.replace(
                "input = cms.untracked.int32(-1)",
                "input = cms.untracked.int32({n:d})".format(n=job_number_of_events),
            )
            job_config = job_config.replace("nevts:-1", "nevts:{n:d}".format(n=job_number_of_events))

            # modify luminosity block index
            job_config += "\n\nprocess.source.firstLuminosityBlock = cms.untracked.uint32(1 + {index:d})".format(
                index=i
            )

            # write job config to output
            _output.dump(job_config, formatter="text")


class ConfigMINIAODSIM(Task, CMSDriverTask, law.LocalWorkflow):

    def create_branch_map(self):
        return ConfigAODSIM.req(self).get_branch_map()

    def requires(self):
        reqs = {"config_aodsim": ConfigAODSIM.req(self, branch=self.branch)}
        return reqs

    def output(self):
        return self.local_targets([
            "{basename:s}_miniaod_{index:d}_cfg.py".format(basename=self.branch_data["name"], index=i)
            for i in sorted(list(self.branch_data["jobs"]))
        ])

    def cmssw_fragment_path(self):
        return None

    def root_input_filename(self):
        return "file:{{ root_input_filename }}"

    def root_output_filename(self):
        return "file:{{ root_output_filename }}"

    def run(self):

        # get the first output and create parent directory if it does not exist
        _output = self.output()[0]
        if not _output.parent.exists():
            _output.parent.touch()

        # create the template config
        tmp_config = law.LocalFileTarget(path=_output.path + ".tpl")
        self.run_command(tmp_config)

        # use template config to create config files for jobs and replace placeholders for filenames and number of
        # events
        config = tmp_config.load(formatter="text")
        tmp_config_filename = os.path.basename(tmp_config.path)
        for i in sorted(list(self.branch_data["jobs"])):
            job_config = config
            _output = self.output()[i]

            # replace the name of the python config file
            config_filename = os.path.basename(_output.path)
            job_config = job_config.replace(tmp_config_filename, config_filename)

            # replace the name of the ROOT input file
            root_input_filename = "{basename:s}_aodsim_{index:d}.root".format(
                basename=self.branch_data["name"], index=i
            )
            job_config = job_config.replace("{{ root_input_filename }}", root_input_filename)

            # replace the name of the ROOT output file
            root_output_filename = "{basename:s}_miniaod_{index:d}.root".format(
                basename=self.branch_data["name"], index=i
            )
            job_config = job_config.replace("{{ root_output_filename }}", root_output_filename)

            # write job config to output
            _output.dump(job_config, formatter="text")


class ConfigNANOAODSIM(Task, CMSDriverTask, law.LocalWorkflow):

    def create_branch_map(self):
        return ConfigMINIAODSIM.req(self).get_branch_map()

    def requires(self):
        reqs = {"config_miniaodsim": ConfigMINIAODSIM.req(self, branch=self.branch)}
        return reqs

    def output(self):
        return self.local_targets([
            "{basename:s}_nanoaod_{index:d}_cfg.py".format(basename=self.branch_data["name"], index=i)
            for i in sorted(list(self.branch_data["jobs"]))
        ])

    def cmssw_fragment_path(self):
        return None

    def root_input_filename(self):
        return "file:{{ root_input_filename }}"

    def root_output_filename(self):
        return "file:{{ root_output_filename }}"

    def run(self):

        # get the first output and create parent directory if it does not exist
        _output = self.output()[0]
        if not _output.parent.exists():
            _output.parent.touch()

        # create the template config
        tmp_config = law.LocalFileTarget(path=_output.path + ".tpl")
        self.run_command(tmp_config)

        # use template config to create config files for jobs and replace placeholders for filenames and number of
        # events
        config = tmp_config.load(formatter="text")
        tmp_config_filename = os.path.basename(tmp_config.path)
        for i in sorted(list(self.branch_data["jobs"])):
            job_config = config
            _output = self.output()[i]

            # replace the name of the python config file
            config_filename = os.path.basename(_output.path)
            job_config = job_config.replace(tmp_config_filename, config_filename)

            # replace the name of the ROOT input file
            root_input_filename = "{basename:s}_miniaod_{index:d}.root".format(
                basename=self.branch_data["name"], index=i
            )
            job_config = job_config.replace("{{ root_input_filename }}", root_input_filename)

            # replace the name of the ROOT output file
            root_output_filename = "{basename:s}_nanoaod_{index:d}.root".format(
                basename=self.branch_data["name"], index=i
            )
            job_config = job_config.replace("{{ root_output_filename }}", root_output_filename)

            # write job config to output
            _output.dump(job_config, formatter="text")
