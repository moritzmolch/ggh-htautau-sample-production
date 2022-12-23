import law
import os
import csv
import luigi

from production.utils import HTTDataset
from production.framework import Task, CMSDriverTask


class CreateFragment(Task, law.LocalWorkflow):

    cmssw_path = luigi.PathParameter(exists=True)
    fragment_template_path = luigi.PathParameter(exists=True)
    mass_grid_definition_path = luigi.PathParameter(exists=True)

    def create_branch_map(self):
        branch_list = []
        with open(self.mass_grid_definition_path, "r") as f:
            csv_reader = csv.reader(f, delimiter=",")
            for row in csv_reader:
                if row[0].strip().startswith("#"):
                    continue
                higgs_mass, higgs_width, number_of_events = float(row[0]), float(row[1]), int(row[2])
                higgs_mass_str = ""
                if float(higgs_mass) == int(higgs_mass):
                    higgs_mass_str = str(int(higgs_mass))
                else:
                    higgs_mass_str = str(higgs_mass).replace(".", "p")
                branch_list.append({
                    "higgs_mass": higgs_mass,
                    "higgs_width": higgs_width,
                    "number_of_events": number_of_events,
                    "name": "GluGluHToTauTau_MH{higgs_mass:s}_pythia8_TuneCP5".format(higgs_mass=higgs_mass_str),
                })
        return {k: v for k, v in enumerate(branch_list)}

    def output(self):
        filename = self.branch_data["name"] + "_cff.py"
        return law.LocalFileTarget(
            os.path.join(self.cmssw_path, "src", "Configuration", "GenProduction", "python", filename)
        )

    def run(self):

        # get output and create parent directory if it does not exist
        _output = self.output()
        if not _output.parent.exists():
            _output.parent.touch()

        # replace Higgs mass and Higgs width in the fragment template
        with open(self.fragment_template_path, "r") as f:
            fragment_content = f.read()
        if self.branch_data["higgs_width"] != -1:
            process_parameters_block = (
                "processParameters = cms.vstring(\n"
                + "            'HiggsSM:gg2H = on',\n"
                + "            '25:onMode = off',\n"
                + "            '25:onIfMatch = 15 -15',\n"
                + "            '25:m0 = {0:f}',\n".format(self.branch_data["higgs_mass"])
                + "            '25:mWidth = {0:f}',\n".format(self.branch_data["higgs_width"])
                + "            '25:doForceWidth = on',\n"
                + "        ),"
            )
        else:
            process_parameters_block = (
                "processParameters = cms.vstring(\n"
                + "            'HiggsSM:gg2H = on',\n"
                + "            '25:onMode = off',\n"
                + "            '25:onIfMatch = 15 -15',\n"
                + "            '25:m0 = {0:f}',\n".format(self.branch_data["higgs_mass"])
                + "        ),"
            )
        fragment_content = fragment_content.replace("{{ process_parameters_block }}", process_parameters_block)

        # write fragment content to output
        _output.dump(fragment_content, formatter="text")


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
        reqs = super(ConfigAODSIM, self).requires()
        reqs["fragment"] = CreateFragment.req(self, branch=self.branch)
        return reqs

    def output(self):
        return self.local_targets([
            "{basename:s}_aodsim_{index:d}_cfg.py".format(basename=self.branch_data["basename"], index=i)
            for i in list(self.branch_data["jobs"]).sort()
        ])

    def cmssw_fragment_path(self):
        fragment_path = self.input()["fragment"].path
        return os.path.relpath(fragment_path, start=self.cmssw_path)

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
        tmp_config = law.LocalFileTarget(is_tmp=True)
        cmd = self.build_command()
        self.run_command(cmd, tmp_config)

        # use template config to create config files for jobs and replace placeholders for filenames and number of
        # events
        config = tmp_config.load(formatter="text")
        tmp_config_filename = os.path.basename(tmp_config.path)
        for i in list(self.branch_data["jobs"]).sort():
            job_config = config
            _output = self.output()[i]
            job_number_of_events = self.branch_data["jobs"][i]

            # replace the name of the python config file
            config_filename = os.path.basename(_output.path)
            job_config = job_config.replace(tmp_config_filename, config_filename)

            # replace the name of the ROOT output file
            root_output_filename = "{basename:s}_aodsim_{index:d}.root".format(
                basename=self.branch_data["basename"], index=i
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

    def workflow_requires(self):
        reqs = super(ConfigAODSIM, self).workflow_requires()
        reqs["config_aodsim"] = ConfigAODSIM.req(self, branches=self.branches)
        return reqs

    def requires(self):
        reqs = super(ConfigAODSIM, self).requires()
        reqs["config_aodsim"] = ConfigAODSIM.req(self, branch=self.branch)
        return reqs

    def output(self):
        return self.local_targets([
            "{basename:s}_miniaod_{index:d}_cfg.py".format(basename=self.branch_data["basename"], index=i)
            for i in list(self.branch_data["jobs"]).sort()
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
        tmp_config = law.LocalFileTarget(is_tmp=True)
        cmd = self.build_command()
        self.run_command(cmd, tmp_config)

        # use template config to create config files for jobs and replace placeholders for filenames and number of
        # events
        config = tmp_config.load(formatter="text")
        tmp_config_filename = os.path.basename(tmp_config.path)
        for i in list(self.branch_data["jobs"]).sort():
            job_config = config
            _output = self.output()[i]

            # replace the name of the python config file
            config_filename = os.path.basename(_output.path)
            job_config = job_config.replace(tmp_config_filename, config_filename)

            # replace the name of the ROOT input file
            root_input_filename = "{basename:s}_aodsim_{index:d}.root".format(
                basename=self.branch_data["basename"], index=i
            )
            job_config = job_config.replace("{{ root_input_filename }}", root_input_filename)

            # replace the name of the ROOT output file
            root_output_filename = "{basename:s}_miniaod_{index:d}.root".format(
                basename=self.branch_data["basename"], index=i
            )
            job_config = job_config.replace("{{ root_output_filename }}", root_output_filename)

            # write job config to output
            _output.dump(job_config, formatter="text")


class ConfigNANOAODSIM(Task, CMSDriverTask, law.LocalWorkflow):

    def create_branch_map(self):
        return ConfigMINIAODSIM.req(self).get_branch_map()

    def workflow_requires(self):
        reqs = super(ConfigMINIAODSIM, self).workflow_requires()
        reqs["config_miniaodsim"] = ConfigMINIAODSIM.req(self, branches=self.branches)
        return reqs

    def requires(self):
        reqs = super(ConfigMINIAODSIM, self).requires()
        reqs["config_miniaodsim"] = ConfigMINIAODSIM.req(self, branch=self.branch)
        return reqs

    def output(self):
        return self.local_targets([
            "{basename:s}_nanoaod_{index:d}_cfg.py".format(basename=self.branch_data["basename"], index=i)
            for i in list(self.branch_data["jobs"]).sort()
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
        tmp_config = law.LocalFileTarget(is_tmp=True)
        cmd = self.build_command()
        self.run_command(cmd, tmp_config)

        # use template config to create config files for jobs and replace placeholders for filenames and number of
        # events
        config = tmp_config.load(formatter="text")
        tmp_config_filename = os.path.basename(tmp_config.path)
        for i in list(self.branch_data["jobs"]).sort():
            job_config = config
            _output = self.output()[i]

            # replace the name of the python config file
            config_filename = os.path.basename(_output.path)
            job_config = job_config.replace(tmp_config_filename, config_filename)

            # replace the name of the ROOT input file
            root_input_filename = "{basename:s}_miniaod_{index:d}.root".format(
                basename=self.branch_data["basename"], index=i
            )
            job_config = job_config.replace("{{ root_input_filename }}", root_input_filename)

            # replace the name of the ROOT output file
            root_output_filename = "{basename:s}_nanoaod_{index:d}.root".format(
                basename=self.branch_data["basename"], index=i
            )
            job_config = job_config.replace("{{ root_output_filename }}", root_output_filename)

            # write job config to output
            _output.dump(job_config, formatter="text")
