import csv
import law
import luigi
import os

from production.framework import Task


class FragmentGeneration(Task, law.LocalWorkflow):

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
