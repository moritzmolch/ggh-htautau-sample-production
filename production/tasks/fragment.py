from jinja2 import Template
import law
import luigi
import os

from production.tasks.base import BaseTask


class FragmentGeneration(BaseTask):

    higgs_mass = luigi.IntParameter(description="mass of the Higgs boson that is put into the event generator configuration")

    cmssw_path = os.path.expandvars("${PROD_CMSSW_BASE}")
    fragment_template = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "templates",
            "GluGluHToTauTau_MHXXX_pythia8_TuneCP5_cff.py.j2",
        )
    )

    exclude_params_req_get = {"workflow"}
    prefer_params_cli = {"workflow"}

    def get_fragment_filename(self, higgs_mass):
        return "GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_cff.py".format(higgs_mass=higgs_mass)

    def output(self):
        _higgs_mass = self.branch_data["higgs_mass"]
        return self.local_target(
            "src",
            "Configuration",
            "GenProduction",
            "python",
            self.get_fragment_filename(_higgs_mass),
            store=self.cmssw_path,
        )

    def run(self):
        # get the output and the process instance of this branch
        _output = self.output()
        _higgs_mass = self.higgs_mass

        # load template and replace placeholders
        _fragment_template = law.LocalFileTarget(self.fragment_template)
        with _fragment_template.open(mode="r") as f:
            template = Template(f.read())
        content = template.render(higgs_mass=_higgs_mass)

        # write the fragment content to the output target
        _output.dump(content, formatter="text")
        self.publish_message(
            "successfully created fragment for dataset with generator Higgs mass of {higgs_mass:s} GeV".format(
                higgs_mass=str(_higgs_mass)
            )
        )
