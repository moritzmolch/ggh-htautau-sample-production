from jinja2 import Template
import law
import os

from production.tasks.base import ProcessTask


class FragmentGeneration(ProcessTask, law.LocalWorkflow):
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

    def create_branch_map(self):
        branch_map = super(FragmentGeneration, self).create_branch_map()
        return branch_map

    def output(self):
        _process_inst = self.branch_data["process_inst"]
        return self.local_target(
            "src",
            "Configuration",
            "GenProduction",
            "python",
            "{filename_prefix:s}_cff.py".format(
                filename_prefix=_process_inst.get_aux("filename_prefix")
            ),
            store=self.cmssw_path,
        )

    def run(self):
        # get the output and the process instance of this branch
        _output = self.output()
        _process_inst = self.branch_data["process_inst"]

        # load template and replace placeholders
        _fragment_template = law.LocalFileTarget(self.fragment_template)
        with _fragment_template.open(mode="r") as f:
            template = Template(f.read())
        content = template.render(higgs_mass=float(_process_inst.get_aux("higgs_mass")))

        # write the fragment content to the output target
        _output.dump(content, formatter="text")
        self.publish_message(
            "successfully created fragment for process {process:s}".format(
                process=_process_inst.name
            )
        )
