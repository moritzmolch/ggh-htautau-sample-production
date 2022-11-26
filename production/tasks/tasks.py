import csv
import law
import luigi
from production.framework.tasks import ProductionConfig, ConfigTask, ProductionTask


@luigi.util.inherits(ProductionConfig)
class ProvideFragmentTemplate(ConfigTask, law.ExternalTask):

    name = "fragment_template"
    fragment_template = luigi.PathParameter(exists=True)

    def output(self):
        return self.local_target("GluGluHToTauTau_MHXXX_pythia8_TuneCP5_cff.py.template")


@luigi.util.inherits(ProductionConfig)
class ProvideMassGridDefinition(ConfigTask, law.ExternalTask):

    name = "mass_grid_definition"
    mass_grid_file = luigi.PathParameter(exists=True)

    def output(self):
        return self.local_target("mass_grid_definition.txt")

