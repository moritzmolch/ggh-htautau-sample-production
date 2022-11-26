import abc
import luigi
import law
import os


class ProductionConfig(luigi.Config):

    cmssw_path = luigi.PathParameter()
    production_output_path = luigi.PathParameter()
    production_config_path = luigi.PathParameter()


@luigi.util.inherits(ProductionConfig)
class BaseTask(law.Task):

    __metaclass__ = abc.ABCMeta

    name = luigi.Parameter()


class ProductionTask(BaseTask):

    __metaclass__ = abc.ABCMeta

    def local_path(self, *path):
        parts = (self.production_output_path, ) + (self.name, ) + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))


class ConfigTask(luigi.Task):

    name = luigi.Parameter()

    __metaclass__ = abc.ABCMeta

    def local_path(self, *path):
        parts = (self.production_config_path, ) + (self.name, ) + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))
