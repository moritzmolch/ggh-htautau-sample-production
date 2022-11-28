import abc
import luigi
from luigi.util import inherits
import law
import os


class ProductionConfig(luigi.Config):

    name = luigi.Parameter()
    cmssw_path = luigi.PathParameter()
    production_output_path = luigi.PathParameter()
    production_config_path = luigi.PathParameter()


@inherits(ProductionConfig)
class ProductionTask(law.Task):

    name = luigi.Parameter()

    __metaclass__ = abc.ABCMeta

    def local_path(self, *path):
        parts = (self.production_output_path, ) + (self.name, ) + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))


@inherits(ProductionConfig)
class ConfigTask(law.Task):

    name = luigi.Parameter()

    __metaclass__ = abc.ABCMeta

    def local_path(self, *path):
        parts = (self.production_config_path, ) + (self.name, ) + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))


@inherits(ProductionConfig)
class CMSSWTask(law.Task):

    name = luigi.Parameter()

    __metaclass__ = abc.ABCMeta

    def local_path(self, *path):
        parts = (self.cmssw_path, ) + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))
