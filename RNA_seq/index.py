from luigi import ExternalTask, Parameter
import os
from luigi.local_target import LocalTarget
from luigi.contrib.external_program import ExternalProgramTask
from pathlib import Path
from .luigi.task import Requires, Requirement


class HumanRNA(ExternalTask):
    human_mRNA_path = Parameter()

    def output(self):
        return LocalTarget(str(self.human_mRNA_path))


class Salmon(ExternalTask):
    salmon_path = Parameter()

    def output(self):
        return LocalTarget(str(self.salmon_path))


class SalmonIndex(ExternalProgramTask):
    human_mRNA_path = Parameter()
    salmon_path = Parameter()
    index_path = Parameter()

    requires = Requires()
    huamn_rna = Requirement(HumanRNA)
    salmon = Requirement(Salmon)

    def output(self):
        flag = '__SUCCESS'
        return os.path.join(str(self.index_path), flag)

    def program_args(self):
        return [
            self.input()['salmon'].path,
            "index",
            "-t",
            self.input()['human_rna'].path,
            "-i",
            self.index_path
        ]

    def run(self):
        super().run()
        # mark complete
        Path(self.output().path).touch()
