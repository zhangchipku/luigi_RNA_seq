from luigi import ExternalTask, Parameter
import os
from luigi.local_target import LocalTarget
from luigi.contrib.external_program import ExternalProgramTask
from pathlib import Path


class HumanRNA(ExternalTask):
    human_mRNA_path = Parameter()

    def output(self):
        return LocalTarget(str(self.human_mRNA_path))


class Salmon(ExternalTask):
    salmon_path = Parameter()

    def output(self):
        return LocalTarget(str(self.salmon_path))


class SalmonIndex(ExternalProgramTask):
    index_path = Parameter()

    def requires(self):
        return {'human_rna': self.clone(HumanRNA),
                'salmon': self.clone(Salmon)}

    def output(self):
        flag = '__SUCCESS'
        return os.path.join(self.index_path, flag)

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
