from luigi import ExternalTask, Parameter, IntParameter
import os
from luigi.local_target import LocalTarget
from luigi.contrib.external_program import ExternalProgramTask
from pathlib import Path
from .luigi.task import Requires, Requirement


class HumanRNA(ExternalTask):
    """
    Make sure human transcriptome file exists
    """

    human_mRNA_path = Parameter()

    def output(self):
        return LocalTarget(str(self.human_mRNA_path))


class Salmon(ExternalTask):
    """
    Make sure Salmon executable exists
    """

    salmon_path = Parameter()

    def output(self):
        return LocalTarget(str(self.salmon_path))


class SalmonIndex(ExternalProgramTask):
    """
    Build salmon index from human transcriptome
    Use Require descriptors for composition
    """

    # parameters
    human_mRNA_path = Parameter()
    salmon_path = Parameter()
    index_path = Parameter()
    n_threads = IntParameter()
    # requirements
    requires = Requires()
    human_rna = Requirement(HumanRNA)
    salmon = Requirement(Salmon)

    def output(self):
        """
        The index output is a folder (specified by user).
        Use flag file to mark complete
        :return: success flag file
        """
        flag = "__SUCCESS"
        return LocalTarget(os.path.join(str(self.index_path), flag))

    def program_args(self):
        return [
            self.input()["salmon"].path,
            "index",
            "-p",
            self.n_threads,
            "-t",
            self.input()["human_rna"].path,
            "-i",
            self.index_path,
        ]

    def run(self):
        super().run()
        # mark complete
        Path(self.output().path).touch()
