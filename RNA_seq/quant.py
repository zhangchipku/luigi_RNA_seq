from luigi import ExternalTask, Parameter, IntParameter
import os
from luigi.local_target import LocalTarget
from luigi.contrib.external_program import ExternalProgramTask
from luigi.util import inherits
from pathlib import Path
from .index import Salmon, SalmonIndex
from .luigi.task import Requires, Requirement


class FastqInput(ExternalTask):
    """
    Make sure sample sequence files exists
    """

    # constant
    fastq_root = os.path.join("data", "fastq")
    # parameters
    file_id = Parameter()
    fastq_r1 = Parameter()
    fastq_r2 = Parameter()
    fastq_suffix = Parameter()

    def _get_fastq_path(self, reads):
        """
        get file path for fastq files
        :param reads: str, r1 or r2
        :return: path
        """
        return os.path.join(
                    str(self.fastq_root),
                    str(self.file_id) + reads + str(self.fastq_suffix),
                )

    def output(self):
        return {
            "R1": LocalTarget(self._get_fastq_path(str(self.fastq_r1))),
            "R2": LocalTarget(self._get_fastq_path(str(self.fastq_r2)))
        }


@inherits(FastqInput, SalmonIndex)
class SalmonQuant(ExternalProgramTask):
    """
    Run the sample sequence quantification using Salmon
    Outputs logs containing mapping stats and transcript counts/tpms
    Use require descriptors for composition
    """

    # constant
    output_root = os.path.join("data", "output")
    fastq_root = os.path.join("data", "fastq")
    flag = "__SUCCESS"

    # requirements
    requires = Requires()
    fastq = Requirement(FastqInput)
    salmon = Requirement(Salmon)
    index = Requirement(SalmonIndex)

    def output(self):
        """
        The output is a folder, named by file_id.
        Use flag file to mark complete
        :return: success flag file
        """
        return LocalTarget(os.path.join(self.output_root, str(self.file_id), self.flag))

    def program_args(self):
        return [
            self.input()["salmon"].path,
            "quant",
            "-p",
            self.n_threads,
            "-i",
            os.path.dirname(self.input()["index"].path),
            "-l",
            "A",
            "-1",
            self.input()["fastq"]["R1"].path,
            "-2",
            self.input()["fastq"]["R2"].path,
            "-o",
            os.path.dirname(self.output().path),
        ]

    def run(self):
        super().run()
        # mark complete
        Path(self.output().path).touch()
