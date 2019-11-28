from luigi import ExternalTask, Parameter
import os
from luigi.contrib.external_program import ExternalProgramTask
from pathlib import Path
from .index import Salmon, SalmonIndex
from .luigi.task import Requires, Requirement


class FastqInput(ExternalTask):
    # constant
    fastq_root = os.path.join('data', 'fastq')
    # parameters
    file_id = Parameter()
    fastq_r1 = Parameter()
    fastq_r2 = Parameter()
    fastq_suffix = Parameter()

    def output(self):
        return {'R1': os.path.join(str(self.fastq_root),
                                   str(self.file_id) +
                                   str(self.fastq_r1) +
                                   str(self.fastq_suffix)),
                'R2': os.path.join(str(self.fastq_root),
                                   str(self.file_id) +
                                   str(self.fastq_r2) +
                                   str(self.fastq_suffix)),
                }


class SalmonQuant(ExternalProgramTask):
    # constant
    output_root = os.path.join('data', 'output')
    # parameters
    file_id = Parameter()
    human_mRNA_path = Parameter()
    salmon_path = Parameter()
    index_path = Parameter()
    fastq_root = os.path.join('data', 'fastq')
    fastq_r1 = Parameter()
    fastq_r2 = Parameter()
    fastq_suffix = Parameter()
    # requirements
    requires = Requires()
    fastq = Requirement(FastqInput)
    salmon = Requirement(Salmon)
    index = Requirement(SalmonIndex)

    def output(self):
        flag = '__SUCCESS'
        return os.path.join(self.output_root, str(self.file_id), flag)

    def program_args(self):
        return [
            self.input()['salmon'].path,
            "quant",
            "-i",
            os.path.dirname(self.input()['index'].path),
            "-l",
            "IA",
            "-1",
            self.input()['fastq']['R1'],
            "-2",
            self.input()['fastq']['R2'],
            "-o",
            self.output_root
        ]

    def run(self):
        super().run()
        # mark complete
        Path(self.output().path).touch()
