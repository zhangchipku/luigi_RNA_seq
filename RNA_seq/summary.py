from luigi import Parameter, Task
import os
from luigi.contrib.external_program import ExternalProgramTask
from luigi.util import inherits
from .quant import SalmonQuant
from .luigi.target import SuffixPreservingLocalTarget
import pandas as pd
from .luigi.task import TargetOutput
from .index import SalmonIndex


@inherits(SalmonIndex)
class SummarizeMapping(ExternalProgramTask):
    """
    Find mapped reads and rates from sample quantification logs
    Use a perl script
    Require all sample quantification
    Output a table text file containing the mapping stats for all samples
    Use targetoutput descriptor for composition
    """

    # constant
    output_root = os.path.join("data", "summary")
    perl_script = os.path.join("data", "scripts", "get_salmon_summary.pl")
    input_root = SalmonQuant.output_root
    # parameters
    ID_path = Parameter()
    fastq_r1 = Parameter()
    fastq_r2 = Parameter()
    fastq_suffix = Parameter()

    out_file = TargetOutput(
        root_dir=output_root, target_class=SuffixPreservingLocalTarget
    )

    def output(self):
        return self.out_file()

    # requirements
    def requires(self):
        ids = get_file_ids(str(self.ID_path))
        return {x: self.clone(SalmonQuant, file_id=x) for x in ids}

    def program_args(self):
        return [
            "perl",
            self.perl_script,
            self.ID_path,
            self.input_root,
            self.temp_output_path,
        ]

    def run(self):
        with self.output().temporary_path() as self.temp_output_path:
            super().run()


@inherits(SummarizeMapping)
class SummarizeCounts(Task):
    """
    Find transcript counts and tpms from sample quantification tables
    Require all sample quantification
    Output two csv files containing the transcript counts and tpms for all samples
    Use targetoutput descriptor for composition
    """

    # constant
    output_root = SummarizeMapping.output_root
    input_root = SalmonQuant.output_root

    # outputs
    count_out = TargetOutput(
        root_dir=output_root, ext="_count.csv", target_class=SuffixPreservingLocalTarget
    )
    tpm_out = TargetOutput(
        root_dir=output_root, ext="_tpm.csv", target_class=SuffixPreservingLocalTarget
    )

    def output(self):
        return {"count": self.count_out(), "tpm": self.tpm_out()}

    # requirements
    def requires(self):
        ids = get_file_ids(str(self.ID_path))
        return {x: self.clone(SalmonQuant, file_id=x) for x in ids}

    def _get_sample_quant(self, x):
        """
        Get the path for each sample's count tables to read
        :param x: str, file_id
        :return: path
        """
        return os.path.join(os.path.dirname(self.input()[x].path), "quant.sf")

    def run(self):
        # Read in all file ids
        ids = get_file_ids(str(self.ID_path))
        # Get the framework from the first table
        with open(self._get_sample_quant(ids[0]), "r") as file:
            df = pd.read_table(file)

        counts = df[["Name", "NumReads"]].rename(
            columns={"Name": "transcript_ID", "NumReads": ids[0]}
        )
        tpm = df[["Name", "TPM"]].rename(
            columns={"Name": "transcript_ID", "TPM": ids[0]}
        )
        # loop through the rest of the files
        for x in ids[1:]:
            with open(self._get_sample_quant(x)) as file:
                df = pd.read_table(file)
            counts[x] = df["NumReads"]
            tpm[x] = df["TPM"]
        # write csv files
        with self.output()["count"].temporary_path() as count_out:
            counts.to_csv(count_out, index=False)
        with self.output()["tpm"].temporary_path() as tpm_out:
            tpm.to_csv(tpm_out, index=False)


def get_file_ids(id_text_file):
    with open(id_text_file, "r") as file:
        ids = file.read().splitlines()
    return ids
