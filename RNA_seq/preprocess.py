import pandas as pd
import seaborn as sns
from luigi import Parameter, Task, IntParameter, format, ExternalTask, LocalTarget
import os
from .summary import SummarizeCounts, SummarizeMapping
from .luigi.target import SuffixPreservingLocalTarget
from .luigi.task import Requirement, Requires


class MapFigure(Task):
    # constant
    output_root = SummarizeMapping.output_root
    # parameters
    ID_path = Parameter()
    human_mRNA_path = Parameter()
    salmon_path = Parameter()
    index_path = Parameter()
    fastq_r1 = Parameter()
    fastq_r2 = Parameter()
    fastq_suffix = Parameter()
    n_threads = IntParameter()

    # requirements
    requires = Requires()
    sum_map = Requirement(SummarizeMapping)

    def output(self):
        return {
            "rate": SuffixPreservingLocalTarget(
                os.path.join(self.output_root, "map_rates.png"), format=format.Nop
            ),
            "reads": SuffixPreservingLocalTarget(
                os.path.join(self.output_root, "map_reads.png"), format=format.Nop
            ),
        }

    def run(self):
        with self.input()["sum_map"].open("r") as file:
            df = pd.read_table(file)

        with self.output()["rate"].open("w") as rate_out:
            sns.barplot(x="Sample", y="Mapped_Rate", data=df).figure.savefig(
                rate_out
            )

        with self.output()["reads"].open("w") as reads_out:
            sns.barplot(x="Sample", y="Mapped_Reads", data=df).figure.savefig(
                reads_out
            )


class AnnotationFile(ExternalTask):
    annotation_path = Parameter()

    def output(self):
        return LocalTarget(str(self.annotation_path))


class CleanCounts(Task):
    # constant
    output_root = SummarizeMapping.output_root
    # parameters
    ID_path = Parameter()
    human_mRNA_path = Parameter()
    salmon_path = Parameter()
    index_path = Parameter()
    fastq_r1 = Parameter()
    fastq_r2 = Parameter()
    fastq_suffix = Parameter()
    n_threads = IntParameter()
    annotation_path = Parameter()

    # requirements
    requires = Requires()
    map_fig = Requirement(MapFigure)
    annotation = Requirement(AnnotationFile)
    raw_counts = Requirement(SummarizeCounts)

    def output(self):
        return {
            "tpm": SuffixPreservingLocalTarget(
                os.path.join(self.output_root, "clean_tpm.csv")
            ),
            "count": SuffixPreservingLocalTarget(
                os.path.join(self.output_root, "clean_count.csv")
            ),
        }

    def run(self):
        with self.input()["annotation"].open("r") as file:
            anno = pd.read_table(file)
        with self.input()["raw_counts"]["tpm"].open("r") as file:
            tpm_raw = pd.read_csv(file)
        with self.input()["raw_counts"]["count"].open("r") as file:
            count_raw = pd.read_csv(file)

        tpm_clean = (
            pd.merge(anno, tpm_raw, on="transcript_ID")
            .groupby("gene_name")
            .sum()
            .drop("transcript_length", axis=1)
            .reset_index()
        )
        keep = tpm_clean.mean(axis=1) > 0.5
        tpm_clean = tpm_clean[keep]

        count_clean = (
            pd.merge(anno, count_raw, on="transcript_ID")
            .groupby("gene_name")
            .sum()
            .drop("transcript_length", axis=1)
            .reset_index()
        )
        count_clean = count_clean[keep]

        with self.output()["count"].open("w") as out:
            count_clean.to_csv(out, index=False)

        with self.output()["tpm"].open("w") as out:
            tpm_clean.to_csv(out, index=False)
