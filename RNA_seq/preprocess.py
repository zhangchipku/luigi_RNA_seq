import pandas as pd
import seaborn as sns
from luigi import Parameter, Task, IntParameter, format, ExternalTask, LocalTarget
from .summary import SummarizeCounts, SummarizeMapping
from .luigi.target import SuffixPreservingLocalTarget
from .luigi.task import Requirement, Requires, TargetOutput


class MapFigure(Task):
    """
    Visualize mapping stats from mapping summary table
    Require all sample quantification
    Output two png files containing mapping stats
    Use Require and targetoutput descriptor for composition
    """
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
    # output
    rate_out = TargetOutput(
        target_class=SuffixPreservingLocalTarget,
        root_dir=output_root,
        ext="_rate.png",
        format=format.Nop,
    )
    reads_out = TargetOutput(
        target_class=SuffixPreservingLocalTarget,
        root_dir=output_root,
        ext="_reads.png",
        format=format.Nop,
    )

    def output(self):
        return {"rate": self.rate_out(), "reads": self.reads_out()}

    def run(self):
        # Read summary
        with self.input()["sum_map"].open("r") as file:
            df = pd.read_table(file)
        # write png files
        with self.output()["rate"].open("w") as rate_out:
            sns.barplot(x="Sample", y="Mapped_Rate", data=df).figure.savefig(rate_out)
        with self.output()["reads"].open("w") as reads_out:
            sns.barplot(x="Sample", y="Mapped_Reads", data=df).figure.savefig(reads_out)


class AnnotationFile(ExternalTask):
    """
    Make sure annotation file containing transcript ID mapping to gene name exists
    """
    annotation_path = Parameter()

    def output(self):
        return LocalTarget(str(self.annotation_path))


class CleanCounts(Task):
    """
    Clean up counts and tpm table
    Map transcript IDs to gene names
    Sum up counts and tpms by gene
    Remove non-expressiong genes
    Require all sample quantification
    Output two csv files containing gene counts and tpms
    Use Require and targetoutput descriptor for composition
    """
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
    # output
    tpm_out = TargetOutput(
        target_class=SuffixPreservingLocalTarget, ext="_tpm.csv", root_dir=output_root
    )
    count_out = TargetOutput(
        target_class=SuffixPreservingLocalTarget, ext="_count.csv", root_dir=output_root
    )

    def output(self):
        return {"tpm": self.tpm_out(), "count": self.count_out()}

    def run(self):
        # Read annotations and raw transcript tables
        with self.input()["annotation"].open("r") as file:
            anno = pd.read_table(file)
        with self.input()["raw_counts"]["tpm"].open("r") as file:
            tpm_raw = pd.read_csv(file)
        with self.input()["raw_counts"]["count"].open("r") as file:
            count_raw = pd.read_csv(file)
        # Map transcripts to gene names, sum by gene
        tpm_clean = (
            pd.merge(anno, tpm_raw, on="transcript_ID")
            .groupby("gene_name")
            .sum()
            .drop("transcript_length", axis=1)
            .reset_index()
        )
        count_clean = (
            pd.merge(anno, count_raw, on="transcript_ID")
            .groupby("gene_name")
            .sum()
            .drop("transcript_length", axis=1)
            .reset_index()
        )
        # use avg tpm from all samples > 0.5 as cutoff
        # drop non-expressing genes
        keep = tpm_clean.mean(axis=1) > 0.5
        tpm_clean = tpm_clean[keep]
        count_clean = count_clean[keep]
        # write csvs
        with self.output()["count"].open("w") as out:
            count_clean.to_csv(out, index=False)
        with self.output()["tpm"].open("w") as out:
            tpm_clean.to_csv(out, index=False)
