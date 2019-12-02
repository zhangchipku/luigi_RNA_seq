import pandas as pd
import seaborn as sns
from luigi import Parameter, Task, IntParameter, format, ExternalTask, LocalTarget
from luigi.util import inherits
from .summary import SummarizeCounts, SummarizeMapping
from .luigi.target import SuffixPreservingLocalTarget
from .luigi.task import Requirement, Requires, TargetOutput


@inherits(SummarizeMapping)
class MapFigure(Task):
    """
    Visualize mapping stats from mapping summary table
    Require all sample quantification
    Output two png files containing mapping stats
    Use Require and targetoutput descriptor for composition
    """

    # constant
    output_root = SummarizeMapping.output_root

    # requirements
    requires = Requires()
    sum_map = Requirement(SummarizeMapping)
    # output
    rate_out = TargetOutput(
        target_class=SuffixPreservingLocalTarget,
        root_dir=output_root,
        ext="_rate.pdf",
        format=format.Nop,
    )
    reads_out = TargetOutput(
        target_class=SuffixPreservingLocalTarget,
        root_dir=output_root,
        ext="_reads.pdf",
        format=format.Nop,
    )

    def output(self):
        return {"rate": self.rate_out(), "reads": self.reads_out()}

    def run(self):
        # Read summary
        with self.input()["sum_map"].open("r") as file:
            df = pd.read_table(file)
        # Create plots
        sns.set(rc={"figure.figsize": (12, 8)})
        rate_plot = make_plots("Mapped_Rate", df)
        reads_plot = make_plots("Mapped_Reads", df)
        #  write pdf files
        with self.output()["rate"].temporary_path() as rate_out:
            rate_plot.figure.savefig(rate_out, dpi=600)
        with self.output()["reads"].temporary_path() as reads_out:
            reads_plot.figure.savefig(reads_out, dpi=600)


def make_plots(y_lab, df):
    """
    Use seaborn to create summary QC plots
    :param y_lab: str, Y label to plot
    :param df: pandas dataframe containing the data to plot
    :return: seaborn plot
    """
    plot = sns.barplot(x="Sample", y=y_lab, data=df)
    plot.set_xticklabels(df.Sample, rotation=45, ha="right")
    return plot


class AnnotationFile(ExternalTask):
    """
    Make sure annotation file containing transcript ID mapping to gene name exists
    """

    annotation_path = Parameter()

    def output(self):
        return LocalTarget(str(self.annotation_path))


def merge_annotation(anno, table):
    """
    Map transcripts to gene names, sum expression by gene
    :param anno: pandas dataframe containing transcript_id and gene name
    :param table: pandas dataframe containing either tpm or count table
    :return: pandas dataframe
    """
    return (
        pd.merge(anno, table, on="transcript_ID")
            .groupby("gene_name")
            .sum()
            .drop("transcript_length", axis=1)
            .reset_index()
    )


@inherits(SummarizeCounts, AnnotationFile)
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

    # requirements
    requires = Requires()
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
        tpm_clean = merge_annotation(anno, tpm_raw)
        count_clean = merge_annotation(anno, count_raw)
        # use avg tpm from all samples > 0.5 as cutoff
        # drop non-expressing genes
        keep = tpm_clean.mean(axis=1) > 0.5
        tpm_clean = tpm_clean[keep]
        count_clean = count_clean[keep]
        # write csvs
        with self.output()["count"].temporary_path() as out:
            count_clean.to_csv(out, index=False)
        with self.output()["tpm"].temporary_path() as out:
            tpm_clean.to_csv(out, index=False)
