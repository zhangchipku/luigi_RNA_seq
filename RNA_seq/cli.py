from luigi import build
import os
import argparse
from RNA_seq.wrapup import AllReports


def main():
    # system level parameters
    # trascriptome fasta file
    transcriptome = os.path.join(
        "data", "human", "gencode.v27.transcripts.formated.fa"
    )
    # salmon program executable
    salmon_path = os.path.join("data", "salmon", "bin", "salmon")
    # folder to store salmon transcriptome index
    index_path = os.path.join("data", "index")
    # transcript annotation file
    annotation_path = os.path.join(
        "data", "human", "GRCh38.gencode.v27.transcripts.annot"
    )

    # project level parameters
    # fastq file patterns, eg sample01_1.fastq.gz, sample01_2.fastq.gz
    fastq_r1 = "_1"
    fastq_r2 = "_2"
    fastq_suffix = ".fastq.gz"
    # number of threads for salmon to parallel compute
    n_threads = 15

    parser = argparse.ArgumentParser()
    parser.add_argument("--id")
    args = parser.parse_args()
    ID_path = args.id

    build(
        [
            AllReports(
                ID_path=ID_path,
                annotation_path=annotation_path,
                transcriptome=transcriptome,
                salmon_path=salmon_path,
                index_path=index_path,
                fastq_r1=fastq_r1,
                fastq_r2=fastq_r2,
                fastq_suffix=fastq_suffix,
                n_threads=n_threads,
            )
        ],
        local_scheduler=True,
        log_level="INFO",
    )
