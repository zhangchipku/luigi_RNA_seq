from luigi import build
import os
import argparse
from RNA_seq.summary import SummarizeMapping, SummarizeCounts
from RNA_seq.index import SalmonIndex


def main():
    # system level parameters
    # human trascriptome fasta file
    human_mRNA_path = os.path.join('data', 'human', 'gencode.v27.transcripts.formated.fa')
    # salmon program executable
    salmon_path = os.path.join('data', 'salmon', 'bin', 'salmon')
    # folder to store salmon transcriptome index
    index_path = os.path.join('data', 'index')

    # project level parameters
    # fastq file patterns, eg sample01_1.fastq.gz, sample01_2.fastq.gz
    fastq_r1 = '_1'
    fastq_r2 = '_2'
    fastq_suffix = '.fastq.gz'
    # number of threads for salmon to parallel compute
    n_threads = 15

    parser = argparse.ArgumentParser()
    parser.add_argument("--id")
    args = parser.parse_args()
    ID_path = args.id
    build([SalmonIndex(human_mRNA_path=human_mRNA_path,
                       salmon_path=salmon_path,
                       index_path=index_path,
                       n_threads=n_threads)],
          local_scheduler=True,
          log_level="INFO",
          )

    # build(
    #     [SummarizeMapping(ID_path=ID_path,
    #                       human_mRNA_path=human_mRNA_path,
    #                       salmon_path=salmon_path,
    #                       index_path=index_path,
    #                       fastq_r1=fastq_r1,
    #                       fastq_r2=fastq_r2,
    #                       fastq_suffix=fastq_suffix,
    #                       n_threads=n_threads),
    #      SummarizeCounts(ID_path=ID_path,
    #                      human_mRNA_path=human_mRNA_path,
    #                      salmon_path=salmon_path,
    #                      index_path=index_path,
    #                      fastq_r1=fastq_r1,
    #                      fastq_r2=fastq_r2,
    #                      fastq_suffix=fastq_suffix,
    #                      n_threads=n_threads)],
    #     local_scheduler=True,
    #     log_level="INFO",
    # )
