from luigi import Parameter, Task, IntParameter
import os
from luigi.contrib.external_program import ExternalProgramTask
from .quant import SalmonQuant
from .luigi.target import suffix_preserving_atomic_file
import pandas as pd


class SummarizeMapping(ExternalProgramTask):
    # constant
    output_root = os.path.join('data', 'summary')
    perl_script = os.path.join('data', 'scripts', 'get_salmon_summary.pl')
    input_root = SalmonQuant.output_root
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
    def requires(self):
        with open(str(self.ID_path), 'r') as id_file:
            ids = id_file.readlines()
        return {x: self.clone(SalmonQuant, file_id=x) for x in ids}

    def output(self):
        map_summary = 'mapping_summary.txt'
        return suffix_preserving_atomic_file(os.path.join(self.output_root, map_summary))

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

class SummarizeCounts(Task):
    # constant
    output_root = os.path.join('data', 'summary')
    input_root = SalmonQuant.output_root
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
    def requires(self):
        with open(str(self.ID_path), 'r') as id_file:
            ids = id_file.readlines()
        return {x: self.clone(SalmonQuant, file_id=x) for x in ids}

    def output(self):
        count_summary = 'count_summary.csv'
        tpm_summary = 'tpm_summary.csv'
        return {'count': suffix_preserving_atomic_file(os.path.join(self.output_root, count_summary)),
                'tpm': suffix_preserving_atomic_file(os.path.join(self.output_root, tpm_summary))}

    def _get_sample_quant(self, x):
        return os.path.join(os.path.dirname(self.input()[x]), 'quant.sf')

    def run(self):
        with open(str(self.ID_path), 'r') as id_file:
            ids = id_file.readlines()

        with open(self._get_sample_quant(ids[0]), 'r') as file:
            df = pd.read_table(file)

        counts = df[['Name', 'NumReads']].rename(columns={'Name': 'gene_id', 'NumReads': ids[0]})
        tpm = df[['Name', 'TPM']].rename(columns={'Name': 'gene_id', 'TPM': ids[0]})

        for x in ids[1:]:
            with open(self._get_sample_quant(x)) as file:
                df = pd.read_table(file)
            counts[x] = df['NumReads']
            tpm[x] = df['TPM']

        with self.output()['count'].open('w') as count_out:
            counts.to_csv(count_out)

        with self.output()['tpm'].open('w') as tpm_out:
            tpm.to_csv(tpm_out)
