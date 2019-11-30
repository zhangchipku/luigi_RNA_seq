from unittest import TestCase
import os
from pathlib import Path
from RNA_seq.quant import SalmonQuant
from RNA_seq.summary import SummarizeCounts, SummarizeMapping
from RNA_seq.preprocess import CleanCounts, AnnotationFile, MapFigure
from RNA_seq.luigi.task import Requirement, Requires, TargetOutput
from RNA_seq.luigi.target import SuffixPreservingLocalTarget
from luigi import build, format
from tempfile import TemporaryDirectory
import pandas as pd


class TaskTests(TestCase):
    def test_work_flow(self):
        expr = {
            "Name": ["transcript_1", "transcript_2", "transcript_3"],
            "NumReads": [50, 100, 200],
            "TPM": [20, 40, 80],
        }
        dummy_quant = pd.DataFrame(data=expr)
        gene = {
            "transcript_ID": ["transcript_1", "transcript_2", "transcript_3"],
            "transcript_length": [500, 200, 600],
            "gene_name": ["gene_1", "gene_1", "gene_2"],
        }
        dummy_annot = pd.DataFrame(data=gene)
        summary = {
            "Sample": ["sample_1", "sample_2"],
            "Mapped_Reads": [1000000, 2000000],
            "Mapped_Rate": [69, 82],
        }
        dummy_summary = pd.DataFrame(data=summary)
        with TemporaryDirectory() as tmp:
            # setup fake sample quant output
            sample_1_dir = os.path.join(tmp, "sample_1")
            sample_2_dir = os.path.join(tmp, "sample_2")
            os.mkdir(sample_1_dir)
            os.mkdir(sample_2_dir)
            dummy_quant.to_csv(
                os.path.join(sample_1_dir, "quant.sf"), index=False, sep="\t"
            )
            dummy_quant.to_csv(
                os.path.join(sample_2_dir, "quant.sf"), index=False, sep="\t"
            )
            Path(os.path.join(sample_1_dir, SalmonQuant.flag)).touch()
            Path(os.path.join(sample_2_dir, SalmonQuant.flag)).touch()
            # setup fake annotation file
            annot_path = os.path.join(tmp, "annotation.file")
            dummy_annot.to_csv(annot_path, index=False, sep="\t")
            # setup fake id file
            id_path = os.path.join(tmp, "id.txt")
            with open(id_path, "w") as file:
                file.write("sample_1\nsample_2\n")
            # setup fake summary
            sum_path = os.path.join(tmp, "DummySum.txt")
            dummy_summary.to_csv(sum_path, index=False, sep="\t")

            # setup fake classes
            class DummyQuant(SalmonQuant):
                output_root = tmp

            class DummyCount(SummarizeCounts):
                output_root = tmp
                input_root = tmp

                def requires(self):
                    with open(str(self.ID_path), "r") as id_file:
                        ids = id_file.read().splitlines()
                    return {x: self.clone(DummyQuant, file_id=x) for x in ids}

                count_out = TargetOutput(
                    root_dir=output_root,
                    ext="_count.csv",
                    target_class=SuffixPreservingLocalTarget,
                )
                tpm_out = TargetOutput(
                    root_dir=output_root,
                    ext="_tpm.csv",
                    target_class=SuffixPreservingLocalTarget,
                )

            class DummySum(SummarizeMapping):
                output_root = tmp
                out_file = TargetOutput(
                    root_dir=output_root, target_class=SuffixPreservingLocalTarget
                )

            class DummyFig(MapFigure):
                output_root = tmp
                requires = Requires()
                sum_map = Requirement(DummySum)
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

            class DummyClean(CleanCounts):
                output_root = tmp
                requires = Requires()
                map_fig = Requirement(DummyFig)
                raw_counts = Requirement(DummyCount)
                annotation = Requirement(AnnotationFile)
                tpm_out = TargetOutput(
                    target_class=SuffixPreservingLocalTarget,
                    ext="_tpm.csv",
                    root_dir=output_root,
                )
                count_out = TargetOutput(
                    target_class=SuffixPreservingLocalTarget,
                    ext="_count.csv",
                    root_dir=output_root,
                )

            # build and test endpoints
            build(
                [
                    DummyClean(
                        ID_path=id_path,
                        annotation_path=annot_path,
                        human_mRNA_path="fake",
                        salmon_path="fake",
                        index_path="fake",
                        fastq_r1="fake",
                        fastq_r2="fake",
                        fastq_suffix="fake",
                        n_threads=5,
                    )
                ],
                local_scheduler=True,
                log_level="INFO",
            )
            self.assertTrue(os.path.exists(os.path.join(tmp, "DummyFig_reads.png")))
            self.assertTrue(os.path.exists(os.path.join(tmp, "DummyFig_rate.png")))
            self.assertEqual(
                pd.read_csv(os.path.join(tmp, "DummyClean_count.csv")).iloc[0, 1], 150
            )
            self.assertEqual(
                pd.read_csv(os.path.join(tmp, "DummyClean_tpm.csv")).iloc[1, 1], 80
            )
