# Using Luigi to control bioinformatics data processing pipeline
## Introduction
Next generation sequencing of extracted mRNAs contain information on gene expression levels of samples. RNA-seq data processing is to first map the base-pair sequences to human genome and then count the number of reads per gene. The task is to submit a sample list with corresponding sequences as input to the workflow and get gene counts table of all samples as output. This workflow can be divided to the following tasks sequentially and controlled by luigi using external tasks and external program tasks.

1, Build genome indexes using genome annotations. 

2, Map sample sequences to human genome.
	
3, Summarize all sample files.

4, Preprocess to remove non-expressing genes. Visualize sample quanlities.
	
## Description
### Run environment
This pipeline is designed to run on a linux server locally. RNA-seq sequences usually take a lot of disk space, so it is not recommended to run this analysis on a laptop. 
This app is written in python3. Please make sure `luigi` `seaborn` and `pandas` are installed using
```bash
pip install luigi pandas seaborn
```
Other than that, `perl` is also required for the pipe line to work.

`RNA_seq/cli.py`: the script to modify paths and parameters

`data/fastq`: the folder to put your input sample sequences, fastq or fastq.gz files

`data/summary`: the folder where counts tables, cleaned data and QC stats will be stored

### Preparation
1, Download transcriptome annotations from [gencode](https://www.gencodegenes.org/).

2, Use provided script under `data/scripts/format_transcript.pl` to get `formatted transcriptome fasta file` and `annotation file` mapping transcript ID to gene names.
```bash
perl format_transcript.pl <gencode transcriptome fa> <formatted fa output> <annotation file output>
```
3, Download [Salmon](https://github.com/COMBINE-lab/salmon/releases).

### Usage
1, Put sample sequences into `data/fastq` folder. Don not create sub directories. 

2, Go to `RNA_seq/cli.py` and modify parameters containing where you put `formatted transcriptome fasta file`, `annotation file` and `Salmon`. Also the folder to put `salmon index file`, the `file pattern` of your fastq files and num of threads for `Salmon` to use.

3, Create a `file_id.txt` containing file IDs in your fastq files. Your fastq file should have the pattern `<file ID><fastq R1/R2><fastq suffix>`. Eg. sample01_R1.fastq.gz. `file ID` is `sample01`, `fastq R1` is `_R1` and `fastq suffix` is `.fastq.gz`

4, Run the pipeline from root directory with 
```bash
python -m RNA_seq --id file_id.txt
```