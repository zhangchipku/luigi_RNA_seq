# Using Luigi to control RNA-seq data processing pipeline
## Introduction
### Background
Next generation sequencing of extracted mRNAs contain information on gene expression levels of samples. RNA-seq data processing is to first map the base-pair sequences to human genome and then count the number of reads per gene. The task is to submit a sample list with corresponding sequences as input to the workflow and get gene counts table of all samples as output. This workflow can be divided to the following tasks sequentially and controlled by luigi using external tasks and external program tasks.

1, Build genome indexes using genome annotations. 

2, Map sample sequences to human genome.
	
3, Summarize all sample files.

4, Preprocess to remove non-expressing genes. Visualize sample quality.

### Motivation
Each of the four steps mentioned above can be run by executing shell commands. However, when there are over 100 samples, it is easy to get tedious and loose track of the progress.

A commonly used solution is to use shell scripts containing for loops to batch submit all samples. However, we will still need to wait for the samples to be quantified before manually executing the next step.

Luigi package in python offers a good task scheduler system that tracks job status automatically and will sequentially kick start all prerequisite tasks by executing the last step. 

I built this pipeline to provide full automation for RNA-seq processing. All we need to do is to specify where all the input files are and run. If there is some run failures, luigi will tell us which job failed so the user can easily make necessary changes. 	
## Description
### Features
Once configured, the pipeline will kick off and provide the cleaned counts and tpm tables ready for downstream analysis. It will also provide visualizations for sample QC for easy identifications of problematic samples.

This pipeline utilized: 

1, Salmon, a "wicked-fast" RNA-seq quantification package.

2, Luigi, a python task scheduler

3, pandas for dataframe operations

4, seaborn for QC plots

5, Descriptors and compositions I learned from CSCI29 to improve the workflow 
### Input files
1, Sample fastq files containing the sequences to be quantified. 

2, Whole transcriptome sequences from annotation databases like gencode, UCSC.
### Output
1, Summary table and plots with mapping rates, total number of reads and number of mapped reads for each sample.

2, Raw transcripts quantification counts and tpms table containing all samples

3, Cleaned counts and tpms table sum by gene name, with non-expressing genes removed. Ready for downstream analysis.

### Run environment
This pipeline is designed to run on a linux server locally. RNA-seq sequences usually take a lot of disk space, so it is not recommended to run this analysis on a laptop. 
This app is written in python3. Please make sure `luigi` `seaborn` and `pandas` are installed using
```bash
pip install luigi pandas seaborn
```
Other than that, `perl` is also required for the pipe line to work.
### Structure
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