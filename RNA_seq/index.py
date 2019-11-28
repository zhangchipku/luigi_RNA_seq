from luigi import ExternalTask, Parameter, Task
import os
from luigi.local_target import LocalTarget
from luigi.contrib.external_program import ExternalProgramTask
from pathlib import Path


class HumanRNA(ExternalTask):
	human_mRNA_path = Parameter()
	
	def output(self):
		return LocalTarget(str(human_mRNA_path))

class Salmon_Index(ExternalProgramTask):
	salmon_path = Parameter()
	index_path = Parameter()

	def requires(self):
		return self.clone(HumanRNA)
	
	def output(self):
		flag = '__SUCCESS'
		return os.path.join(index_path, flag)
	
	def program_args(self)
		return [
			str(self.salmon_path),
			"index",
			"-t",
			self.input().path,
			"-i",
			self.index_path
	
	def run(self):
		super().run()
		Path(self.output().path).touch()
		
