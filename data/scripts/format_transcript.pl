if (@ARGV <3) {
	print "$0 <gencode transcriptome fa> <formatted fa output> <annotation file output>\n";
	exit;
}

open ANNO, ">AGRV[2]";
open FORMATTED, ">AGRV[1]";
open INPUT, "AGRV[0]";

print ANNO "transcript_ID\tgene_ID\ttranscript_name\tgene_name\ttranscript_length\ttranscript_type\n";
foreach $line (<INPUT>){
	if ($line =~ /^>/){
		chomp $line;
		@temp = split(/\|/, $line);
		print FORMATTED $temp[0], "\n";
		$temp[0] =~ s/^>//;
		print ANNO "$temp[0]\t$temp[1]\t$temp[4]\t$temp[5]\t$temp[6]\t$temp[7]\n";
	}
	else{
		print FORMATTED $line;
	}
}

close ANNO;
close FORMATTED;
close INPUT;
