use strict;
if (@ARGV <1) {
	print "$0 <id file> <input_folder> <output_file>\n";
	exit;
}
sub getCountingSummary {
	my $id = shift;
	open (SUMMARY, "<", "$ARGV[1]/$id/salmon.log") || die $@;

	my ($total_reads, $mapped_reads, $percent_mapped);
	while (my $aline = <SUMMARY>) {
		chomp($aline);
		if ($aline =~ /^Observed\s(\d+)\stotal\sfragments/) {
			$total_reads=$1;
		}
		elsif ($aline =~ /Counted\s(\d+)\stotal\sreads/) {
			$mapped_reads=$1;
		}
		elsif ($aline =~ /Mapping\srate\s\=\s(\d+\.\d+)\%/) {
			$percent_mapped=$1;
		}

	}
	
	close(SUMMARY);
	

	return "$id\t$total_reads\t$mapped_reads\t$percent_mapped";
}


open (OUTPUT, $ARGV[2]) || die $@;

print OUTPUT "Sample\tTotal_Reads\tMapped_Reads\tMapped_Rate\n";

open(FILE, $ARGV[0]) || die $@;
while(my $line = <FILE>) {
	chomp($line);
	my @items = split(/\t/, $line);
	my $id = $items[0];

	print OUTPUT getCountingSummary($id), "\n";
}
close(FILE);
close(OUTPUT);
