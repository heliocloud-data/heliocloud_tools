#!/usr/bin/perl

print("Usage:  perl ssw_count_or_replace.pl [replace]\n");

# Recursively works from the current directory down and
# replaces all the plot, oplot, tv, and tvscl old-style proc calls with the
# Jupyter-safe helper routines (that pass params to the new IDL function calls)
#
# Ideally run this from the SOLARSOFT root
# Antunes, June 2022
#
# Dev note: using a script rather than a one-liner because there are multiple
# replaces and I needed to add logging (# replacements, for example)
# So perl script it is.
#
# Looks for items in replaceset[] in godir() and appends 'jn' to each
# Also converts tvscl -> jntv,/scale and oplot -> jnplot,/overplot

$replaceon = 0; # 1 = yes, replace, 0 = just count them, no replace
# items to replace (we'll add the trailing , in the regex)
@replaceset = ('plot', 'tvscl', 'tv', 'oplot','xyouts');
@newset = ('jnplot','jntv,/scale','jntv','jnplot,/overplot','jnxyouts');
$startin="."; # start in cwd

if ($ARGV[0] =~ 'replace') {
    $replaceon = 1;
}

if ($replaceon) {
    print("Replace mode\n");
} else {
    print("Just counting\n");
}

# globals, ugh
$global_all = 0;
$global_dirs = 0;
$global_filecount = 0;
$global_changecount = 0;

sub godir() {
    my $curdir=$_[0];

    chdir($curdir);
    opendir(DIRH,".");
    my @inhere=readdir(DIRH);
    closedir(DIRH);

    foreach my $fname (@inhere) {
	if ($fname !~ /^\./) {
	   # skipping all . directories, both hidden and . and ..
	    if (-d $fname) {
		if ($global_dirs % 100 == 0) { print "... checking $global_dirs directories ...\n";}
		++$global_dirs;
		#print "Entering $fname\n";
		&godir($fname);
	    } elsif ($fname =~ /.pro$/i) {
		++$global_all;
		#print "Processing $fname.";
		open(FIN,'<',$fname);
		my @array = <FIN>;
		close(FIN);
		$count=0;
		foreach $line (@array) {
		    # ignore keyword if it is after a comment
		    # and it should start or have whitespace before it
		    # and end with a , as per a proper procedure call
		    $index=0;
		    foreach $key (@replaceset) {
			# note space in front (essential!) because of regex
			#$newkey = ' jn' . $key;
			$newkey = " " . @newset[$index];
			#print("checking $key vs $newkey ");
			if ($line !~ m/\;.*$key/ig) {
			    if ($line =~ m/(^|\s)$key,/ig) {
				++$count;
				$line =~ s/(^|\s)$key,/$newkey,/ig;
			    }
			}
			++$index;
		    }
		}
		# replace file iff internals got changed
		if ($count > 0) {
		    ++$global_filecount;
		    push(@fnames, $fname);
		    $global_changecount += $count;
		    if ($replaceon) {
			print("Updating $count lines in $fname.\n");
			##open(FOUT,'>',$fname.'b');  # test version, copies
			open(FOUT,'>',$fname);  # prod version, overwrites
			print FOUT @array;
			close(FOUT);
		    }
		}
	    }
	}
    }
    unless ($curdir =~ /^\.$/) {chdir("..");}
}

&godir($startin);

print("Search was for: "+join(", ",@contents)+"\n");
if ($replaceon) {
    $infostr = "replaced";
} else {
    $infostr = "contains";
}

print join(", ",@fnames);
print("\nDone! For $global_all files in $global_dirs dirs, $global_filecount files $infostr contents, $global_changecount lines $infostr contents.\n");
#use Path::Tiny qw(path);
#my $data = $fname->slurp_utf8;
#$data =~ s/plot/splot/g;
#$fname->spew_utf8($data);
