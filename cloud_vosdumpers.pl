#!/usr/bin/perl
;#
;#    $Header:$
;#
;#    $Revision:$
;#
;#    $Date:$
;#    $Locker:$
;#    $Author:$
;#
;#
;#  Copyright (C) 2015 Terry McCoy     (terry@nd.edu)
;# 
;#   This program is free software; you can redistribute it and/or
;#   modify it under the terms of the GNU General Public License 
;#   as published by the Free Software Foundation; either version 2
;#   of the License, or (at your option) any later version.
;#
;#   This program is distributed in the hope that it will be useful, 
;#   but WITHOUT ANY WARRANTY; without even the implied warranty of
;#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
;#   GNU General Public License for more details.
;#
;#   You should have received a copy of the GNU General Public License 
;#   along with this program; if not, write to the Free Software
;#   Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
;# 
;#
;#
;#
;#   This software was designed and written by:
;#
;#       Terry McCoy                            (terry@nd.edu)
;#       University of Notre Dame
;#       Office of Information Technologies
;#
;#
;#
;# Source code location:     sysadmin/Private/AFS/AFS_tools/CloudBackups/cloud_vosdumpers.pl
;#
;# Local location:           
;#
;#
;# Propose:
;#
;#   This Perl program will perform the vos dump commands as directed via the  cloud_afsdump.pl  program
;#
;#   
;#
;#
;# Logic overview:
;#
;#
;#
;#
;# Command Line Parameters:
;#
;#   This program takes these additional optional parameters
;#
;#
;#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
;#
;#             K E E P I N G     O L D     H I S T R O Y
;#
;#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
;#
;# History:
;#
;#   Version 1.1     TMM   07/19/2007
;#
;#      Initial code drop  -  Working for several months many bugs fixed before dropped into SCCS
;#
;#
;#   Version 1.2     TMM   10/23/2007
;#
;#      Increase the number of queues that can be supported from 3 to 6.  This change is needed to
;#      support on going modifications to the program "afsdump.pl" at and above version 1.5
;#
;#
;#   Version 1.3     TMM   08/25/2009
;#
;#      Change the path to "/usr/bin/perl"  to suport move from Solaris 8 to RHEL 5.3
;#
;#
;#
;#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
;#
;#
;#
;#            H I S T R O Y 
;#
;#
;#   Version 1.1     TMM   04/07/2015
;#
;#      Initial code drop
;#
;#      Note that this code draws on development from the older version  afsdumps.pl.  Where the vos
;#      dump images where saved without modification on premise.
;#
;#
;#
;#
;#=   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =


;#
;# SYSTEM GLOBAL VARIABLES
;#
use Getopt::Std;
use File::stat;

require  'flush.pl';
require  'ctime.pl';



;#	
$DEBUG                   = 0;                ;#  If true (not = 0) then debugging is enabled
$DUMPDIR                 = "/AFS_backups_in_Cloud/DailyDump";

$SHORT_SLEEP = 15;
$LONG_SLEEP = 60;


;#
;#  Snag program name (drop the full pathname) :
;#
$ProgName = $0;
$tmpName= "";
GETPROG: while ($letr = chop($ProgName)) {
        $_ = $letr;
        /\// && last GETPROG;
        $tmpName .= $letr;
}
$ProgName = reverse($tmpName); 

$tstamp = time;
$QueueNumber = "";
$HostName = `/bin/hostname`;
chomp($HostName);


getopts('q:dh', \%opts) || FatalError("Pleaes Use -h for help");

ProcessCommandLine();

if ($DEBUG) {
    $DUMPDIR = "/dumpafs/TEST";
}




;#  Define the file names for standard Out and standard Error used to capture output from external commands like /usr/afsws/etc/vos
$STDout = sprintf ("/tmp/%s__%s__%s__stdout", $ProgName, $tstamp, $ProgPID);
$STDerr = sprintf ("/tmp/%s__%s__%s__stderr", $ProgName, $tstamp, $ProgPID);


$LOGFILE = sprintf ("%s/LOG/vosdumper_log__%s__queue_%s", $DUMPDIR, $tstamp, $QueueNumber);

unless (open(LOG, ">$LOGFILE")) {
    printf ("\nERROR:  Can not create log file (%s),  $!\n\n", $LOGFILE);
    exit 1;
}


if ($QueueNumber  eq  "1") {
    $CmdInput_file =   "$DUMPDIR/CMD_queue_1";
    $READY_file =      "$DUMPDIR/READY_queue_1";
    $ALIVE_file =      "$DUMPDIR/ALIVE_queue_1";
    $FINISH_file =     "$DUMPDIR/FINISH_queue_1";
}
elsif ($QueueNumber  eq  "2") {
    $CmdInput_file =   "$DUMPDIR/CMD_queue_2";
    $READY_file =      "$DUMPDIR/READY_queue_2";
    $ALIVE_file =      "$DUMPDIR/ALIVE_queue_2";
    $FINISH_file =     "$DUMPDIR/FINISH_queue_2";
}
elsif ($QueueNumber  eq  "3") {
    $CmdInput_file =   "$DUMPDIR/CMD_queue_3";
    $READY_file =      "$DUMPDIR/READY_queue_3";
    $ALIVE_file =      "$DUMPDIR/ALIVE_queue_3";
    $FINISH_file =     "$DUMPDIR/FINISH_queue_3";

}
elsif ($QueueNumber  eq  "4") {
    $CmdInput_file =   "$DUMPDIR/CMD_queue_4";
    $READY_file =      "$DUMPDIR/READY_queue_4";
    $ALIVE_file =      "$DUMPDIR/ALIVE_queue_4";
    $FINISH_file =     "$DUMPDIR/FINISH_queue_4";
}
elsif ($QueueNumber  eq  "5") {
    $CmdInput_file =   "$DUMPDIR/CMD_queue_5";
    $READY_file =      "$DUMPDIR/READY_queue_5";
    $ALIVE_file =      "$DUMPDIR/ALIVE_queue_5";
    $FINISH_file =     "$DUMPDIR/FINISH_queue_5";
}
elsif ($QueueNumber  eq  "6") {
    $CmdInput_file =   "$DUMPDIR/CMD_queue_6";
    $READY_file =      "$DUMPDIR/READY_queue_6";
    $ALIVE_file =      "$DUMPDIR/ALIVE_queue_6";
    $FINISH_file =     "$DUMPDIR/FINISH_queue_6";
}
else {
    FatalError("ERROR:  Unexpected value [$QueueNumber]  specified for queue number");
}


;#
;# Check if this program is still running from a previous invocation
;#
if ((-e  $READY_file)  ||  (-e  $ALIVE_file)  ||  (-e  $FINISH_file)) {
    FatalError("Program appears to already be running");
}


;#
;#  Wait for the  cloud_afsdump.pl  program to signal it is ready to provide vos dump commands
;#
LogIt("Waiting for the  cloud_afsdump.pl  program to signal it is ready");
$flag_loop = 1;

while ($flag_loop) {
    if (-e $CmdInput_file) {
	if (open(INPUT, $CmdInput_file)) {
	    ;#
	    ;#  Walk through the file, look for the start phrase
	    ;#
	    while ($line = <INPUT>) {
		chomp($line);
		if ($line  eq  "Hello vos dumper") {   $flag_loop = 0;   }
	    }
	    close(INPUT);
	}
	else {
	    unlink ($STDout, $STDerr);
	    FatalError("ERROR:  Unable to read $CmdInput_file:  $!");
	}
    }
    else {
	sleep($SHORT_SLEEP);        ;#  File does not exist keep sleeping, wait for the  cloud_afsdump.pl  program to signal
    }
}


;#
;#  Signal the  cloud_afsdump.pl  program that this vosdumper queue is ready to accept vos dump commands
;#
LogIt("Signal the  cloud_afsdump.pl  program that this vosdumper queue is ready");
unlink ($CmdInput_file);

unless (open(READY, ">$READY_file")) {
    FatalError("ERROR:  Can not create $READY_file,  $!");
}

printf (READY  "Queue number %s is ready on host %s\n", $QueueNumber, $HostName);
close(READY);


;#
;#  Now wait for the  cloud_afsdump.pl  program to write the vos dump commands to the input file (vos dump command file) for this vosdumper queue
;#
LogIt("Wait for the  cloud_afsdump.pl  program to write the vos dump commands");
$previous_file_size = 0;
$current_file_size = 0;
$flag_loop = 1;   
while ($flag_loop) {
    if (-e $CmdInput_file) {
	;#
	;#  stat the vos dump command file, compare previous file attributes to current to determine if the file is still being updated
	;#
	$fstat_h = stat($CmdInput_file);
	if ($fstat_h->size  >  0) {
	    if ($previous_file_size  ==  0) {
		$previous_file_size = $fstat_h->size;
		sleep($SHORT_SLEEP);             ;#  first time we stat'd the vos dump command file, take a long nap
	    }
	    else {
		if ($previous_file_size  ==  $fstat_h->size) {
		    if ($current_file_size  ==  $previous_file_size) {
			$flag_loop = 0;
		    }
		    else {
			$current_file_size = $previous_file_size;
			sleep($SHORT_SLEEP);     ;#  looks like we have reached the end, take one last long nap
		    }
		}
		else {
		    $previous_file_size = $fstat_h->size;
		    sleep($SHORT_SLEEP);         ;#  save the size of the file for comparison on the next sample
		}
	    }
	}
	else {
	   sleep($SHORT_SLEEP);                  ;#  the size of the vos dump command file is NOT greater than zero, keep on sleeping
	}
    }
    else {
	sleep($LONG_SLEEP);                     ;#  the vos dump command file does not exist, sleep awhile longer
    }
}



;#
;#  Now open the vos dump command file and read in all the vos dump commands
;#
LogIt("Now open the vos dump command file and read in all the vos dump commands");
if (open(INPUT, $CmdInput_file)) {
    ;#
    ;#  Walk through the file, look for the start phrase
    ;#
    while ($line = <INPUT>) {
	chomp($line);
	push(@DumpCMDS, $line);
    }
    close(INPUT);
}
else {
    unlink ($STDout, $STDerr);
    FatalError("ERROR:  Unable to read $CmdInput_file:  $!");
}



unless (open(ALIVE, ">$ALIVE_file")) {
    FatalError("ERROR:  Can not create $ALIVE_file,  $!");
}




;#
;#  Now begin to execute the vos dump commands
;#
for ($i = 0;    $i <= $#DumpCMDS;    $i++) {
    @_ = split(/ /, $DumpCMDS[$i]);
    $volume = $_[3];
    $volume =~ s/.backup//g;
    $backup_level = $_[5];

    LogIt("Dump $volume  at level  $backup_level");
    LogIt("CMD:   $DumpCMDS[$i]");

    `$DumpCMDS[$i]`;

    printf (ALIVE  "%s\n", $DumpCMDS[$i]);
    flush(ALIVE);
}
close(ALIVE);


;#
;#  Signal the  cloud_afsdump.pl  program that we are done
;#
unless (open(FINISH, ">$FINISH_file")) {
    FatalError("ERROR:  Can not create $FINISH_file,  $!");
}

$msg_string = sprintf ("Queue number %s on host %s   Has executed %d vos dump commands", $QueueNumber, $HostName, ($#DumpCMDS + 1));
printf (FINISH  "%s\n", $msg_string);
close(FINISH);
LogIt("$msg_string");





;# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
;# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub   LogIt  {

    my ($message) = @_;

    my ($package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask);
    my ($tag, $indent);
        
    ($package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask) = caller(1);
    $subroutine = "main" unless dnz($subroutine);
    $subroutine =~ s/^main:://;
    $indent = IndentSpacing();
    $tag = sprintf("%s%s[%s]", GetTimeStamp(), $indent, $subroutine);

    printf (LOG  "%s %s\n", $tag, $message);
    flush(LOG);
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   FatalError  {

    my ($message) = @_;

    my ($package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask);
    my ($tag, $indent);

    ($package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask) = caller(1);

    $subroutine = "main" unless dnz($subroutine);
    $subroutine =~ s/^main:://;

    $indent = IndentSpacing();
    $tag = sprintf("%s%s[%s]", GetTimeStamp(), $indent, $subroutine);

    $output = sprintf ("%s %s\n", $tag, $message);

    ;# If any text in $STDerr then display and log it as well
    if ( -s $STDerr ) {
        foreach $line (`$CAT $STDerr`) {
            $output = sprintf ("%s%s %s", $output, $tag, $line);
        }
    }

    printf ("%s", $output);
    printf (LOG  "%s", $output);
    close(LOG);
    exit 1;
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#
;#  Returns a string of spaces, based on the number of subroutines
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   IndentSpacing  {

    my ($package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask);
    my ($i, $spaces);

    $subroutine ="top";
    $spaces = "";
    $i = 1;

    while (dnz($subroutine)) {
        $i++;
        ($package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask) = caller($i);
        $subroutine = "" unless dnz($subroutine);
        $spaces = sprintf("%s  ", $spaces);
    }
    return($spaces);
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#
;#  Return a string with the date and time   2006_11_17__13:34:04
;#
;# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub   GetTimeStamp  {

    my ($second,$minute,$hour,$monthday,$month,$year,$weekday,$yday,$isdst);

    ($second,$minute,$hour,$monthday,$month,$year,$weekday,$yday,$isdst) = localtime(time());
    $month++; $year += 1900;
    return (sprintf("%d_%0.2d_%0.2d__%0.2d:%0.2d:%0.2d", $year,$month,$monthday,$hour,$minute,$second));
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   dnz  {
    return(defined($_[0]) && $_[0] ne '');
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   ProcessCommandLine
;#
;#   Purpose:      To process the command line options
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   ProcessCommandLine  {


    if (dnz($opts{'h'})) {
	DisplayHelp();
    }

    if (dnz($opts{'q'})) {
	$QueueNumber = $opts{'q'};
    }

    if (dnz($opts{'d'})) {
        $DEBUG = 1;
    }
}


;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   DisplayHelp
;#
;#   Purpose:      To display the help message for this program
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   DisplayHelp  {


	printf ("\n\n\t%s [ Options ]\n\nWhere Options are:\n\n", ProgName);

	print ("\t -q      Specify the queue to use (1, 2, 3, 4, 5 or 6)\n");
	print ("\t -d      Debug mode\n");
	print ("\t -h      This Help\n");
	exit(0);
}







