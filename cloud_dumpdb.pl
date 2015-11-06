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
;# Source code location:     sysadmin/Private/AFS/AFS_tools/CloudBackups/cloud_dumpdb.pl
;#
;# Local location:           
;#
;#
;# Propose:
;#
;#   This Perl program will dump out the backup database used by  cloud_afsdump.pl
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
;#
;# History:
;#
;#   Version 1.1     TMM   04/07/2015
;#
;#      Initial code drop
;#
;#      Note that this code draws on development from the older version dumpdb.pl.  Where the vos
;#      dump images where saved without modification on premise.
;#
;#
;#
;#=   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =


;#
;# SYSTEM GLOBAL VARIABLES
;#
use Getopt::Std;
use DBI;

require  'flush.pl';
require  'ctime.pl';



;#	
$DEBUG                   = 0;                ;#  If true (not = 0) then debugging is enabled
$ConvertTstamp           = 1;                ;#  If true (not = 0) then covert all time stamps into text strings
$WriteSTDout             = 1;                ;#  If true (not = 0) then display database records to standard out
$DUMPDIR                 = "/AFS_backups_in_Cloud/DailyDump";
$DBNAME                  = "AFS_Backup_DataBase";



$DBH                     = "";
$TH_Images               = "";
$TH_DumpDates            = "";
$TH_VolumeNames          = "";
$TH_History              = "";

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


getopts('mqdh', \%opts) || FatalError("Pleaes Use -h for help");

ProcessCommandLine();

if ($DEBUG) {
    $DUMPDIR = "/dumpafs/TEST";
}


;#  Define the file names for standard Out and standard Error used to capture output from external commands like /usr/afsws/etc/vos
$STDout = sprintf ("/tmp/%s__%s__stdout", $ProgName, $tstamp);
$STDerr = sprintf ("/tmp/%s__%s__stderr", $ProgName, $tstamp);


$DB_name = sprintf ("%s/DB/%s", $DUMPDIR, $DBNAME);
$LOGFILE = sprintf ("%s/LOG/dumpdb_log_%s", $DUMPDIR, GetTimeStamp());

unless (open(LOG, ">$LOGFILE")) {
    printf ("\nERROR:  Can not create log file (%s),  $!\n\n", $LOGFILE);
    exit 1;
}


$DBH = DBI->connect("DBI:SQLite:dbname=$DB_name", "", "", { PrintError => 1, RaiseError => 1 });

Query_DataBase();

if ($TH_Images) {   $TH_Images->finish();   }
if ($TH_DumpDates) {   $TH_DumpDates->finish();   }
if ($TH_VolumeNames) {   $TH_VolumeNames->finish();   }
if ($TH_History) {   $TH_History->finish();   }

$DBH->disconnect;
close(LOG);



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   Query_DataBase  {

    my (@row, $last_query_date, $increment_status);
    my ($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size, $backup_level, $dump_date, $parent_dump_date, $delete_date);
    my ($image_cnt, $image_cnt_fulls, $size_of_fulls, $size_of_images);


    print ("List contents of the table   VolumeNames\n");
    if (!($ConvertTstamp)) {   print (LOG "BEGIN TABLE VolumeNames\n");   }   else {   print (LOG "List contents of the table   VolumeNames\n");   }
    $TH_VolumeNames = $DBH->prepare("SELECT * FROM VolumeNames");
    $TH_VolumeNames->execute();
    while (@row = $TH_VolumeNames->fetchrow_array()) {
	$volume = $row[0];
	if ($ConvertTstamp) {  $last_query_date = ConvertDate($row[1]);  }   else {  $last_query_date = $row[1];  }
	$increment_status = $row[2];
	if ($WriteSTDout) {   print ("$volume $last_query_date $increment_status\n");   }
	print (LOG "$volume $last_query_date $increment_status\n");
    }
    if (!($ConvertTstamp)) {   print (LOG "END TABLE\n");   }


    print ("\n\n\nList contents of the table   Images\n");
    if (!($ConvertTstamp)) {   print (LOG "BEGIN TABLE Images\n");   }   else {   print (LOG "\n\n\nList contents of the table   Images\n");   }
    $TH_Images = $DBH->prepare("SELECT * FROM Images ORDER BY volume AND dump_date");
    $TH_Images->execute();
    while (@row = $TH_Images->fetchrow_array()) {
	$volume              = $row[0];
	$vid                 = $row[1];
	if ($ConvertTstamp) {  $volume_create_time  = ConvertDate($row[2]);  }   else {  $volume_create_time  = $row[2];  }
	if ($ConvertTstamp) {  $volume_mod_time  = ConvertDate($row[3]);  }   else {  $volume_mod_time  = $row[3];  }
	$volume_size         = $row[4];
	$backup_level        = $row[5];
	if ($ConvertTstamp) {  $dump_date  = ConvertDate($row[6]);  }   else {  $dump_date  = $row[6];  }
	if ($ConvertTstamp) {  $parent_dump_date  = ConvertDate($row[7]);  }   else {  $parent_dump_date  = $row[7];  }

	if ($WriteSTDout) {   print ("$volume $vid $volume_create_time $volume_mod_time $volume_size $backup_level $dump_date $parent_dump_date \n");   }
	print (LOG "$volume $vid $volume_create_time $volume_mod_time $volume_size $backup_level $dump_date $parent_dump_date \n");
    }
    if (!($ConvertTstamp)) {   print (LOG "END TABLE\n");   }


    print ("\n\n\nList contents of the table   History\n");
    if (!($ConvertTstamp)) {   print (LOG "BEGIN TABLE History\n");   }   else {   print (LOG "\n\n\nList contents of the table   History\n");   }
    $TH_History = $DBH->prepare("SELECT * FROM History ORDER BY volume AND dump_date");
    $TH_History->execute();
    while (@row = $TH_History->fetchrow_array()) {
	$volume              = $row[0];
	$vid                 = $row[1];
	if ($ConvertTstamp) {  $volume_create_time  = ConvertDate($row[2]);  }   else {  $volume_create_time  = $row[2];  }
	if ($ConvertTstamp) {  $volume_mod_time  = ConvertDate($row[3]);  }   else {  $volume_mod_time  = $row[3];  }
	$volume_size         = $row[4];
	$backup_level        = $row[5];
	if ($ConvertTstamp) {  $dump_date  = ConvertDate($row[6]);  }   else {  $dump_date  = $row[6];  }
	if ($ConvertTstamp) {  $parent_dump_date  = ConvertDate($row[7]);  }   else {  $parent_dump_date  = $row[7];  }
	if ($ConvertTstamp) {  $delete_date  = ConvertDate($row[8]);  }   else {  $delete_date  = $row[8];  }

	if ($WriteSTDout) {   print ("$volume $vid $volume_create_time $volume_mod_time $volume_size $backup_level $dump_date $parent_dump_date $delete_date \n");   }
	print (LOG "$volume $vid $volume_create_time $volume_mod_time $volume_size $backup_level $dump_date $parent_dump_date $delete_date \n");
    }
    if (!($ConvertTstamp)) {   print (LOG "END TABLE\n");   }


    print ("\n\n\nList contents of the table   DumpDates\n");
    if (!($ConvertTstamp)) {   print (LOG "BEGIN TABLE DumpDates\n");   }   else {   print (LOG "\n\n\nList contents of the table   DumpDates\n");   }
    $TH_DumpDates = $DBH->prepare("SELECT * FROM DumpDates ORDER BY dump_date");
    $TH_DumpDates->execute();
    while (@row = $TH_DumpDates->fetchrow_array()) {
	if ($ConvertTstamp) {  $dump_date  = ConvertDate($row[0]);  }   else {  $dump_date  = $row[0];  }
        $image_cnt        = $row[1];
        $image_cnt_fulls  = $row[2];
        $size_of_fulls    = $row[3];
        $size_of_images   = $row[4];

        if ($WriteSTDout) {   print ("$dump_date $image_cnt $image_cnt_fulls $size_of_fulls $size_of_images \n");   }
        print (LOG "$dump_date $image_cnt $image_cnt_fulls $size_of_fulls $size_of_images \n");
    }
    if (!($ConvertTstamp)) {   print (LOG "END TABLE\n");   }
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   ConvertDate  {

    my ($time_in_seconds) = @_;
    my ($second, $minute, $hour, $monthday, $month, $year, $weekday, $yday, $isdst);


    ;#  Convert into string  "mm/dd/yyyy HH:MM:SS"
    ($second, $minute, $hour, $monthday, $month, $year, $weekday, $yday, $isdst) = localtime($time_in_seconds);
    $month++; $year += 1900;
    return(sprintf("%0.2d/%0.2d/%d %0.2d:%0.2d:%0.2d", $month, $monthday, $year, $hour, $minute, $second));
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   dnz  {
    return(defined($_[0]) && $_[0] ne '');
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
;#   Subroutine:   ProcessCommandLine
;#
;#   Purpose:      To process the command line options
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   ProcessCommandLine  {


    if (dnz($opts{'h'})) {
	DisplayHelp();
    }

    if (dnz($opts{'m'})) {
	$ConvertTstamp = 0;
    }

    if (dnz($opts{'q'})) {
	$WriteSTDout = 0;
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

	print ("\t -m      Time stamps are NOT converted to text strings\n");
	print ("\t -q      Do not write the database records to standard out\n");
	print ("\t -d      Debug mode\n");
	print ("\t -h      This Help\n");
	exit(0);
}
