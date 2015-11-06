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
;# Source code location:     sysadmin/Private/AFS/AFS_tools/CloudBackups/cloud_loaddb.pl
;#
;# Local location:           
;#
;#
;# Propose:
;#
;#   This Perl program will (re)load database that is used by the  cloud_afsdump.pl  program
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
;#      Note that this code draws on development from the older version  loaddb.pl.  Where the vos
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
use Time::Local;

require  'flush.pl';
require  'ctime.pl';



$DEBUG                   = 0;                ;#  If true (not = 0) then debugging is enabled
$DUMPDIR                 = "/AFS_backups_in_Cloud/DailyDump";
$DBNAME                  = "new__AFS_Backup_DataBase";
$RELOAD                  = "Reload__AFS_Backup_DataBase";

@DB_Images       = ();
@DB_DumpDates    = ();
%DB_VolumeNames  = ();
@DB_History      = ();

$DBH             = "";
$TH_Images       = "";
$TH_DumpDates    = "";
$TH_VolumeNames  = "";
$TH_History      = "";


$Convert_Date_and_Time_string = 0;

$SQL_insert_Images = "INSERT INTO Images (volume, volume_id, create_time, mod_time, image_size, backup_level, dump_date, parent_dump_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

$SQL_insert_DumpDates = "INSERT INTO DumpDates (dump_date, number_of_images, number_of_fulls, size_of_fulls, total_size) VALUES (?, ?, ?, ?, ?)";

$SQL_insert_VolumeNames = "INSERT INTO VolumeNames (volume, last_query_date, increment_status) VALUES (?, ?, ?)";

$SQL_insert_History = "INSERT INTO History (volume, volume_id, create_time, mod_time, image_size, backup_level, dump_date, parent_dump_date, delete_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";



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





getopts('dh', \%opts) || FatalError("Pleaes Use -h for help");

ProcessCommandLine();

if ($DEBUG) {
    $DUMPDIR = "/dumpafs/TEST";
}


$DB_name = sprintf ("%s/DB/%s", $DUMPDIR, $DBNAME);
$INPUT = sprintf ("%s/DB/%s", $DUMPDIR, $RELOAD);


;#  Save existing database

if (-e $DB_name) {
    $tstamp = time;
    $tmpName = sprintf("%s_%s", $DB_name, $tstamp);
    `/bin/mv $DB_name $tmpName`;
}


$DBH = DBI->connect("DBI:SQLite:dbname=$DB_name", "", "", { PrintError => 1, RaiseError => 1 });

CreateDataBase();
ReadInputFile();
LoadDataBase();


;#  After the data is loaded create the Indexes

    ;#
    ;#  Create an index for the Images table using the volume column
    ;#
    $TH_Images = $DBH->prepare("CREATE INDEX Images_volume ON Images(volume)");
    $TH_Images->execute();
    $TH_Images->finish();
    $TH_Images = "";


    ;#
    ;#  Create an index for the History table using the volume column
    ;#
    $TH_History = $DBH->prepare("CREATE INDEX History_volume ON History(volume)");
    $TH_History->execute();
    $TH_History->finish();
    $TH_History = "";



if ($TH_Images) {   $TH_Images->finish();   }
if ($TH_DumpDates) {   $TH_DumpDates->finish();   }
if ($TH_VolumeNames) {   $TH_VolumeNames->finish();   }
if ($TH_History) {   $TH_History->finish();   }

$DBH->disconnect;



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   CreateDataBase
;#
;#   Purpose:      To get the database laid out and populated with test data
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   CreateDataBase  {


    ;#
    ;#   If the the structure of the Images table is modified also update $SQL_insert_Images
    ;#
    $DBH->do( "CREATE TABLE Images (volume                 CHAR NOT NULL,
                                    volume_id              INTEGER NOT NULL,
                                    create_time            INTEGER NOT NULL,
                                    mod_time               INTEGER NOT NULL,
                                    image_size             INTEGER NOT NULL,
                                    backup_level           CHAR NOT NULL,
                                    dump_date              INTEGER NOT NULL,
                                    parent_dump_date       INTEGER NOT NULL)" );

    ;#
    ;#   If the the structure of the DumpDates table is modified also update $SQL_insert_DumpDates
    ;#
    $DBH->do( "CREATE TABLE DumpDates (dump_date           INTEGER NOT NULL UNIQUE,
                                       number_of_images    INTEGER NOT NULL,
                                       number_of_fulls     INTEGER NOT NULL,
                                       size_of_fulls       INTEGER NOT NULL,
                                       total_size          INTEGER NOT NULL)" );

    ;#
    ;#   If the the structure of the VolumeNames table is modified also update $SQL_insert_VolumeNames
    ;#
    $DBH->do( "CREATE TABLE VolumeNames (volume            CHAR NOT NULL UNIQUE,
                                         last_query_date   INTEGER NOT NULL,
                                         increment_status  CHAR NOT NULL)" );



    ;#
    ;#   If the the structure of the History table is modified also update $SQL_insert_History
    ;#
    $DBH->do( "CREATE TABLE History (volume                CHAR NOT NULL,
                                    volume_id              INTEGER NOT NULL,
                                    create_time            INTEGER NOT NULL,
                                    mod_time               INTEGER NOT NULL,
                                    image_size             INTEGER NOT NULL,
                                    backup_level           CHAR NOT NULL,
                                    dump_date              INTEGER NOT NULL,
                                    parent_dump_date       INTEGER NOT NULL,
                                    delete_date            INTEGER NOT NULL)" );


}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   LoadDataBase  {

    my ($i, $line, $last_query_date, $increment_status);
    my ($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size, $backup_level, $dump_date, $parent_dump_date, $delete_date);
    my ($image_cnt, $image_cnt_fulls, $size_of_fulls, $size_of_images);

    my ($last_query_date_mm_dd_yyyy, $last_query_date_hh_mm_ss);
    my ($volume_create_time_mm_dd_yyyy, $volume_create_time_hh_mm_ss, $volume_mod_time_mm_dd_yyyy, $volume_mod_time_hh_mm_ss);
    my ($dump_date_mm_dd_yyyy, $dump_date_hh_mm_ss, $parent_dump_date_mm_dd_yyyy, $parent_dump_date_hh_mm_ss);
    my ($delete_date_mm_dd_yyyy, $delete_date_hh_mm_ss);


    printf ("Processing   VolumeNames   table\n");
    foreach $volume (sort(keys %DB_VolumeNames)) {
	;#
	;#  Add a new record for this volume to the "VolumeNames" table
	;#
	if ($Convert_Date_and_Time_string) {
	    ($volume, $last_query_date_mm_dd_yyyy, $last_query_date_hh_mm_ss, $increment_status) = split(/ /, $DB_VolumeNames{$volume});

            $last_query_date = ConvertDateString($last_query_date_mm_dd_yyyy, $last_query_date_hh_mm_ss);
	}
	else {
	    ($volume, $last_query_date, $increment_status) = split(/ /, $DB_VolumeNames{$volume});
	}

	if (!($TH_VolumeNames)) {   $TH_VolumeNames = $DBH->prepare($SQL_insert_VolumeNames);   }
	$TH_VolumeNames->execute($volume, $last_query_date, $increment_status);
    }
    if ($TH_VolumeNames) {   $TH_VolumeNames->finish();   $TH_VolumeNames = "";   }


    printf ("Processing   Image   table\n");
    for($i = 0;  $i <= $#DB_Images;  $i++) {
	;#
	;#  Add a new record for this dump to the "Image" table in the backup database
	;#
	if ($Convert_Date_and_Time_string) {
	    ($volume, $vid, $volume_create_time_mm_dd_yyyy, $volume_create_time_hh_mm_ss, $volume_mod_time_mm_dd_yyyy, $volume_mod_time_hh_mm_ss, $volume_size,
	     $backup_level, $dump_date_mm_dd_yyyy, $dump_date_hh_mm_ss, $parent_dump_date_mm_dd_yyyy, $parent_dump_date_hh_mm_ss) = split(/ /, $DB_Images[$i]);

	    $volume_create_time = ConvertDateString($volume_create_time_mm_dd_yyyy, $volume_create_time_hh_mm_ss);
	    $volume_mod_time = ConvertDateString($volume_mod_time_mm_dd_yyyy, $volume_mod_time_hh_mm_ss);
	    $dump_date = ConvertDateString($dump_date_mm_dd_yyyy, $dump_date_hh_mm_ss);
	    $parent_dump_date = ConvertDateString($parent_dump_date_mm_dd_yyyy, $parent_dump_date_hh_mm_ss);
	}
	else {
	    ($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size,
	     $backup_level, $dump_date, $parent_dump_date) = split(/ /, $DB_Images[$i]);
	}

	if (!($TH_Images)) {   $TH_Images = $DBH->prepare($SQL_insert_Images);   }
	$TH_Images->execute($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size, $backup_level, $dump_date, $parent_dump_date);
    }
    if ($TH_Images) {   $TH_Images->finish();   $TH_Images = "";   }


    printf ("Processing   DumpDates   table\n");
    for($i = 0;  $i <= $#DB_DumpDates;  $i++) {
	;#
	;#  Add a new record for this dump to the "DumpDates" table in the backup database
	;#
	if ($Convert_Date_and_Time_string) {
	    ($dump_date_mm_dd_yyyy, $dump_date_hh_mm_ss, $image_cnt, $image_cnt_fulls, $size_of_fulls, $size_of_images) = split(/ /, $DB_DumpDates[$i]);
	    $dump_date = ConvertDateString($dump_date_mm_dd_yyyy, $dump_date_hh_mm_ss);
	}
	else {
	    ($dump_date, $image_cnt, $image_cnt_fulls, $size_of_fulls, $size_of_images) = split(/ /, $DB_DumpDates[$i]);
	}

	if (!($TH_DumpDates)) {   $TH_DumpDates = $DBH->prepare($SQL_insert_DumpDates);   }
	$TH_DumpDates->execute($dump_date, $image_cnt, $image_cnt_fulls, $size_of_fulls, $size_of_images);
    }
    if ($TH_DumpDates) {   $TH_DumpDates->finish();   $TH_DumpDates = "";   }



    printf ("Processing   History   table\n");
    for($i = 0;  $i <= $#DB_History;  $i++) {
	;#
	;#  Add a new record for this dump to the "History" table in the backup database
	;#
	if ($Convert_Date_and_Time_string) {
	    ($volume, $vid, $volume_create_time_mm_dd_yyyy, $volume_create_time_hh_mm_ss, $volume_mod_time_mm_dd_yyyy, $volume_mod_time_hh_mm_ss, $volume_size,
	     $backup_level, $dump_date_mm_dd_yyyy, $dump_date_hh_mm_ss, $parent_dump_date_mm_dd_yyyy, $parent_dump_date_hh_mm_ss,
             $delete_date_mm_dd_yyyy, $delete_date_hh_mm_ss) = split(/ /, $DB_History[$i]);

	    $volume_create_time = ConvertDateString($volume_create_time_mm_dd_yyyy, $volume_create_time_hh_mm_ss);
	    $volume_mod_time = ConvertDateString($volume_mod_time_mm_dd_yyyy, $volume_mod_time_hh_mm_ss);
	    $dump_date = ConvertDateString($dump_date_mm_dd_yyyy, $dump_date_hh_mm_ss);
	    $parent_dump_date = ConvertDateString($parent_dump_date_mm_dd_yyyy, $parent_dump_date_hh_mm_ss);
	    $delete_date = ConvertDateString($delete_date_mm_dd_yyyy, $delete_date_hh_mm_ss);
	}
	else {
	    ($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size,
	     $backup_level, $dump_date, $parent_dump_date, $delete_date) = split(/ /, $DB_History[$i]);
	}

	if (!($TH_History)) {   $TH_History = $DBH->prepare($SQL_insert_History);   }
	$TH_History->execute($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size,
			     $backup_level, $dump_date, $parent_dump_date, $delete_date);
    }
    if ($TH_History) {   $TH_History->finish();   $TH_History = "";   }
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   ReadInputFile  {


    my ($line);
    my ($flag_VolumeNames, $flag_Images, $flag_DumpDates, $flag_History);

    $flag_VolumeNames = 0;
    $flag_Images = 0;
    $flag_DumpDates = 0;
    $flag_History = 0;


    if (open(INPUT, "$INPUT")) {
	;#
	;#  Walk through the file and relaod Backup Database
	;#
	while ($line = <INPUT>) {
	    chomp($line);
	    if ($line  eq  "BEGIN TABLE VolumeNames") {
		$flag_VolumeNames++;
	    }
	    elsif ($line  eq  "BEGIN TABLE Images") {
		$flag_Images++;
	    }
	    elsif ($line  eq  "BEGIN TABLE DumpDates") {
		$flag_DumpDates++;
	    }
	    elsif ($line  eq  "BEGIN TABLE History") {
		$flag_History++;
	    }

	    elsif ($line  eq  "List contents of the table   VolumeNames") {
		$flag_VolumeNames++;
		$flag_Images = 0;
		$flag_DumpDates = 0;
		$flag_History = 0;
		$Convert_Date_and_Time_string++;
	    }
	    elsif ($line  eq  "List contents of the table   Images") {
                $flag_VolumeNames = 0;
                $flag_Images++;
                $flag_DumpDates = 0;
                $flag_History = 0;
		$Convert_Date_and_Time_string++;
	    }
	    elsif ($line  eq  "List contents of the table   DumpDates") {
                $flag_VolumeNames = 0;
                $flag_Images = 0;
                $flag_DumpDates++;
                $flag_History = 0;
		$Convert_Date_and_Time_string++;
	    }
	    elsif ($line  eq  "List contents of the table   History") {
                $flag_VolumeNames = 0;
                $flag_Images = 0;
                $flag_DumpDates = 0;
		$flag_History++;
		$Convert_Date_and_Time_string++;
	    }
	    elsif (($line  eq  "END TABLE")  ||  ($line  eq  ""))  {
		if ($flag_VolumeNames)   {  $flag_VolumeNames = 0;   }
		if ($flag_Images)        {  $flag_Images = 0;   }
		if ($flag_DumpDates)     {  $flag_DumpDates = 0;   }
		if ($flag_History)       {  $flag_History = 0;   }
	    }
	    else {
		@_ = split(/ /, $line);
		if (!($_[0])) {   next;   }

		if ($flag_VolumeNames) {
		    $DB_VolumeNames{$_[0]} = $line;
		}
		elsif ($flag_Images) {
		    push (@DB_Images, $line);
		}
		elsif ($flag_DumpDates) {
		    push (@DB_DumpDates, $line);
		}
		elsif ($flag_History) {
		    push (@DB_History, $line);
		}
	    }
	}
        close(INPUT);
    }
    else {
	;#
	;#  Unable to read file
	;#
	print ("ERROR:  unable to read:  $!\n");
    }
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

	print ("\t -d      Debug mode\n");
	print ("\t -h      This Help\n");
	exit(0);
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   dnz  {
    return(defined($_[0]) && $_[0] ne '');
}


;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   ConvertDateString  {

    my ($mm_dd_yyyy, $hh_mm_ss) = @_;

    my ($second, $minute, $hour, $monthday, $month, $year);

    ($month, $monthday, $year) = split(/\//, $mm_dd_yyyy);
    ($hour, $minute, $second) = split(/:/, $hh_mm_ss);

    $month--;
    $year -= 1900;

    ;#  Convert string  "mm/dd/yyyy HH:MM:SS" into a time stamp
    return(timelocal($second, $minute, $hour, $monthday, $month, $year));
}

