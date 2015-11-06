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
;# Source code location:     sysadmin/Private/AFS/AFS_tools/CloudBackups/cloud_afsdump.pl
;#
;# Local location:           
;#
;#
;# Propose:
;#
;#   This Perl program will manage the creation of backup images from the contents of our AFS file
;#   system (/afs/nd.edu).  These images will be deduped and compressed and stored within an object
;#   store.  Ultimately this object store will be off premise (AWS S3 or another cloud storage provider)
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
;#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
;#
;#             K E E P I N G     O L D     H I S T R O Y
;#
;#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
;# History:
;#
;#   Version 1.1     TMM   07/19/2007
;#
;#      Initial code drop  -  Working for several months many bugs fixed before dropped into SCCS
;#
;#
;#   Version 1.2     TMM   08/03/2007
;#
;#      In the function Get_RW_VolumeInfo if the vos listvol command fails it is not a Fatal error
;#
;#
;#   Version 1.3     TMM   09/24/2007
;#
;#      Added the code for the -V flag  exclude the listed volumes.  Also removed the development
;#      code that specified the AFS servers in the cell.
;#
;#
;#   Version 1.4     TMM   10/11/2007
;#
;#      Added code to exclude the restored volumes from being backed up again.  Volumes that start with
;#      the prefix defined in $Restore_Volume_PREFIX are considered restored volumes.  Currently this is
;#      declared to be the prefix string "R."
;#
;#
;#   Version 1.5     TMM   10/25/2007
;#
;#      Added code to support the VOS dump clients writing the dump files directly into the IMAGE archive
;#      changes made to the functions  "CreatedDumpCommands" "VerifyDumpedVolumes"  and  "ArchiveDumpedVolumes"
;#
;#      Increased the number of input queues (vos dump threads) from 3 to 6.
;#
;#      Also modified the function "WriteDumpCommands2Queue" to shuffle the vos dump commands as they are
;#      assigned to the processing queues for the vos dumpers
;#
;#      Added code to use Jeff Hutzelman's scan dump utilities to list the meta data for each vos dump image
;#      changes are within CreatedDumpCommands()  VerifyDumpedVolumes()  ArchiveDumpedVolumes()  
;#      RemoveOlderImages()  and  RemoveImagesOfPurgedAFSvolumes()
;#
;#      An addition side effect is that the scan dump utilities will detect problems such as orphan files
;#      within the vos dump image.  Hence we have another array DUMP_needs_salvage to track these events.
;#
;#      Modified the function DisplayErrorAndLogIt() so that any errors messages contained within STDerr
;#      are sanitized to remove ^M "carriage returns".  Also these text from the STDerr file is indented
;#      but does not have the time stamp and originating subroutine name
;#
;#      Made the error and warning message more informative and consistent.
;#
;#
;#   Version 1.6     TMM   01/10/2008
;#
;#      Changed what we do with the meta files of dump files that are being removed from the IMAGE archive.
;#      We are now going to move them from the IMAGE archive and save them the meta file archive.  The purpose
;#      is that we still have these dump files on tape and if we wanted to browse the corresponding meta file
;#      we would need to pull it off tape.
;#
;#      This functional enhancment creates a TODO  which is to purge the meta file archive of old meta files
;#      whose corresponding vos dump file no longer exist on tape.
;#
;#
;#   Version 1.7     TMM   01/30/2008
;#
;#      Change the parent dump date attribute such that we can do either accumulative or differential incremenatal
;#      backup methods see the explanation below for the variable DUMP_STYLE.  This was a major rewrite / hack on
;#      the function BuildDumpList().
;#
;#      Also fixed a bug within the function RemoveOlderImages() the meta files for images that were being
;#      purged from the IMAGE archived were getting saved without the ".meta" extension on the file name
;#
;#
;#      Found very significant BUG.
;#
;#         For the vos dump command we were using the "parent dump date" as the time parameter.  This worked
;#         for the most part because the backup volumes were recloned on the file servers between 1:00AM and 2:30AM.
;#         And this backup program was run at 3AM.  But if the backup command ran later in the day, say 11:30AM
;#         then the next time the backup program ran the "parent dump date" would be 11:30AM and we would miss
;#         and changes that happened before 11:30AM.
;#
;#         Made changes to the function CreateDumpCommands() to use the backup volumes modification time for the
;#         volume's previous dump.  Both dump styles accumulative or differential incremenatal are supported
;#
;#
;#
;#   Version 1.8     TMM   02/01/2008
;#
;#      Within the function BuildDumpList()  we did a query to the "Image" table for each volume and ordered the
;#      return values by the dump date.  As the database has grown this SQL query was taking over 26 minutes.
;#      Reduced the query time to 40 seconds by removing the ORDER BY directive from the SQL statement and
;#      using two variables to save the dump date information during the volumes query.
;#
;#      Also removed the ORDER BY directive from the SQL statement in the RemoveOlderImages() function.
;#      This also had the same effect of reducing the query time from 26 minutes to about 50 seconds
;#
;#      Also reworked the code within the RemoveOlderImages() function to make it a bit cleaner.  Now we only
;#      write to the log when the incremental storage exceeds 25%
;#       
;#      Within the function Get_RW_VolumeInfo() added some counters to track these events and log them
;#
;#           -  number of RW volumes queried from each AFS file server
;#           -  total number of RW volumes queried from all the AFS file servers
;#           -  number of current volumes within the database
;#           -  number of volumes in the database that have not been queried within the last 4 days
;#
;#
;#
;#   Version 1.9     TMM   02/11/2008
;#
;#      Bug within the function CreateDumpCommands().  The logic in place made an assumpation that the volume
;#      had an value within the array DB_recent.  Of course new volumes would NOT have an entry, therefore the
;#      bug was that the local variable "$volume" would get clobbered and become a null value.  Hence no new
;#      volumes are getting backed up.  This bug was introduced in the modifacation from version 1.7
;#
;#      Also within the function BuildDumpList()  we did not handle the volumes who have backup images that have
;#      reached their max size.  Did alot of rework on this function to clean up the messy logic.
;#
;#      Reworked the function RemoveOlderImages().  Now the trigger for INCREMENTAL_MAX_SIZE only happens
;#      when the size of the incremental dumps for the volume are over 65% of the volumes full dump size.
;#
;#
;#      TMM Feb 12th 2008
;#
;#           Skip volumes that start with "N." 
;#
;#
;#      TMM April 4th 2008
;#
;#           Found bug in the function CreateDumpCommands().  When we dumped the array DB_recent it steped
;#           on the value for $backup_level.  Hence the database entry was "good" but the file name used
;#           within the archive has the previous backup level embedded in the name.  The fix was to create
;#           a variable $previous_backup_level thus preserving the value within $backup_level
;#
;#
;#
;#   Version 1.10     TMM   10/02/2008
;#
;#      Reworked the function RemoveOlderImages().  Replaced the variable $DELAY_OLD_IMAGE_PURGE with the
;#      a variable called $SLA_DATA_RETENTION.  This will specify our data retention of user data.  The
;#      logic within the previous version of RemoveOlderImages() had some major flaws  :-(
;#
;#
;#
;#   Version 1.11     TMM   03/25/2009
;#
;#      Reworked the function RemoveOlderImages().  Now we verify the duplicate full dump images by looking
;#      at the last modifcation time stamp from the AFS volume and the size.
;#
;#      Also removed temp code for mapping the 192.168.211.X address to bigboote.helios.nd.edu
;#
;#
;#
;#   Version 1.12     TMM   08/21/2009
;#
;#      Reworking the code to support running on RHEL 5.3  (moving away from Solaris)
;#
;#
;#
;#
;#   Version 1.3  (old version numbers:  1.13)     TMM   09/08/2009   
;#
;#      Comment changes added the header tag for RCS  using the RCS version number now at 1.3
;#
;#
;#
;#
;#   Version 1.4      TMM   11/19/2009   
;#
;#      Reworking the code to support removing backup image files from the IMAGE archive.  Specifically
;#      adding a feature such that we can choose to have backup image files for specific AFS volumes
;#      remain within the IMAGE archive regardless of the following global variables:
;#
;#           SLA_DATA_RETENTION
;#           DELAY_DELETE
;#
;#      This required modifications to the following functions 
;#
;#           RemoveImagesOfPurgedAFSvolumes()
;#           RemoveOlderImages()
;#
;#     This feature is enabled via the command line parameter "-K".  Also reworked the manual force full
;#     command line option "-f" to put a date stamp on the specified file, such that we don't keep creating
;#     a full dump each day.
;#
;#     Note a bug found during testing when dump produced fewer volumes than the number of dump queues things
;#     became bound up because the "FINNISH" files were not created
;#
;#
;#
;#
;#   Version 1.5      TMM   12/04/2009   
;#
;#      Reworking the code remove debug and development code for the changes made in version 1.4.  Also increased the
;#      default values for these two parameters.
;#
;#            $INCREMENTAL_MIN_SIZE   from   500MB   -->   1GB
;#            $INCREMENTAL_MAX_SIZE   from    20GB   -->  30GB
;#
;#
;#
;#
;#   Version 1.6      TMM   04/24/2013
;#
;#      For implementation of disaster recovery for the AFS cell, we have begun to use the "vos shadow" command to
;#      create replicas of the AFS volumes within our cell.  These replicas are what is called a shadow volume, in
;#      that it is exact copy of the volume but it does not appear with the vldb.  The overall concept is that each
;#      AFS file server will have "DR" node that will have have a shadow volume for every volume on the AFS file server.
;#
;#      Therefore it is a requirement that the backup process does not create backups of these shadow volumes,  as a
;#      naming convention for the "DR" node it is the AFS file server name prefix with a "dr-".  To accommodate this a
;#      change was made within the function  GetFileServers()  to filter out any AFS file server that has a "dr-" prefix.
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
;#      dump images where saved without modification on premise.  And those vos dump files where then
;#      backed up via Veritas NetBackup as a secondary copy
;# 
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


$DEBUG                           = 0;                          ;#  If true (not = 0) then debugging is enabled
$DUMPDIR                         = "/AFS_backups_in_Cloud/DailyDump";
$DBNAME                          = "AFS_Backup_DataBase";
@EXCLUDE_FILE_SERVERS            = ();
@EXPLICIT_FILE_SERVERS           = ();
@EXCLUDE_VOLUMES                 = ();

$FORCE_full_dump_filename        = "";                         ;#  Path to file that contains volumes that we want to force a full dump
%EXPLICIT_full_dump              = ();                         ;#  These volumes will be force to have a full dump

$KEEP_volumes_filename           = "";                         ;#  Path to file that contains volumes that we want to have their corresponding backup files removed from the IMAGE archive
%KEEP_volumes                    = ();                         ;#  These volumes will not have any of their backup files removed from the IMAGE archive


;#
;#  Any AFS volume that starts with this prefix is known to be a restored volume so as a default we
;#  do not back them up again.  This is the agreed upon prefix as used by the RestoreAFSbackup.pl.
;#  NOTE that in this program we add \\ before the dot so we can do the matches correctly
;#
$Restore_Volume_PREFIX   = "R\\.";




;#
;#    TMM Feb 12th 2008    Any AFS volume that starts with this prefix  "N."  just skip
;#
$Skip_Volume_PREFIX   = "N\\.";




;#
;#  If true, will create sub directories under the dump directory based on volume names
;#  This is recommended if you have large number ( greater than 500) of volumes within
;#  your cell.  Otherwise a single directory with all the dump image files will not scale.
;#
$CREATE_SUBDIRS = 1;


$ONE_DAY = 86400;                                      ;# Number of seconds in 24 hours

$MAX_DAYS = 120;                                       ;# Full dump has to happen every 120 days     NOTE:  value change from 140 --> 120 on March 24th 2008

$MAX_DAYS_BETWEEN_FULLS  =  ($MAX_DAYS * $ONE_DAY);


$SLA_DATA_RETENTION  =  (92  * $ONE_DAY);              ;# Defines the data retention window of 13 weeks



;#
;#  The DUMP_STYLE defines if the incremental dumps after the full are "Accumulative" or "Differential"
;#
;#  With the "Accumulative" style it uses more space on disk as well as tape but you only need at most
;#  three (full, major and minor) vos dump image files to perform a restore.
;#
;#  The "Differential" style uses much less space on disk and tape.  The draw back is you need everyone
;#  of those vos dump image files to perform a restore.
;#
;#
;#        $DUMP_STYLE  =  "Accumulative";
;#        $DUMP_STYLE  =  "Differential";
;#
;#
;#  I think the "Accumulative" style is useful in environments without a tape library.  In these environements
;#  tape handling would become to clumbersome to perform restores.
;#
;#  If you have a large tape library and an enterprise class backup software this becomes a non issue, therefore
;#  the "Differential" style would be better choice.  Because it uses less disk space within the on-line image
;#  archive.  This also translates into less space being required on tape.
;#
;#
$DUMP_STYLE  =  "Differential";



;#
;#  The variables MINOR_MAX & MAJOR_MAX control the number of incremental dumps that can be created.
;#
;#    The "backup_level" of volume dump is based on the dump date of the full dump which is identified as
;#    backup_level "0-0".  Hence the next backup of the volume would be an incremental against the full "0-1".
;#    We can perform up to 9 (MINOR_MAX) incremental dumps against the full "0-2" "0-3"   ... "0-9".
;#
;#    The tenth incremental dump against the full is recorded as "1-0".  The next dump will be an incremental
;#    dump against "1-0" and be recorded as "1-1".  Hence we can have up to 39 incremental dumps before we
;#    need to do another full.
;#
;#    NOTE:
;#          You could increase the number of incremental by setting MAJOR_MAX to a larger value.  But you
;#          will also need to increase the value of MAX_DAYS_BETWEEN_FULLS as well.  Also your backup system
;#          (Veritas NetBackup) may need to have its data retention policy on tape increased as well.
;#
;#          Also the "backup_level" is the last component of the image file name and we have coded logic that
;#          expects the  MINOR_MAX & MAJOR_MAX values to be single digit values (see below)
;#
;#                user.terry.mail__2006_11_26_0810__0-0
;#                user.terry.mail__2006_11_27_0820__0-1
;#                user.terry.mail__2006_11_28_0812__0-2
;#                user.terry.mail__2006_12_30_0815__0-3
;#                user.terry.mail__2006_12_01_0813__0-4
;#                user.terry.mail__2006_12_02_0846__0-5
;#                user.terry.mail__2006_12_04_0849__0-6
;#                user.terry.mail__2006_12_05_0851__0-7
;#                user.terry.mail__2006_12_06_0854__0-8
;#                user.terry.mail__2006_12_07_0856__0-9
;#                user.terry.mail__2006_12_08_0800__1-0
;#                user.terry.mail__2006_12_09_0803__1-1
;#                user.terry.mail__2006_12_10_0841__1-2
;#                user.terry.mail__2006_12_11_0815__1-3
;#                user.terry.mail__2006_12_12_0813__1-4
;#                user.terry.mail__2006_12_13_0838__1-5
;#
;#          Also the date stamp embedded within the image file name is the date when the dump process was run.  It
;#          is not the modification time of the last update to the volume.
;#
;#
$MINOR_MAX = 9;
$MAJOR_MAX = 3;




;#
;#  Defines the number of days to wait before we delete dump images for AFS volumes that
;#  no longer exist within the AFS cell.
;#
$DELAY_DELETE = ( 30 * $ONE_DAY);


;#
;#  Add a 2nd stage for the DELAY_DELETE where a warning message is issued for each volume that will have backup files deleted from the IMAGE archive
;#  In this case add 10 days of warning before removing backup files from the IMAGE archive
;#
$DELAY_DELETE_2nd_STAGE = ( $DELAY_DELETE + ( 10 * $ONE_DAY));





;#
;#  Defines the size that the cummulation of the incrementals must be in order to trigger a forced dump at a new backup level (0-0, 1-0, 2-0, 3-0)
;#
;#    The trigger for the INCREMENTAL_MAX_SIZE only happens when the size of the incrementals over 65% of the volumes (full dump size).
;#    when triggered it will cause the volume's next dump to be a full dump
;#
;#
$INCREMENTAL_MIN_SIZE = (1024 * 1024);     ;#  [ 1GB ]    Value times the number of MB (1024 KB)

$INCREMENTAL_MAX_SIZE = (30720 * 1024);    ;#  [ 30GB ]   Value times the number of MB (1024 KB)


;#
;#  Defines the number of five second time outs [ calls to sleep() ] to be run until we find all the vos dumper clients ready to go
;#
$FIVE_SECOND_LOOPS = 60;     ;#  shared value between this program and vosdumpers.pl

;#
;#  Defines the number of seconds to sleep when we find no vos dump output files that are ready to be processed
;#
$WAIT_ON_VOS_DUMPS = 60;

;#
;#  Defines the number of seconds to sleep when we looked through the dump directory and standard out was zero length (should not be!)
;#  Hence let use wait a bit and see if the output does get written to the standard output file
;#
$WAIT_ON_STDOUT = 5;


@File_Servers = ();           ;# File servers to dump volumes from
%RW_volumes = ();             ;# Information about the current (RW) read/write volumes
%BK_volumes = ();             ;# Information about the current (BK) backup volumes
%DB_recent = ();              ;# Info from the backup data base, about the most recent backup for the associated volume
%DB_full = ();                ;# Info from the backup data base, about the most recent FULL backup for the associated volume
%DB_oldest_full = ();         ;# Info from the backup data base, about the oldest FULL backup for the associated volume
%DUMP_volumes = ();           ;# Save a record for each of the AFS volumes that will be dumped
%DUMP_commands = ();          ;# The vos dump command for each volume to be dump
%DUMP_no_try =();             ;# Save a record for the volumes where the vos dump command was not tried
%DUMP_done = ();              ;# As each dump completes get the name of the volume and the name of the dump file created
%DUMP_success = ();           ;# Save a record for each volume that was successfully dumped
%DUMP_failed = ();            ;# Save a record for each volume that failed to be dumped successfully
%DUMP_needs_salvage = ();     ;# Track the volumes that need to be salvaged according to the output from the META file creation
%BUSY_retry = ();             ;# Save the volumes that could not be dumped the first time and retry at the end
%NEW_volumes = ();            ;# When we dump a new volume that has not been dumped save its name here
@OLD_Images = ();             ;# Saves information about old backup images to purge


@QUEUE_1_cmds = ();
@QUEUE_2_cmds = ();
@QUEUE_3_cmds = ();
@QUEUE_4_cmds = ();
@QUEUE_5_cmds = ();
@QUEUE_6_cmds = ();

$FLAG_queue_1 = 0;
$FLAG_queue_2 = 0;
$FLAG_queue_3 = 0;
$FLAG_queue_4 = 0;
$FLAG_queue_5 = 0;
$FLAG_queue_6 = 0;


$CNT_failed_archive = 0;
$CNT_failed_dumps = 0;
$CNT_no_dumps = 0;

$CNT_images = 0;
$CNT_full_images = 0;
$CNT_sizeof_full_images = 0;
$CNT_sizeof_images = 0;

$DBH                     = "";
$TH_Images               = "";
$TH_DumpDates            = "";
$TH_VolumeNames          = "";
$TH_History              = "";


$SQL_insert_Images = "INSERT INTO Images (volume, volume_id, create_time, mod_time, image_size, backup_level, dump_date, parent_dump_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

$SQL_insert_DumpDates = "INSERT INTO DumpDates (dump_date, number_of_images, number_of_fulls, size_of_fulls, total_size) VALUES (?, ?, ?, ?, ?)";

$SQL_insert_VolumeNames = "INSERT INTO VolumeNames (volume, last_query_date, increment_status) VALUES (?, ?, ?)";

$SQL_update_VolumeNames_last_query_date = "UPDATE VolumeNames SET last_query_date = ? WHERE volume = ?";

$SQL_update_VolumeNames_status = "UPDATE VolumeNames SET increment_status = ? WHERE volume = ?";

$SQL_insert_History = "INSERT INTO History (volume, volume_id, create_time, mod_time, image_size, backup_level, dump_date, parent_dump_date, delete_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";



;#
;#   OK if we are running this from a cronjob then there could be a problem in that for historical reason unknown
;#   the cron daemon has hard coded a fairly short and terse path specification for the $PATH environment variable.
;#
;#   The code below attempts to determine that and set the $PATH variable to root's default path (For RHEL 5.3) 
;#
if ( $ENV{'PATH'}  eq  "/usr/bin:/bin" ) {
    ;#  Force the script to use root's default $PATH variable  (RHEL 5.3)
    $ENV{'PATH'} = "/usr/bin:/bin:/usr/sbin:/sbin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin";
}
elsif ( $ENV{'PATH'}  eq  "/bin:/usr/bin" ) {
    ;#  Force the script to use root's default $PATH variable  (RHEL 5.3)
    $ENV{'PATH'} = "/bin:/usr/bin:/usr/sbin:/sbin:/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin";
}



;#
;#   Find where unix shell commands are located use the /bin/which command to walk the user's $PATH
;#
if (-e "/usr/bin/which") { $WHICH = "/usr/bin/which"; } elsif (-e "/bin/which") { $WHICH = "/bin/which"; } else { die "Unable to find the which command\n\n"; }

;#
;#   External standard Unix programs
;#
$token = "cat";       $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $CAT         = $path; }
$token = "cp";        $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $COPY        = "$path -p"; }
$token = "cut";       $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $CUT         = $path; }

$token = "echo";      $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $ECHO        = $path; }
$token = "egrep";     $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $EGREP       = $path; }
$token = "grep";      $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $GREP        = $path; }

$token = "head";      $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $HEAD        = $path; }
$token = "hostname";  $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $HOSTNAME    = $path; }
$token = "id";        $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $ID          = $path; }
$token = "ln";        $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $LN          = $path;       $SYMLINK  = "$LN -s"; }
$token = "ls";        $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $LISTDIR     = $path; }
$token = "mkdir";     $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $MAKEDIR     = "$path -p"; }
$token = "mv";        $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $MOVE        = $path; }
$token = "ps";        $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $PS          = $path; }
$token = "rm";        $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $REMOVE      = "$path -f"; }
$token = "rmdir";     $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $RMDIR       = $path; }
$token = "sort";      $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $SORT        = $path; }
$token = "tail";      $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $TAIL        = $path; }
$token = "touch";     $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $TOUCH       = $path; }
$token = "tokens";    $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $TOKENS      = $path; }
$token = "tr";        $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $TR          = $path; }


;#
;#   AFS specific commands
;#
$token = "vos";       $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $VOS         = $path; }
$token = "fs";        $path = `$WHICH $token 2>&1`;    if ($?) { die "Unable to find the $token command\n\n"; } else { $path =~ s/\n//;   $FS          = $path; }

$VOS_listvol       = sprintf ("%s listvol -long -format -server", $VOS);
$VOS_backup        = sprintf ("%s backup -verbose -id", $VOS);
$VOS_examine       = sprintf ("%s examine -format -id", $VOS);
$VOS_dump          = sprintf ("%s dump -id", $VOS);


if ( -e "/usr/sbin/afsdump_extract" ) {
    $LIST_VOS_dump = "/usr/sbin/afsdump_extract -n -p -v";
}
else {
    die "Unable to find the  afsdump_extract  program\n\n";
}


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

;#
;#  Get the dump date
;#
$This_Dump_Date = time;
$tstamp = $This_Dump_Date;


getopts('hdD:E:F:V:f:K:', \%opts) || FatalError("Please Use -h for help");

ProcessCommandLine();

if ($DEBUG) {
    $DUMPDIR = "/dumpafs/TEST";
}


;#  Define the file names for standard Out and standard Error used to capture output from external commands like /usr/afsws/etc/vos
$STDout = sprintf ("/tmp/%s__%s__stdout", $ProgName, $tstamp);
$STDerr = sprintf ("/tmp/%s__%s__stderr", $ProgName, $tstamp);

$SCRATCH_output = sprintf ("/tmp/%s__%s__scratch", $ProgName, $tstamp);

$DB_name = sprintf ("%s/DB/%s", $DUMPDIR, $DBNAME);
$ARCHDIR = sprintf ("%s/IMAGES", $DUMPDIR);
$ERRORDIR = sprintf ("%s/ERRORS", $DUMPDIR);
$METADIR = sprintf ("%s/METAFILE_ARCHIVE", $DUMPDIR);
$LOGFILE = sprintf ("%s/LOG/afsdump_log_%s", $DUMPDIR, GetTimeStamp());

unless (open(LOG, ">$LOGFILE")) {
    printf ("\nERROR:  Can not create log file (%s),  $!\n\n", $LOGFILE);
    exit 1;
}

;#
;# Check if this program is still running from a previous invocation
;#
if ((-e  "$DUMPDIR/CMD_queue_1")  ||  (-e  "$DUMPDIR/CMD_queue_2")  ||  (-e  "$DUMPDIR/CMD_queue_3")  ||
    (-e  "$DUMPDIR/CMD_queue_4")  ||  (-e  "$DUMPDIR/CMD_queue_5")  ||  (-e  "$DUMPDIR/CMD_queue_6")) {
    FatalError("The vosdumpers programs from the previous day appear to still be running");
}

;#
;#  Save existing database
;#
if (-e $DB_name) {
    $tmpName = sprintf("%s_%s", $DB_name, GetTimeStamp());
    unlink ($STDout, $STDerr);
    `$COPY $DB_name $tmpName >$STDout 2>$STDerr`;
    if ($?) {
	;#
	;#  Unable to save a copy of the database
	;#
	FatalError("Unable save a copy of the Backup data base");
    }
    $DBH = DBI->connect("DBI:SQLite:dbname=$DB_name", "", "", { PrintError => 1, RaiseError => 1 });
}
else {
    ;#
    ;#  No database exist must be first time
    ;#
    $DBH = DBI->connect("DBI:SQLite:dbname=$DB_name", "", "", { PrintError => 1, RaiseError => 1 });
    CreateDataBase();
}



;### TMM   -----   START   -----




;### TMM   -----    END    -----


@File_Servers = GetFileServers();

Get_RW_VolumeInfo();

BuildDumpList();

ListVolumes2Dump();

CreateDumpCommands();


;#
;#  TODO:
;#        Currently the vos dump clients (vosdumper.pl) are run on other machines from a cron entry
;#        on each machine.  At some point it would be nice to start these vos dump clients up from
;#        this program via ssh  
;#
;#             Start_VosDump_Clients();
;#

WriteDumpCommands2Queue();

MonitorQueue();

Update_VolumeNames_last_query_date();

RemoveImagesOfPurgedAFSvolumes();

RemoveOlderImages();

;#
;#  Remove the files used to synchronize with the vos dump clients
;#
unlink ("$DUMPDIR/CMD_queue_1");
unlink ("$DUMPDIR/CMD_queue_2");
unlink ("$DUMPDIR/CMD_queue_3");
unlink ("$DUMPDIR/CMD_queue_4");
unlink ("$DUMPDIR/CMD_queue_5");
unlink ("$DUMPDIR/CMD_queue_6");

unlink ("$DUMPDIR/READY_queue_1");
unlink ("$DUMPDIR/READY_queue_2");
unlink ("$DUMPDIR/READY_queue_3");
unlink ("$DUMPDIR/READY_queue_4");
unlink ("$DUMPDIR/READY_queue_5");
unlink ("$DUMPDIR/READY_queue_6");

unlink ("$DUMPDIR/ALIVE_queue_1");
unlink ("$DUMPDIR/ALIVE_queue_2");
unlink ("$DUMPDIR/ALIVE_queue_3");
unlink ("$DUMPDIR/ALIVE_queue_4");
unlink ("$DUMPDIR/ALIVE_queue_5");
unlink ("$DUMPDIR/ALIVE_queue_6");

unlink ("$DUMPDIR/FINISH_queue_1");
unlink ("$DUMPDIR/FINISH_queue_2");
unlink ("$DUMPDIR/FINISH_queue_3");
unlink ("$DUMPDIR/FINISH_queue_4");
unlink ("$DUMPDIR/FINISH_queue_5");
unlink ("$DUMPDIR/FINISH_queue_6");


if ($TH_Images) {   $TH_Images->finish();   }
if ($TH_DumpDates) {   $TH_DumpDates->finish();   }
if ($TH_VolumeNames) {   $TH_VolumeNames->finish();   }
if ($TH_History) {   $TH_History->finish();   }

$DBH->disconnect;





    ;#
    ;#  If any volumes where to be explicitly be FULL dumps make sure they were dumped and update the input file $FORCE_full_dump_filename
    ;#
    @volume_names = ();
    foreach $volume (sort(keys %EXPLICIT_full_dump)) {
	push (@volume_names, $volume);
    }

    if ($#volume_names > -1) {
	;#
	;#  Rename the input file  $FORCE_full_dump_filename  place todays timestamp on the end of the file
	;#
        $new_filename = sprintf ("%s_%s", $FORCE_full_dump_filename, GetTimeStamp());
	`$MOVE $FORCE_full_dump_filename $new_filename >$STDout 2>$STDerr`;
	if ($?) {
	    ;#
	    ;#  Unable to rename the $FORCE_full_dump_filename
	    ;#
	    FatalError("Unable to rename $FORCE_full_dump_filename");
	}

	unless (open(FORCE_LOG, ">$FORCE_full_dump_filename")) {
	    FatalError("Can not create file $FORCE_full_dump_filename ,  $!");
	}

	for ($i = 0;    $i <= $#volume_names;    $i++) {
	    ;#
	    ;#  Any volume that was not dumped (full) recreate a new input file and try the explicit dump tomorrow
	    ;#
	    $volume = $volume_names[$i];
	    if ($EXPLICIT_full_dump{$volume}  !=  3) {
                printf (FORCE_LOG  "%s\n", $volume);
	    }
	}
        close(FORCE_LOG);
    }


DisplayAndLogIt("The dump has completed");




;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   WriteDumpCommands2Queue
;#
;#   Purpose:      To write the dump commands to the active queue's
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   WriteDumpCommands2Queue  {


    my ($i, $pop_flag, @all_commands);
    
    @all_commands = ();   $FLAG_queue_1 = 0;   $FLAG_queue_2 = 0;   $FLAG_queue_3 = 0;   $FLAG_queue_4 = 0;   $FLAG_queue_5 = 0;   $FLAG_queue_6 = 0;

    ;#  Do some hand shake to make sure all the vos dump servers are READY
    unlink ("$DUMPDIR/READY_queue_1");
    unlink ("$DUMPDIR/READY_queue_2");
    unlink ("$DUMPDIR/READY_queue_3");
    unlink ("$DUMPDIR/READY_queue_4");
    unlink ("$DUMPDIR/READY_queue_5");
    unlink ("$DUMPDIR/READY_queue_6");

    unlink("$SCRATCH_output");
    unless (open(SCRATCH, ">$SCRATCH_output")) {
	FatalError("Unable to create $SCRATCH_output:  $!");
    }

    printf (SCRATCH "Hello vos dumper\n");
    close(SCRATCH);

    system("/bin/cp $SCRATCH_output $DUMPDIR/CMD_queue_1"); 
    system("/bin/cp $SCRATCH_output $DUMPDIR/CMD_queue_2"); 
    system("/bin/cp $SCRATCH_output $DUMPDIR/CMD_queue_3"); 
    system("/bin/cp $SCRATCH_output $DUMPDIR/CMD_queue_4"); 
    system("/bin/cp $SCRATCH_output $DUMPDIR/CMD_queue_5"); 
    system("/bin/cp $SCRATCH_output $DUMPDIR/CMD_queue_6"); 
    unlink("$SCRATCH_output");
    sleep(10);


    for ($i = 0;    $i <= $FIVE_SECOND_LOOPS;    $i++) {
	if (-s "$DUMPDIR/READY_queue_1")     {   $FLAG_queue_1 = 1;   }
	if (-s "$DUMPDIR/READY_queue_2")     {   $FLAG_queue_2 = 1;   }
	if (-s "$DUMPDIR/READY_queue_3")     {   $FLAG_queue_3 = 1;   }
	if (-s "$DUMPDIR/READY_queue_4")     {   $FLAG_queue_4 = 1;   }
	if (-s "$DUMPDIR/READY_queue_5")     {   $FLAG_queue_5 = 1;   }
	if (-s "$DUMPDIR/READY_queue_6")     {   $FLAG_queue_6 = 1;   }

	if (($FLAG_queue_1)  &&  ($FLAG_queue_2)  &&  ($FLAG_queue_3)  &&  ($FLAG_queue_4)  &&  ($FLAG_queue_5)  &&  ($FLAG_queue_6)) {   last;   }

	sleep(5);
    }

    if (!($FLAG_queue_1)  &&  !($FLAG_queue_2)  &&  !($FLAG_queue_3)  &&  !($FLAG_queue_4)  &&  !($FLAG_queue_5)  &&  !($FLAG_queue_6)) {
	FatalError("Unable to begin vos dumps  -  No queues appear to be ready");
    }

    foreach $volume (sort(keys %DUMP_commands)) {
        push (@all_commands, $DUMP_commands{$volume});
    }

    ;#
    ;#  Distribute the vos dump commands across the available queue's, make an attempt to shuffle the vos dump commands within the queues
    ;#
    $pop_flag = 1;

    while ($#all_commands != -1) {

	if ($pop_flag  ==  1) {
	    if (($FLAG_queue_1)  &&  ($#all_commands != -1))  {   push(@QUEUE_1_cmds, pop(@all_commands));      }
	    if (($FLAG_queue_2)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_2_cmds, pop(@all_commands));   }
	    if (($FLAG_queue_3)  &&  ($#all_commands != -1))  {   push(@QUEUE_3_cmds, pop(@all_commands));      }
	    if (($FLAG_queue_4)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_4_cmds, pop(@all_commands));   }
	    if (($FLAG_queue_5)  &&  ($#all_commands != -1))  {   push(@QUEUE_5_cmds, pop(@all_commands));      }
	    if (($FLAG_queue_6)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_6_cmds, pop(@all_commands));   }
	    $pop_flag++;
        }
	elsif ($pop_flag  ==  2) {
	    if (($FLAG_queue_1)  &&  ($#all_commands != -1))  {   push(@QUEUE_1_cmds, shift(@all_commands));      }
	    if (($FLAG_queue_2)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_2_cmds, shift(@all_commands));   }
	    if (($FLAG_queue_3)  &&  ($#all_commands != -1))  {   push(@QUEUE_3_cmds, shift(@all_commands));      }
	    if (($FLAG_queue_4)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_4_cmds, shift(@all_commands));   }
	    if (($FLAG_queue_5)  &&  ($#all_commands != -1))  {   push(@QUEUE_5_cmds, shift(@all_commands));      }
	    if (($FLAG_queue_6)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_6_cmds, shift(@all_commands));   }
	    $pop_flag++;
        }
	elsif ($pop_flag  ==  3) {
	    if (($FLAG_queue_1)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_1_cmds, shift(@all_commands));   }
	    if (($FLAG_queue_2)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_2_cmds, pop(@all_commands));     }
	    if (($FLAG_queue_3)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_3_cmds, pop(@all_commands));     }
	    if (($FLAG_queue_4)  &&  ($#all_commands != -1))  {   push(@QUEUE_4_cmds, shift(@all_commands));      }
	    if (($FLAG_queue_5)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_5_cmds, pop(@all_commands));     }
	    if (($FLAG_queue_6)  &&  ($#all_commands != -1))  {   push(@QUEUE_6_cmds, shift(@all_commands));      }
	    $pop_flag++;
        }
	elsif ($pop_flag  ==  4) {
	    if (($FLAG_queue_1)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_1_cmds, shift(@all_commands));   }
	    if (($FLAG_queue_2)  &&  ($#all_commands != -1))  {   push(@QUEUE_2_cmds, pop(@all_commands));        }
	    if (($FLAG_queue_3)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_3_cmds, pop(@all_commands));     }
	    if (($FLAG_queue_4)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_4_cmds, shift(@all_commands));   }
	    if (($FLAG_queue_5)  &&  ($#all_commands != -1))  {   unshift(@QUEUE_5_cmds, shift(@all_commands));   }
	    if (($FLAG_queue_6)  &&  ($#all_commands != -1))  {   push(@QUEUE_6_cmds, pop(@all_commands));        }
	    $pop_flag = 1; 
        }
    }

    ;#
    ;#  Write the vos dump commands to each queue's Command Input file
    ;#
    if ($FLAG_queue_1) {
	unlink("$SCRATCH_output");
	unless (open(SCRATCH, ">$SCRATCH_output")) {
	    FatalError("Unable to create $SCRATCH_output:  $!");
	}

	for ($i = 0;    $i <= $#QUEUE_1_cmds;    $i++) {
	    printf (SCRATCH "%s\n", $QUEUE_1_cmds[$i]);
	}
	close(SCRATCH);
	unlink ("$DUMPDIR/CMD_queue_1");
	system("/bin/cp $SCRATCH_output $DUMPDIR/CMD_queue_1"); 
    }

    if ($FLAG_queue_2) {
	unlink("$SCRATCH_output");
	unless (open(SCRATCH, ">$SCRATCH_output")) {
	    FatalError("Unable to create $SCRATCH_output:  $!");
	}

	for ($i = 0;    $i <= $#QUEUE_2_cmds;    $i++) {
	    printf (SCRATCH "%s\n", $QUEUE_2_cmds[$i]);
	}
	close(SCRATCH);
	unlink ("$DUMPDIR/CMD_queue_2");
	system("/bin/cp $SCRATCH_output $DUMPDIR/CMD_queue_2"); 
    }

    if ($FLAG_queue_3) {
	unlink("$SCRATCH_output");
	unless (open(SCRATCH, ">$SCRATCH_output")) {
	    FatalError("Unable to create $SCRATCH_output:  $!");
	}

	for ($i = 0;    $i <= $#QUEUE_3_cmds;    $i++) {
	    printf (SCRATCH "%s\n", $QUEUE_3_cmds[$i]);
	}
	close(SCRATCH);
	unlink ("$DUMPDIR/CMD_queue_3");
	system("/bin/cp $SCRATCH_output $DUMPDIR/CMD_queue_3"); 
    }

    if ($FLAG_queue_4) {
	unlink("$SCRATCH_output");
	unless (open(SCRATCH, ">$SCRATCH_output")) {
	    FatalError("Unable to create $SCRATCH_output:  $!");
	}

	for ($i = 0;    $i <= $#QUEUE_4_cmds;    $i++) {
	    printf (SCRATCH "%s\n", $QUEUE_4_cmds[$i]);
	}
	close(SCRATCH);
	unlink ("$DUMPDIR/CMD_queue_4");
	system("/bin/cp $SCRATCH_output $DUMPDIR/CMD_queue_4"); 
    }

    if ($FLAG_queue_5) {
	unlink("$SCRATCH_output");
	unless (open(SCRATCH, ">$SCRATCH_output")) {
	    FatalError("Unable to create $SCRATCH_output:  $!");
	}

	for ($i = 0;    $i <= $#QUEUE_5_cmds;    $i++) {
	    printf (SCRATCH "%s\n", $QUEUE_5_cmds[$i]);
	}
	close(SCRATCH);
	unlink ("$DUMPDIR/CMD_queue_5");
	system("/bin/cp $SCRATCH_output $DUMPDIR/CMD_queue_5"); 
    }

    if ($FLAG_queue_6) {
	unlink("$SCRATCH_output");
	unless (open(SCRATCH, ">$SCRATCH_output")) {
	    FatalError("Unable to create $SCRATCH_output:  $!");
	}

	for ($i = 0;    $i <= $#QUEUE_6_cmds;    $i++) {
	    printf (SCRATCH "%s\n", $QUEUE_6_cmds[$i]);
	}
	close(SCRATCH);
	unlink ("$DUMPDIR/CMD_queue_6");
	system("/bin/cp $SCRATCH_output $DUMPDIR/CMD_queue_6"); 
    }

    unlink("$SCRATCH_output");
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   MonitorQueue
;#
;#   Purpose:      To monitor the progress of the vos dumps from the vos dump clients (vosdumpers.pl)
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   MonitorQueue  {


    my ($cmd, $volume, $line, $image_file, $busy_retry_flag, $loop_flag);
    my ($flag_finish_queue_1, $flag_finish_queue_2, $flag_finish_queue_3, $flag_finish_queue_4, $flag_finish_queue_5, $flag_finish_queue_6);

    $loop_flag = 1;   
    $busy_retry_flag = 1;

    if ($FLAG_queue_1)   {  $flag_finish_queue_1 = 0;  }   else {  $flag_finish_queue_1 = 1;  }
    if ($FLAG_queue_2)   {  $flag_finish_queue_2 = 0;  }   else {  $flag_finish_queue_2 = 1;  }
    if ($FLAG_queue_3)   {  $flag_finish_queue_3 = 0;  }   else {  $flag_finish_queue_3 = 1;  }
    if ($FLAG_queue_4)   {  $flag_finish_queue_4 = 0;  }   else {  $flag_finish_queue_4 = 1;  }
    if ($FLAG_queue_5)   {  $flag_finish_queue_5 = 0;  }   else {  $flag_finish_queue_5 = 1;  }
    if ($FLAG_queue_6)   {  $flag_finish_queue_6 = 0;  }   else {  $flag_finish_queue_6 = 1;  }


    while ($loop_flag) {

	if (($flag_finish_queue_1)  &&  ($flag_finish_queue_2)  &&  ($flag_finish_queue_3)  &&
            ($flag_finish_queue_4)  &&  ($flag_finish_queue_5)  &&  ($flag_finish_queue_6)) {
	    ;#
	    ;#   The vos dump clients have inidcated that they are done.
	    ;#   Clear the "loop_flag" to terminate the while loop and then process the dump directory for the last time
	    ;#
	    $loop_flag = 0;
	    if ($busy_retry_flag) {
		DisplayAndLogIt("The vos dump clients have inidcated they are done");
	    }
	}

	;#
	;#  Look for any "done" files within the dump directory
	;#
	unlink ($STDout, $STDerr);
	$cmd = sprintf ("%s %s | %s \"__[0-9][0-9][0-9][0-9]_[0-9][0-9]_[0-9][0-9]_[0-9][0-9][0-9][0-9]__[0-9]-[0-9].done\"",
                          $LISTDIR, $DUMPDIR, $GREP);
	`($cmd) >$STDout 2>$STDerr`;	    

	if ($?) {
	    ;#
	    ;#  Either no vos dumps are complete at this time (standard error zero length)
	    ;#
	    ;#                         OR
	    ;#
	    ;#  We have an error condition (standard error NOT zero length)
	    ;#
	    ;#
	    if (!(-z $STDerr)) {
		DisplayErrorAndLogIt("ERROR:  unable to list directory $DUMPDIR");
	    }
	}
	else {
	    if (-z $STDout) {    sleep($WAIT_ON_STDOUT);    }

	    if (open(INPUT, $STDout)) {
		;#
		;#  Walk through the file that contains the list of "done" files.
		;#  These files indicate that a vos dump command has been completed
		;#
		while ($line = <INPUT>) {
		    chomp($line);
		    $volume = $line;
		    $volume =~ s/__[0-9][0-9][0-9][0-9]_[0-9][0-9]_[0-9][0-9]_[0-9][0-9][0-9][0-9]__[0-9]-[0-9].done//g;
		    $image_file = $line;
		    $image_file =~ s/\.done$//g;
		    $DUMP_done{$volume} = $image_file;
		}
		close(INPUT);
	    }
	    else {
		unlink ($STDout, $STDerr);
		DisplayErrorAndLogIt("ERROR:  Unable to read $STDout:  $!");
	    }
	}

        VerifyDumpedVolumes();
	ArchiveDumpedVolumes();

	;#
	;#  Check to see if all the queues (the vos dump clients) are finished, set the finish flags accordingly
	;#
	if (-s "$DUMPDIR/FINISH_queue_1")     {   $flag_finish_queue_1 = 1;   }
	if (-s "$DUMPDIR/FINISH_queue_2")     {   $flag_finish_queue_2 = 1;   }
	if (-s "$DUMPDIR/FINISH_queue_3")     {   $flag_finish_queue_3 = 1;   }
	if (-s "$DUMPDIR/FINISH_queue_4")     {   $flag_finish_queue_4 = 1;   }
	if (-s "$DUMPDIR/FINISH_queue_5")     {   $flag_finish_queue_5 = 1;   }
	if (-s "$DUMPDIR/FINISH_queue_6")     {   $flag_finish_queue_6 = 1;   }


	if (($flag_finish_queue_1)  &&  ($flag_finish_queue_2)  &&  ($flag_finish_queue_3)  &&
	    ($flag_finish_queue_4)  &&  ($flag_finish_queue_5)  &&  ($flag_finish_queue_6)  &&  (!($loop_flag))) {
	    ;#
	    ;#      The vos dump clients have inidcated they are done and the dump directory had been processed for the last time
	    ;#
	    ;#   EXCEPT
	    ;#
	    ;#      If we find any volumes that were BUSY and could not be dumped.  At this point for each of these busy volumes
	    ;#      we will run the vos dump command and retry to dump the volume.  Therefore we will set the "loop_flag" and
	    ;#      cause the dump directory to be processed one more time
	    ;#
	    if ($busy_retry_flag) {
		foreach $volume (sort(keys %BUSY_retry)) {
		    DisplayAndLogIt("Retry dumping the volume $volume");
		    `$DUMP_commands{$volume}`;
		    $loop_flag = 1;
		    $busy_retry_flag = 0;
		}
	    }
	}
    }


    ;#
    ;#  Prepare to add a record to the "DumpDates" table in the backup database
    ;#
    if ($CNT_images > 0) {
	if ($TH_DumpDates)      {   $TH_DumpDates->finish();     $TH_DumpDates = "";     }
        $TH_DumpDates = $DBH->prepare($SQL_insert_DumpDates);
        $TH_DumpDates->execute($This_Dump_Date, $CNT_images, $CNT_full_images, $CNT_sizeof_full_images, $CNT_sizeof_images);
        $TH_DumpDates->finish();   $TH_DumpDates = "";
        $CNT_sizeof_images = sprintf("%d", ($CNT_sizeof_images / 1024));
        DisplayAndLogIt("Moved $CNT_images volume backup image files with a size of $CNT_sizeof_images MB to the archive");
    }
    else {
        DisplayAndLogIt("There were no backup image files to move to the archive");
    }


    if ($CNT_failed_archive != 0) {
        DisplayAndLogIt("Unable to move $CNT_failed_archive volume backup image files to the archive");
    }

    if ($CNT_failed_dumps != 0) {
        DisplayAndLogIt("Unable to dump $CNT_failed_dumps volumes");
    }

    if ($CNT_no_dumps != 0) {
        DisplayAndLogIt("No image files for $CNT_no_dumps volumes");
    }
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   VerifyDumpedVolumes
;#
;#   Purpose:      To verify the dump images that were produced had no errors during the dump.
;#                 For volumes that were successfully dumped the array %DUMP_success will use
;#                 the volume name as the key and save the file name of the dump image.  The
;#                 array %DUMP_failed will recorded similar information for volumes that had
;#                 errors during the vos dump.
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   VerifyDumpedVolumes  {


    my ($i, @done_volumes, $success_flag, $busy_flag);
    my ($volume, $line, $image_file, $vos_stderr, $meta_file, $meta_stderr);

    @done_volumes = ();
    foreach $volume (sort(keys %DUMP_done)) {
	push (@done_volumes, $volume);
    }

    unlink ($STDout, $STDerr);

    for ($i = 0;    $i <= $#done_volumes;    $i++) {
	$volume = $done_volumes[$i];
	$image_file = $DUMP_done{$volume};
	delete $DUMP_done{$volume};

        ;#
        ;#  If dump was successful then the vos dump commands standard error file should have
        ;#  a text line indicating success:
        ;#
        ;#      Dumped volume user.terry.backup in file /dumpafs/user.terry__2006_11_30_1210__0-0
        ;#
        ;#  Yeah very messy, why does standard error have anything if the command was successful
        ;#
        $vos_stderr = sprintf("%s/%s.stderr", $DUMPDIR, $image_file);

	$busy_flag = 0;
	$success_flag = 0;
        if (open(INPUT, $vos_stderr)) {
            ;#
            ;#  Walk through the file looking for the success string
            ;#
            while ($line = <INPUT>) {
                chomp($line);
                if ($line =~ m/^Dumped volume/i) {
                    if ($line =~ m/$image_file/) {    $success_flag = 1;    }
                }
		elsif ($line =~ m/^VOLSER: volume is busy/i) {    $busy_flag = 1;    }
            }
            close(INPUT);

	    if ($success_flag) {
		$DUMP_success{$volume} = $image_file;
	    }

            if (!(exists $DUMP_success{$volume})) {
		if ($busy_flag) {
		    unlink ($STDout, $STDerr);
		    if (!(exists $BUSY_retry{$volume})) {
			$BUSY_retry{$volume} = $image_file;
			;#
			;#  Volume was busy so we will retry the dump later
			;#
			DisplayAndLogIt("WARN:   for volume $volume  -  the volume is busy retry the vos dump later");
		    }
		    else {
			;#
			;#  We have already retried dumping this volume once already, now force an error condition
			;#
			DisplayErrorAndLogIt("ERROR:   for volume $volume  -  vos dump failed volume was still busy");
			$DUMP_failed{$volume} = $image_file;
		    }
		}
		else {
		    ;#
		    ;#  Didn't find the "success" string or the "busy" string,
		    ;#  Within the Error message display the contents of the vos dump commands standard error file
		    ;#
		    `$COPY $vos_stderr $STDerr`;

		    ;### TMM    The copy of the error file from the vos dump command to the STANDARD ERROR file appears to be null
		    ;### TMM    Made changes to the "ArchiveDumpedVolumes" function to save the files for debugging

		    DisplayErrorAndLogIt("ERROR:   for volume $volume  -  vos dump failed");
		    $DUMP_failed{$volume} = $image_file;
		}
            }
	    else {
		;#
		;#  The vos dump was successful, Now see if the meta information about the vos dump image was created successfully
		;#  If it was NOT then delete entry from the success array (DUMP_success) add entry to the failed array
		;#
		$meta_file = sprintf("%s/%s.meta", $DUMPDIR, $image_file);
		$meta_stderr = sprintf("%s/%s.meta_error", $DUMPDIR, $image_file);
		if (!(-z $meta_stderr)) {
		    if (-z $meta_file) {
			`$COPY $meta_stderr $STDerr`;
			;#   DisplayErrorAndLogIt("ERROR:   for volume $volume  -  needs to be salvaged, unable to create the META file for the vos dump image");
			;#   $DUMP_failed{$volume} = $image_file;
			;#   delete $DUMP_success{$volume};

			DisplayErrorAndLogIt("WARN:   for volume $volume  -  needs to be salvaged, unable to create the META file for the vos dump image");
			$DUMP_needs_salvage{$volume} = $image_file;
		    }
		    else {
			`$COPY $meta_stderr $STDerr`;
			DisplayErrorAndLogIt("WARN:   for volume $volume  -  needs to be salvaged, problems discovered while creating the META file for the vos dump image");
			$DUMP_needs_salvage{$volume} = $image_file;
		    }
		}
	    }
        }
        else {
            ;#
            ;#  Unable to read the standard error file from the vos dump command
            ;#
            DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to read the vos dump image file:  $!");
            $DUMP_failed{$volume} = $image_file;
        }
    }
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   ArchiveDumpedVolumes
;#
;#   Purpose:      To archive the successful dump images that were produced.  Also update the
;#                 backup database with information about these new dumps
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   ArchiveDumpedVolumes  {


    my ($cmd, $image_file, $archive_path, $archive_image_file);
    my ($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size, $backup_level, $dump_date, $parent_dump_date);
    my ($dev, $inode, $mode, $nlink, $uid, $gid, $rdev, $size, $atime, $mtime, $ctime, $blksize, $blocks);
    my (@row, $last_query_date, $increment_status, $new_status);
    my ($file_name, @success_volumes, @failed_volumes, $i, $cloud_msg);


    ;#  Prepare to add a record to the backup database for each dumped image
    ;#
    if ($TH_Images)         {   $TH_Images->finish();        $TH_Images = "";        }
    if ($TH_VolumeNames)    {   $TH_VolumeNames->finish();   $TH_VolumeNames = "";   }

    @success_volumes = ();
    foreach $volume (sort(keys %DUMP_success)) {
	push (@success_volumes, $volume);
    }

    if ($#success_volumes  >=  0) {
	$i = $#success_volumes + 1;
	DisplayAndLogIt("Moving $i backup image files to the archive");
    }
    else {
	;#
	;#  Currently there are not any vos dumps that are "done", so lets sleep a bit
	;#
	DisplayIt("debug message:  Nothing to process going to sleep");
	sleep($WAIT_ON_VOS_DUMPS);
    }


    unlink ($STDout, $STDerr);

    for ($i = 0;    $i <= $#success_volumes;    $i++) {
	$volume = $success_volumes[$i];
	$image_file = $DUMP_success{$volume};

	;#
	;#  Move the META file (if it exist) for the vos dump image to the archive and update the backup database.
	;#  Also update the counters for this dump (number of images, number of Fulls,  ...etc)
	;#
	($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size,
	$backup_level, $dump_date, $parent_dump_date) = split(/ /, $DUMP_volumes{$volume});

	if ($CREATE_SUBDIRS) {
	    $archive_path = GetSubDirectoryPath($volume);
	    $archive_path = sprintf("%s/%s", $ARCHDIR, $archive_path);
	    $archive_image_file = sprintf("%s/%s", $archive_path, $image_file);
	}
	else {
	    $archive_image_file = sprintf("%s/%s", $ARCHDIR, $image_file);
	}

	;#
	;#  Go stat the vos dump image file within the IMAGE archive,
	;#  If the returned vaules for dev, inode and bock size are zero it an error condition
	;#
	($dev, $inode, $mode, $nlink, $uid, $gid, $rdev, $size, $atime, $mtime, $ctime, $blksize, $blocks) = stat($archive_image_file);
        if (($dev == 0)  &&  ($inode == 0)  &&  ($blksize == 0)) {
	    ;#
	    ;#  Unable to stat the vos dump image to the IMAGE archive directory
	    ;#
	    $CNT_failed_archive++;
	    DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to stat the vos dump image file within the IMAGE archive");
	    $DUMP_failed{$volume} = $image_file;
	}
	else {
	    ;#
	    ;# recalculate the size of the volume (dumped image file) in terms of 1KB blocks
	    ;#
	    $volume_size = sprintf("%d", $size / 1024);

	    ;#
	    ;#  copy the META file from the dump directory into the IMAGE archive, note because of corrupted AFS volumes
	    ;#  we are not able to create a META file, in these cases the META file is zero length
	    ;#
	    unlink ($STDout, $STDerr);
	    $cmd = sprintf("%s %s/%s.meta %s.meta", $COPY, $DUMPDIR, $image_file, $archive_image_file);
	    `$cmd >$STDout 2>$STDerr`;
	    if ($?) {
		;#
		;#  Unable to copy the META file for the dump image to the archive directory
		;#
		$CNT_failed_archive++;
		DisplayAndLogIt("CMD:  $cmd");
		DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to copy the META file for the vos dump image into the IMAGE archive");
		$DUMP_failed{$volume} = $image_file;
	    }
	    else {
		LogIt("CMD:  $cmd");
		if (exists $NEW_volumes{$volume}) {
		    ;#
		    ;#  This volume has not been recorded in the backup database before.
		    ;#  add a new record for this volume to the "VolumeNames" table
		    ;#
		    $TH_VolumeNames = $DBH->prepare($SQL_insert_VolumeNames);
		    $TH_VolumeNames->execute($volume, $This_Dump_Date, "normal");
		}
		else {
		    ;#
		    ;#  Verify the volume's status and update if required
		    ;#
		    $TH_VolumeNames = $DBH->prepare("SELECT * FROM VolumeNames WHERE volume = \'$volume\'");
		    $TH_VolumeNames->execute();
		    @row = $TH_VolumeNames->fetchrow_array();
		    $volume = $row[0];
		    $last_query_date = $row[1];
		    $increment_status = $row[2];
		    $new_status = $increment_status;
		    $TH_VolumeNames->finish();    $TH_VolumeNames = "";
		    if ($increment_status  ne  "normal") {
			if ($increment_status  eq  "NEXT_level_over_25_percent")    {    $new_status = "over_25_percent";    }
			elsif ($increment_status  eq  "NEXT_level_over_45_percent") {    $new_status = "over_45_percent";    }
			elsif ($increment_status  eq  "NEXT_level_over_65_percent") {    $new_status = "over_65_percent";    }
			elsif ($increment_status  eq  "NEXT_full_over_85_percent")  {    $new_status = "over_85_percent";    }
			elsif ($increment_status  eq  "NEXT_full_over_max_size")    {    $new_status = "over_max_size";      }

			if ($increment_status  ne  $new_status) {
			    ;#
			    ;#  Update the table VolumeNames with the volumes new status
			    ;#
			    $TH_VolumeNames = $DBH->prepare($SQL_update_VolumeNames_status);
			    $TH_VolumeNames->execute($new_status, $volume);
			}
		    }
		}

                ;#  Post message that the vos dump file is ready to be place into the cloud object store
                $cloud_msg = sprintf("CloudSlice:   ADD     volume:%s:    vos dump file:%s:", $volume, $archive_image_file);
                LogIt("$cloud_msg");

		;#
		;#  add a new record for this dump to the "Image" table in the backup database
		;#
		if (!($TH_Images)) {   $TH_Images = $DBH->prepare($SQL_insert_Images);   }
		$TH_Images->execute($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size, $backup_level, $dump_date, $parent_dump_date);

		;#
		;#  Track the number of images that were dumped and the size (in 1KB) of all the dumped images 
		;#
		$CNT_images++;
		$CNT_sizeof_images += $volume_size;
		if ($backup_level  eq  "0-0") {
		    $CNT_full_images++;
		    $CNT_sizeof_full_images += $volume_size;
		}

		;#
		;#  After moving the dump image, now remove the associated files (.done  .stderr  .stdout)
		;#
		unlink ($STDout, $STDerr);
		$cmd = sprintf("%s %s/%s.done %s/%s.stderr %s/%s.stdout %s/%s.meta %s/%s.meta_error", 
			 $REMOVE, $DUMPDIR, $image_file, $DUMPDIR, $image_file, $DUMPDIR, $image_file, $DUMPDIR, $image_file, $DUMPDIR, $image_file);
		`$cmd >$STDout 2>$STDerr`;
		if ($?) {
		    ;#
		    ;#  Unable to remove the associated files (.done  .stderr  .stdout)
		    ;#
		    DisplayAndLogIt("CMD:  $cmd");
		    DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to remove the .done, .meta, .meta_error, .stderr and .stdout files");
		}
	    }
	    delete $DUMP_success{$volume};

	    ;#
	    ;#  Track the option to force a full dump.     For  $EXPLICIT_full_dump   a value of "3" indicates a FULL dump was performed
	    ;#
	    if (exists $EXPLICIT_full_dump{$volume})   {   $EXPLICIT_full_dump{$volume} = 3;   }
	}
    }


    @failed_volumes = ();
    foreach $volume (sort(keys %DUMP_failed)) {
	push (@failed_volumes, $volume);
    }

    for ($i = 0;    $i <= $#failed_volumes;    $i++) {
                ;#
	        ;#  To aid in debugging for each volume that failed to dump,
	        ;#  Move the associated files (.done   .meta   .meta_error   .stderr   .stdout)  to an error directory
                ;#
                $CNT_failed_dumps++;
	        $volume = $failed_volumes[$i];
                $image_file = $DUMP_failed{$volume};

                $file_name = sprintf("%s/%s.done", $DUMPDIR, $image_file);
                $cmd = sprintf("%s %s %s", $MOVE, $file_name, $ERRORDIR);
                unlink ($STDout, $STDerr);
                `$cmd >$STDout 2>$STDerr`;
                if ($?) {
                    DisplayAndLogIt("CMD:  $cmd");
                    DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to move  $file_name  to the error repository");
                }

                $file_name = sprintf("%s/%s.meta", $DUMPDIR, $image_file);
                $cmd = sprintf("%s %s %s", $MOVE, $file_name, $ERRORDIR);
                unlink ($STDout, $STDerr);
                `$cmd >$STDout 2>$STDerr`;
                if ($?) {
                    DisplayAndLogIt("CMD:  $cmd");
                    DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to move  $file_name  to the error repository");
                }

                $file_name = sprintf("%s/%s.meta_error", $DUMPDIR, $image_file);
                $cmd = sprintf("%s %s %s", $MOVE, $file_name, $ERRORDIR);
                unlink ($STDout, $STDerr);
                `$cmd >$STDout 2>$STDerr`;
                if ($?) {
                    DisplayAndLogIt("CMD:  $cmd");
                    DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to move  $file_name  to the error repository");
                }

                $file_name = sprintf("%s/%s.stderr", $DUMPDIR, $image_file);
                $cmd = sprintf("%s %s %s", $MOVE, $file_name, $ERRORDIR);
                unlink ($STDout, $STDerr);
                `$cmd >$STDout 2>$STDerr`;
                if ($?) {
                    DisplayAndLogIt("CMD:  $cmd");
                    DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to move  $file_name  to the error repository");
                }

                $file_name = sprintf("%s/%s.stdout", $DUMPDIR, $image_file);
                $cmd = sprintf("%s %s %s", $MOVE, $file_name, $ERRORDIR);
                unlink ($STDout, $STDerr);
                `$cmd >$STDout 2>$STDerr`;
                if ($?) {
                    DisplayAndLogIt("CMD:  $cmd");
                    DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to move  $file_name  to the error repository");
                }

		;#
		;#  Also make sure that a partial vos dump image file and/or META file is not contained within the IMAGE archive
		;#
		if ($CREATE_SUBDIRS) {
		    $archive_path = GetSubDirectoryPath($volume);
		    $archive_path = sprintf("%s/%s", $ARCHDIR, $archive_path);
		    $archive_image_file = sprintf("%s/%s", $archive_path, $image_file);
		}
		else {
		    $archive_image_file = sprintf("%s/%s", $ARCHDIR, $image_file);
		}

                $file_name = $archive_image_file;
                $cmd = sprintf("%s %s", $REMOVE, $file_name);
                unlink ($STDout, $STDerr);
                `$cmd >$STDout 2>$STDerr`;
                if ($?) {
                    DisplayAndLogIt("CMD:  $cmd");
                    DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to delete  $file_name");
                }

                $file_name = sprintf("%s.meta", $archive_image_file);
                $cmd = sprintf("%s %s", $REMOVE, $file_name);
                unlink ($STDout, $STDerr);
                `$cmd >$STDout 2>$STDerr`;
                if ($?) {
                    DisplayAndLogIt("CMD:  $cmd");
                    DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to delete  $file_name");
                }
		delete $DUMP_failed{$volume};
    }

    if ($TH_Images)         {   $TH_Images->finish();        $TH_Images = "";        }
    if ($TH_VolumeNames)    {   $TH_VolumeNames->finish();   $TH_VolumeNames = "";   }
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   RemoveOlderImages
;#
;#   Purpose:      To query the backup database and remove images that have more than one Full
;#                 dump image (level 0-0)
;#                 
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub  RemoveOlderImages  {

    my ($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size, $backup_level, $dump_date, $parent_dump_date);
    my ($older_volume_mod_time, $older_volume_size, $cloud_msg);
    my (@row, %imagelist, $oldest_full_dump_date, $recent_full_dump_date, $i, $disk_space, $purge_cnt, $archive_path, $rtn_code);
    my ($second, $minute, $hour, $monthday, $month, $year, $weekday, $yday, $isdst);
    my ($last_query_date, $increment_status, %update_VolumeNames, $new_status, $date_string);
    my ($basename, $meta_path, $archive_meta_file, $message, %full_dump_dates, $full_dump_cnt);




    ;#
    ;#  Query the Images table to find the record for the most recent backup for each AFS volume within the backup data base
    ;#
    DisplayAndLogIt("Query the Images table  -  find the oldest FULL backup image for each AFS volume");

    %DB_full = ();
    %DB_oldest_full = ();
    %update_VolumeNames = ();

    if ($TH_VolumeNames)   {   $TH_VolumeNames->finish();   $TH_VolumeNames = "";   }
    if ($TH_Images)        {   $TH_Images->finish();        $TH_Images = "";        }

    $TH_VolumeNames = $DBH->prepare("SELECT * FROM VolumeNames");
    $TH_VolumeNames->execute();

    while (@row = $TH_VolumeNames->fetchrow_array()) {
        $volume = $row[0];
        $last_query_date = $row[1];
        $increment_status = $row[2];
        %imagelist = ();

        $TH_Images = $DBH->prepare("SELECT * FROM Images WHERE volume = \'$volume\'");
        $TH_Images->execute();

        $oldest_full_dump_date = 0;
        $recent_full_dump_date = 0;
        %full_dump_dates = ();
        $full_dump_cnt = 0;

        while (@row = $TH_Images->fetchrow_array()) {
	    $volume              = $row[0];
	    $vid                 = $row[1]; 
	    $volume_create_time  = $row[2];
	    $volume_mod_time     = $row[3];
	    $volume_size         = $row[4];
	    $backup_level        = $row[5];
	    $dump_date           = $row[6];
	    $parent_dump_date    = $row[7];

	    $imagelist{$dump_date} = "$volume $vid $volume_create_time $volume_mod_time $volume_size $backup_level $dump_date $parent_dump_date";

	    ;#
	    ;#  Store the dump dates for all the FULL backup images
	    ;#
	    if ($backup_level  eq  "0-0") {
		$full_dump_cnt++;
		$full_dump_dates{$dump_date} = $imagelist{$dump_date};
		;#
		;#  Store the record for most recent FULL backup image for the AFS volume
		;#
		if ($dump_date  >  $recent_full_dump_date) {
		    $DB_full{$volume} = $imagelist{$dump_date};
		    $recent_full_dump_date = $dump_date;
		}

		;#
		;#  Store the information about the oldest FULL backup image
		;#
		if (!(exists $DB_oldest_full{$volume})) {
		    $DB_oldest_full{$volume} = $imagelist{$dump_date};
		    $oldest_full_dump_date = $dump_date;
		}
		elsif ($oldest_full_dump_date  >  $dump_date) {
		    $DB_oldest_full{$volume} = $imagelist{$dump_date};
		    $oldest_full_dump_date = $dump_date;
		}
	    }
	}


	if (($full_dump_cnt > 1)  &&  (!(exists $KEEP_volumes{$volume}))) {
	    if ($full_dump_cnt == 2) {
		;#
		;#  There exist TWO FULL dump images for this AFS volume.
		;#
		;#  Look at the time stamp of the most recent full dump image.  If it is
		;#  outside the data retention window ($SLA_DATA_RETENTION), then that means
		;#  we can remove the older full dump and all of its incremental dump images
		;#
		($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size,
		 $backup_level, $dump_date, $parent_dump_date) = split(/ /, $DB_full{$volume});

		if (($dump_date + $SLA_DATA_RETENTION) < $This_Dump_Date) {
		    $recent_full_dump_date = $dump_date;
		    $purge_cnt = 0;
		    foreach $dump_date (sort(keys %imagelist)) {
			if ($dump_date < $recent_full_dump_date) {
			    push (@OLD_Images, $imagelist{$dump_date});
			    $purge_cnt++;
			}
		    }
		    DisplayAndLogIt("The volume  $volume  has $purge_cnt dump image files to purge from the archive");
		}
		else {
		    ;#
		    ;#  The recent FULL dump image is to new, wait before purging the older dump image(s)
		    ;#
		    ;#  EXCEPT for the case:
		    ;#
		    ;#     Where the volume only has two images in the archive.  Both of which are fulls, which implies
		    ;#     the two full dump images are exactly the same.  Hence in this case force the older full image
		    ;#     to be purged from the archive
		    ;#
		    $i = 0;
		    foreach $dump_date (sort(keys %imagelist)) {
			$i++;
		    }

		    if ($i == 2) {
			($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size,
			 $backup_level, $dump_date, $parent_dump_date) = split(/ /, $DB_oldest_full{$volume});
 
			$older_volume_mod_time = $volume_mod_time;
			$older_volume_size = $volume_size;

			($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size,
			 $backup_level, $dump_date, $parent_dump_date) = split(/ /, $DB_full{$volume});
 
			if (($older_volume_mod_time == $volume_mod_time)  &&  ($older_volume_size == $volume_size)) {
			    push (@OLD_Images, $DB_oldest_full{$volume});
			    DisplayAndLogIt("The volume  $volume  has a duplicate full dump image file to purge from the archive");
			}
			else {
			    if (($older_volume_mod_time == $volume_mod_time)  &&  ($older_volume_size != $volume_size)) {
				DisplayErrorAndLogIt("ERROR:  The volume  $volume  appears to have duplicate full dump image files with different volume sizes");
			    }
			}
		    }
		}
	    }
	    else {
		;#
		;#  There exist 3 or more Full dump images for this AFS volume.  Try to purge older dump images
		;#  that are outside the data retention window ($SLA_DATA_RETENTION) 
		;#
		$recent_full_dump_date = 0;
		foreach $dump_date (sort(keys %full_dump_dates)) {
		    if (($dump_date + $SLA_DATA_RETENTION) < $This_Dump_Date) {
			$recent_full_dump_date = $dump_date;
		    }
		}

		$purge_cnt = 0;
		foreach $dump_date (sort(keys %imagelist)) {
		    if ($dump_date < $recent_full_dump_date) {
			push (@OLD_Images, $imagelist{$dump_date});
		        $purge_cnt++;
		    }
		}
		DisplayAndLogIt("The volume  $volume  has $purge_cnt dump image files to purge from the archive");
	    }
	}


	if (exists $KEEP_volumes{$volume}) {
	    ;#
	    ;#  If the volume has been identified as one that will not have its backup files within the IMAGE archive removed
	    ;#  then we will just post a message and get the next volume in the list to process
	    ;#
            DisplayAndLogIt("As specified by the \"-K\" option...   The volume  $volume  will NOT have any of its backup files within the IMAGE archive removed");
	}


	;#
	;#  Calculate the amount of space being used by the incremental dump images
	;#
	($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size,
	        $backup_level, $dump_date, $parent_dump_date) = split(/ /, $DB_full{$volume});

	$recent_full_dump_date = $dump_date;
        $disk_space = 0;
	foreach $dump_date (sort(keys %imagelist)) {
	    if ($dump_date  >  $recent_full_dump_date) {
		($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size,
                 $backup_level, $dump_date, $parent_dump_date) = split(/ /, $imagelist{$dump_date});

		$disk_space += $volume_size;
	    }
	}

	;#
	;#  determine how much space the incremental dump images are using
	;#
	($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size,
	    $backup_level, $dump_date, $parent_dump_date) = split(/ /, $DB_full{$volume});

	if (($volume_size * .25) > $disk_space) {
	    $message = "incrementals less than 25%";
	    $update_VolumeNames{$volume} = "normal";
	}
	elsif (($disk_space > ($volume_size * .25))  &&  (($volume_size * .45) > $disk_space)  &&  ($disk_space > $INCREMENTAL_MIN_SIZE)) {
	    $message = "incrementals over 25%";
	    if ($increment_status  eq  "normal") {
		$update_VolumeNames{$volume} = "NEXT_level_over_25_percent";
	    }
	}
	elsif (($disk_space > ($volume_size * .45))  &&  (($volume_size * .65) > $disk_space)  &&  ($disk_space > $INCREMENTAL_MIN_SIZE)) {
	    $message = "incrementals over 45%";
	    if (($increment_status  eq  "normal")  ||  ($increment_status  eq  "over_25_percent")) {
		$update_VolumeNames{$volume} = "NEXT_level_over_45_percent";
	    }
	}
	elsif (($disk_space > ($volume_size * .65))  &&  (($volume_size * .85) > $disk_space)  &&  ($disk_space > $INCREMENTAL_MIN_SIZE)) {
	    $message = "incrementals over 65%";
	    if (($increment_status  eq  "normal")  ||
		            ($increment_status  eq  "over_25_percent")  ||  ($increment_status  eq  "over_45_percent")) {
		$update_VolumeNames{$volume} = "NEXT_level_over_65_percent";
	    }

 	    if ($disk_space > $INCREMENTAL_MAX_SIZE) {
		;#
		;#  incremental storage is over the max size, the next run force a full dump 
		;#
		$message = "incrementals exceed MAX size";
		$update_VolumeNames{$volume} = "NEXT_full_over_max_size";
	    }
	}
	elsif ($disk_space > $INCREMENTAL_MIN_SIZE) {
	    $message = "incrementals over 85%";
	    if (($increment_status  eq  "normal")  ||  ($increment_status  eq  "over_25_percent")  ||
			    ($increment_status  eq  "over_45_percent")  ||  ($increment_status  eq  "over_65_percent")) {
		$update_VolumeNames{$volume} = "NEXT_full_over_85_percent";
	    }

 	    if ($disk_space > $INCREMENTAL_MAX_SIZE) {
		;#
		;#  incremental storage is over the max size, the next run force a full dump 
		;#
		$message = "incrementals exceed MAX size";
		$update_VolumeNames{$volume} = "NEXT_full_over_max_size";
	    }
	}
	else {
	    ;#
	    ;#  Else the size of the incremental storage being used has NOT exceeded the minimum size ($INCREMENTAL_MIN_SIZE)
	    ;#
	    $message = "";
	}

	if (($message  ne  "")  &&  ($message  ne  "incrementals less than 25%")) {
	    ;#  Only display message if incremental usage is over 25%
	    DisplayAndLogIt("$volume $message");
	}
    }


    if ($TH_Images)        {   $TH_Images->finish();        $TH_Images = "";        }
    if ($TH_VolumeNames)   {   $TH_VolumeNames->finish();   $TH_VolumeNames = "";   }

    ;#
    ;#  Update the status of volumes within the VolumeNames table moving to the next level in the backup schedule 
    ;#
    DisplayAndLogIt("Update the status of volumes moving to the next level in the backup schedule");
    $TH_VolumeNames = $DBH->prepare($SQL_update_VolumeNames_status);
    foreach $volume (sort(keys %update_VolumeNames)) {
	$new_status = $update_VolumeNames{$volume};
	;#
	;#  Update the table VolumeNames for each volume whose backup schedule will move to the next level
	;#
	$TH_VolumeNames->execute($new_status, $volume);
    }
    $TH_VolumeNames->finish();   $TH_VolumeNames = "";


    if ($#OLD_Images == -1) {
	DisplayAndLogIt("Found no old dump images to purge");
	return();
    }


    $disk_space = 0;
    $purge_cnt = 0;

    if (!($TH_History)) {   $TH_History = $DBH->prepare($SQL_insert_History);   }

    ;#
    ;#  If we have old images to purge let the user know
    ;#
    if ($#OLD_Images > -1) {
	DisplayAndLogIt("Begin to purge old vos dump files from the IMAGE archive");
    }

    for($i = 0;    $i <= $#OLD_Images;    $i++) {
	($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size, $backup_level, $dump_date, $parent_dump_date) = split(/ /, $OLD_Images[$i]);

	$date_string = ConvertDate($dump_date);

	LogIt("Delete from the Images table where volume = $volume  and  dump date = $date_string");
	$TH_Images = $DBH->prepare("DELETE FROM Images WHERE volume = \'$volume\' AND dump_date = \'$dump_date\'");
	$TH_Images->execute();

	LogIt("Add to the History table where volume = $volume  and  dump date = $date_string");
	$TH_History->execute($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size,
			     $backup_level, $dump_date, $parent_dump_date, $This_Dump_Date);

	if ($CREATE_SUBDIRS) {
	    $archive_path = GetSubDirectoryPath($volume);
	    $archive_path = sprintf("%s/%s", $ARCHDIR, $archive_path);
	}
	else {
	    $archive_path = sprintf("%s/%s", $ARCHDIR, $volume);
	}

	;#
	;#  Create the path to the dump image file
	;#
	($second, $minute, $hour, $monthday, $month, $year, $weekday, $yday, $isdst) = localtime($dump_date);
	$month++; $year += 1900;
        $basename = sprintf("%s__%d_%0.2d_%0.2d_%0.2d%0.2d__%s", $volume, $year, $month, $monthday, $hour, $minute, $backup_level);
	$fname = sprintf("%s/%s", $archive_path, $basename);

        ;#  Post message that the vos dump file  should be removed from the cloud object store
        $cloud_msg = sprintf("CloudSlice:   REMOVE     volume:%s:    vos dump file:%s:", $volume, $fname);
        LogIt("$cloud_msg");
    }

    ;#
    ;# recalculate the amount of space to recovered when the dump images were purged in terms of MB blocks
    ;#
    $disk_space = sprintf("%d", $disk_space / 1024);

    DisplayAndLogIt("Purged $purge_cnt old dump images from the archive");
    DisplayAndLogIt("This will free up $disk_space MB of storage within the archive");
}





;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   RemoveImagesOfPurgedAFSvolumes
;#
;#   Purpose:      To verify if the database information about the AFS volume(s) and their
;#                 coresponding dump images is current/valid.  What were are looking for is
;#                 the use case scenario where the AFS volume has been deleted from the AFS
;#                 cell.  In this case after the volumes dump image has been written to tape,
;#                 there is little reason to keep the dump image on disk as the likely hood
;#                 of a data restore is low.
;#
;#                 We are going to use the value of the "last_query_date" from the VolumeNames
;#                 table as an indicator that the AFS volume may have been deleted from the cell.
;#                 We will wait ($DELAY_DELETE) days from after the volume was last queried
;#                 by (vos listvol) this program before we delete the dump image.  This will
;#                 help in the rare event that we need to restore a perviously deleted volume.
;#
;#
;#   Nov 19th 2009
;#
;#                Added a second stage delay before removing the backup files of "PURGED"
;#                AFS volumes from the IMAGE archive.
;#
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   RemoveImagesOfPurgedAFSvolumes  {

    my ($last_query_date, $cmd, $line, $rtn_code, $pdays, $cloud_msg);
    my ($i, $j, $archive_path, $fname, @row, $history_cnt, $volume_cnt, $image_cnt, $disk_space);
    my ($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size, $backup_level, $dump_date, $parent_dump_date);
    my ($second, $minute, $hour, $monthday, $month, $year, $weekday, $yday, $isdst);

    my (%DirPath_Level_1, %DirPath_Level_2, @Deleted_volumes, @Deleted_images);


    %DirPath_Level_1 = ();
    %DirPath_Level_2 = ();
    @Deleted_volumes = ();          ;#  Volumes that have been deleted from the AFS cell
    @Deleted_images = ();           ;#  File paths to dump image files to delete
    @Deleted_images_vnames = ();    ;#  Volume names that corrspond to the dump images in @Deleted_images

    $disk_space = 0;

    DisplayAndLogIt("Look for dump images to remove because the AFS volume has been  \"PURGED\"  and no longer exists");

    $TH_VolumeNames = $DBH->prepare("SELECT * FROM VolumeNames");
    $TH_VolumeNames->execute();
    while (@row = $TH_VolumeNames->fetchrow_array()) {
	$volume = $row[0];
	$last_query_date = $row[1];

	if ($last_query_date != $This_Dump_Date) {
	    if (($last_query_date + $DELAY_DELETE) < $This_Dump_Date) {
		;#
		;#  Verify if volume has been deleted/purged from the AFS cell (vos examine)
		;#
		unlink ($STDout, $STDerr);
		$cmd = sprintf ("%s %s", $VOS_examine, $volume);
		`$cmd >$STDout 2>$STDerr`;
		if ($?) {
		    foreach $line (`$CAT $STDerr`) {
			chomp($line);
			if ($line  eq  "VLDB: no such entry") {
			    ;#
			    ;#  This volume appears to have been deleted or "PURGED" from the AFS cell
			    ;#
                            ;#  Before we delete any of the backup files for this volume from the IMAGE archive, make sure they are not being 
			    ;#  retained as specified by the -K option
			    ;#
                            if (!(exists $KEEP_volumes{$volume})) {
				;#
				;#  Now we are going to go into a 2nd stage delay during thsi time interval we will log messages that we will be
				;#  removing the backup files of "PURGED" AFS volumes from the IMAGE archive
				;#
				if (($last_query_date + $DELAY_DELETE_2nd_STAGE) < $This_Dump_Date) {
				    push (@Deleted_volumes, $volume);
				}
				else {
                                    $pdays = sprintf ("%d", ((($last_query_date + $DELAY_DELETE_2nd_STAGE) - $This_Dump_Date) / $ONE_DAY));
				    DisplayAndLogIt("WARN:  The PURGED volume  $volume   in $pdays day(s)   will have its backup files REMOVED from the IMAGE archive");
				}
			    }
			    else {
				DisplayAndLogIt("As specified by the \"-K\" option...   The PURGED volume  $volume  will NOT have any of its backup files within the IMAGE archive removed");
			    }
			}
			;#    [else] When the volume was examined  -  had an error in the VLDB  -  Do not delete dump image
			break;
		    }
		}
		;#    [else] When the volume was examined  -  vos examine was successful, the volume does exist  -  Do not delete dump image
	    }
	    ;#    [else] Waiting for the delay time period to be over ($DELAY_DELETE)
	}
	;#    [else] The volume was queried today 
    }

    if ($TH_History)        {   $TH_History->finish();       $TH_History = "";       }

    for($i = 0;    $i <= $#Deleted_volumes;    $i++) {
	;#
	;#  Get path to dump image(s) to delete from the archive
	;#
	$volume = $Deleted_volumes[$i];

	$TH_VolumeNames = $DBH->prepare("SELECT * FROM VolumeNames WHERE volume = \'$volume\'");
	$TH_VolumeNames->execute();
        while (@row = $TH_VolumeNames->fetchrow_array()) {
	    $volume = $row[0];
	    $last_query_date = $row[1];
	}

	$last_query_date = ConvertDate($last_query_date);

	if ($CREATE_SUBDIRS) {
	    $archive_path = GetSubDirectoryPath($volume);
	    $archive_path = sprintf("%s/%s", $ARCHDIR, $archive_path);
	}
	else {
	    $archive_path = sprintf("%s/%s", $ARCHDIR, $volume);
	}

	$history_cnt = 0;
	$fname = "";

	;#
	;#  Save the path to the directory ($DirPath_Level_1) and the level above ($DirPath_Level_2)
	;#
	$DirPath_Level_1{$archive_path} = 0;
        @_ = split(/\//, $archive_path);
	for ($j = 1;    $j < $#_;    $j++) {
	    $fname = sprintf("%s/%s", $fname, $_[$j]);
	}
	$DirPath_Level_2{$fname} = 0;

	;#
	;#  Query the Images table to get all the information about the dump images on disk
	;#  We use this information to build the file path for the dump image that we will delete
	;#
	$TH_Images = $DBH->prepare("SELECT * FROM Images WHERE volume = \'$volume\'");
	$TH_Images->execute();

	while (@row = $TH_Images->fetchrow_array()) {
	    $volume              = $row[0];
	    $vid                 = $row[1];
	    $volume_create_time  = $row[2];
	    $volume_mod_time     = $row[3];
	    $volume_size         = $row[4];
	    $backup_level        = $row[5];
	    $dump_date           = $row[6];
	    $parent_dump_date    = $row[7];
	    ;#
	    ;#  Create the path to the dump image file
	    ;#
	    ($second, $minute, $hour, $monthday, $month, $year, $weekday, $yday, $isdst) = localtime($dump_date);
	    $month++; $year += 1900;
	    $fname = sprintf("%s/%s__%d_%0.2d_%0.2d_%0.2d%0.2d__%s", $archive_path, $volume, $year, $month, $monthday, $hour, $minute, $backup_level);

	    push (@Deleted_images, $fname);
            push (@Deleted_images_vnames, $volume);

	    ;#
	    ;#  Copy the record from the "Images" table to the "History" table
	    ;#  Add the date the dump image file is deleted [$This_Dump_Date] from the disk
	    ;#
	    if (!($TH_History)) {   $TH_History = $DBH->prepare($SQL_insert_History);   }
	    $TH_History->execute($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size,
				 $backup_level, $dump_date, $parent_dump_date, $This_Dump_Date);
	    $disk_space += $volume_size;
	    $history_cnt++;
	}

	if ($history_cnt > 0) {   LogIt("For the PURGED volume  $volume  added $history_cnt records to the History table");   }
    }

    if ($TH_History)        {   $TH_History->finish();       $TH_History = "";       }


    if ($#Deleted_volumes  ==  -1) {
	DisplayAndLogIt("No dump images need to be PURGED");
    }
    else {
	$volume_cnt = $#Deleted_volumes + 1;
	$image_cnt = $#Deleted_images + 1;

	DisplayAndLogIt("Begin the PURGE process for the  $volume_cnt  AFS volumes that no longer exist");
	DisplayAndLogIt("Remove the corresponding records from the tables:  Images  and  VolumeNames");

	for($i = 0;    $i <= $#Deleted_volumes;    $i++) {
	    ;#
	    ;#  Get name of the AFS volume that no longer exist in the AFS cell
	    ;#
	    $volume = $Deleted_volumes[$i];
	    ;#
	    ;#  Delete references to the AFS volume from the Images table and the VolumeNames table
	    ;#
	    LogIt("Remove references to the PURGED volume  $volume  from the Images table and the VolumeNames table");
	    $TH_Images = $DBH->prepare("DELETE FROM Images WHERE volume = \'$volume\'");
	    $TH_Images->execute();

	    $TH_VolumeNames = $DBH->prepare("DELETE FROM VolumeNames WHERE volume = \'$volume\'");
	    $TH_VolumeNames->execute();
	}

	;#
	;# recalculate the amount of space to recovered when the dump images are deleted in terms of MB blocks
	;#
	$disk_space = sprintf("%d", $disk_space / 1024);

	DisplayAndLogIt("Purge the corresponding $image_cnt dump images for the PURGED volume  $volume");
	DisplayAndLogIt("This will free up $disk_space MB of space in the IMAGE archive");

	;#
	;#  Remove each of the image files from the disk archive
	;#
	for($i = 0;    $i <= $#Deleted_images;    $i++) {
	    ;#
	    ;#  Get the full path to dump image to delete from the disk archive
	    ;#
	    $fname = $Deleted_images[$i];
            $volume = $Deleted_images_vnames[$i];

            ;#  Post message that the vos dump file  should be  purged  from the cloud object store
            $cloud_msg = sprintf("CloudSlice:   PURGE     volume:%s:    vos dump file:%s:", $volume, $fname);
            LogIt("$cloud_msg");
	}
    }
}




;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   Update_VolumeNames_last_query_date
;#
;#   Purpose:      To update the table VolumeNames with the date stamp when the AFS volumes was
;#                 last queried via the vos listvol command.
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   Update_VolumeNames_last_query_date  {

    my ($volume, $last_query_date);


    DisplayAndLogIt("Update the last query date");

    $last_query_date = $This_Dump_Date;

    if ($TH_VolumeNames) {   $TH_VolumeNames->finish();   }
    $TH_VolumeNames = $DBH->prepare($SQL_update_VolumeNames_last_query_date);

    foreach $volume (sort(keys %RW_volumes)) {
	;#
	;#  Update the table VolumeNames for each volume that was queried (vos listvol)
	;#
	$TH_VolumeNames->execute($last_query_date, $volume);
    }
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   CreateDumpCommands
;#
;#   Purpose:      Based on the contents of %DUMP_volumes array, build the appropriate vos dump
;#                 comamnds and store them within %DUMP_commands.
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   CreateDumpCommands  {

    my ($volume, $vid, $volume_create_time, $bk_volume_mod_time, $bk_volume_size, $backup_level, $dump_date, $parent_dump_date);
    my ($previous_backup_level, $previous_bk_volume_mod_time, $parent_bk_volume_mod_time, $bk_volume, $record_count);
    my ($cmd, $string, $fname, $no_colon_link_fname, $tstamp, $archive_path, $archive_image_file);
    my ($second, $minute, $hour, $monthday, $month, $year, $weekday, $yday, $isdst);


    LogIt("Display the  -  vos dump commands within the array DUMP_commands");

    foreach $volume (sort(keys %DUMP_volumes)) {
	;#
	;#  Get the information for the current volume we are going to dump from the array $DUMP_volumes
	;#
        ($volume, $vid, $volume_create_time, $bk_volume_mod_time, $bk_volume_size,
	 $backup_level, $dump_date, $parent_dump_date) = split(/ /, $DUMP_volumes{$volume});

	$bk_volume = sprintf("%s.backup", $volume);

	if ($dump_date  ==  $parent_dump_date) {
	    ;#  This is a full dump so the time stamp is zero
	    $tstamp = "0";
	}
	else {
	    ;#  This is an incremental dump
	    ;#
	    if ($DUMP_STYLE  eq  "Accumulative") {
		;#
		;#  Get the record from the database where the current value for "parent_dump_date" was the dump date
		;#  in this record the value of the volume modification time is value to use as the time parameter for
		;#  for the vos dump command
		;#
		$TH_Images = $DBH->prepare("SELECT * FROM Images WHERE volume = \'$volume\' AND dump_date = \'$parent_dump_date\'");
		$TH_Images->execute();
		$record_count = 0;
		while (@row = $TH_Images->fetchrow_array()) {
		    $parent_bk_volume_mod_time     = $row[3];
		    $record_count++;
		}

		if ($record_count  !=  1) {
		    ($second, $minute, $hour, $monthday, $month, $year, $weekday, $yday, $isdst) = localtime($parent_dump_date);
		    $month++; $year += 1900;
		    $string = sprintf("%0.2d/%0.2d/%d %0.2d:%0.2d", $month, $monthday, $year, $hour, $minute);
		    if ($record_count  ==  0) {
			$string = sprintf ("ERROR:  For volume %s   unable to query Image table with dump date  %s", $volume, $string);
		    }
		    else {
			$string = sprintf ("ERROR:  For volume %s   more than one record in Image table with dump date  %s", $volume, $string);
		    }
		    $DUMP_no_try{$volume} = $string;
		    LogIt("$DUMP_no_try{$volume}");
		    next;
		}

		($second, $minute, $hour, $monthday, $month, $year, $weekday, $yday, $isdst) = localtime($parent_bk_volume_mod_time);
	    }
	    else {
		if ($DUMP_STYLE  ne  "Differential") {
		    LogIt("WARN:  Variable DUMP_STYLE has unknown value [$DUMP_STYLE]  over ride and assume dump style of Differential");
		}


		;#
		;#  From the last entry in the database, get the volume's previous backup volume's modification time ($previous_bk_volume_mod_time)
		;#  When doing "Differential" incremental backups this value will used as the time parameter for the vos dump command 
		;#
		if (exists $DB_recent{$volume}) {
		    ($volume, $vid, $volume_create_time, $previous_bk_volume_mod_time,
		     $bk_volume_size, $previous_backup_level, $dump_date, $parent_dump_date) = split(/ /, $DB_recent{$volume});


		    ;#  Use the value $previous_bk_volume_mod_time as the time parameter for the vos dump command
		    ($second, $minute, $hour, $monthday, $month, $year, $weekday, $yday, $isdst) = localtime($previous_bk_volume_mod_time);

		}
		else {
		    ;#  This is an error condition this volume should have a recent entry in the database
		    ;#
		    $string = sprintf ("ERROR:  For volume %s   should have recent entry within database, no value found in the array DB_recent", $volume);
		    $DUMP_no_try{$volume} = $string;
		    LogIt("$DUMP_no_try{$volume}");
		    next;
		}
	    }

	    ;#  Convert vos dump time "parameter" into string  "mm/dd/yyyy HH:MM"
	    $month++; $year += 1900;
	    $tstamp = sprintf("%0.2d/%0.2d/%d %0.2d:%0.2d", $month, $monthday, $year, $hour, $minute);
	}

	;#  create the file name used for the dump image
	($second, $minute, $hour, $monthday, $month, $year, $weekday, $yday, $isdst) = localtime($This_Dump_Date);
	$month++; $year += 1900;
	$fname = sprintf("%s__%d_%0.2d_%0.2d_%0.2d%0.2d__%s",  $volume, $year, $month, $monthday, $hour, $minute, $backup_level);

	;#  Figure out the path where to insert the vos dump file into the IMAGE archive

	if ($CREATE_SUBDIRS) {
	    $archive_path = GetSubDirectoryPath($volume);
	    $archive_path = sprintf("%s/%s", $ARCHDIR, $archive_path);
	    unlink ($STDout, $STDerr);
	    $cmd = sprintf("%s %s", $MAKEDIR, $archive_path);
	    `$cmd >$STDout 2>$STDerr`;
	    if ($?) {
		DisplayAndLogIt("CMD:  $cmd");
	        DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to create the directory");
		$archive_image_file = "";
	    }
	    else {
	        $archive_image_file = sprintf("%s/%s", $archive_path, $fname);
	    }
	}
        else {
	    $archive_image_file = sprintf("%s/%s", $ARCHDIR, $fname);
	}

	if ($archive_image_file  eq  "") {
	    $DUMP_no_try{$volume} = sprintf ("ERROR:  For volume %s   unable to create the directory  %s", $volume, $archive_path);
	    LogIt("$DUMP_no_try{$volume}");
	    next;
	}

	$fname = sprintf("%s/%s",  $DUMPDIR, $fname);

	;#
	;#  Build the command to create a vos dump file within the IMAGE archive
	;#
	$string = sprintf("%s %s -time \"%s\" -file %s -verbose >%s.stdout 2>%s.stderr",
			  $VOS_dump, $bk_volume, $tstamp, $archive_image_file, $fname, $fname);

	;#
	;#  After the vos dump file is created we will use a tool "afsdump_extract" to list the meta data about the
	;#  files that are within the vos dump file.
	;#
	;#
	;#  This comes from version 1.2 of the CMU volume dump utilities written by Jeff Hutzelman from Carnegie Mellon University
	;#
	;#
	;#  Note I found a bug in these utilities that they did not like file names that had a colon, the work around was to
	;#  create s symbolic link to the vos dump file to examine.
	;#
	$no_colon_link_fname = sprintf("%s/%s__%d_%0.2d_%0.2d_%s",  $DUMPDIR, $volume, $year, $month, $monthday, $backup_level);
	$string = sprintf("%s ; %s %s %s ; %s %s >%s.meta 2>%s.meta_error ; %s %s ; %s %s.done",
			  $string, $SYMLINK, $archive_image_file, $no_colon_link_fname, $LIST_VOS_dump,
			  $no_colon_link_fname, $fname, $fname, $REMOVE, $no_colon_link_fname, $TOUCH, $fname);

	$DUMP_commands{$volume} = $string;
	LogIt("$DUMP_commands{$volume}");
    }
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   ListVolumes2Dump
;#
;#   Purpose:      To list the volume sthat will be dumped.  This is the contents of %DUMP_volumes
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   ListVolumes2Dump  {

    my ($volume, $counter);

    $counter = 0;

    LogIt("Display volumes that will be dumped");

    foreach $volume (sort(keys %DUMP_volumes)) {
	$counter++;
	LogIt("$DUMP_volumes{$volume}");
    }

    DisplayAndLogIt("Will be dumping $counter volumes");
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   BuildDumpList
;#
;#   Purpose:      To query the backup database and compare to the current status of the RW volumes
;#                 within the cell.  The comparison is done on the last modification or update time
;#                 of the corresponding backup volume (BK).  The end result is a list of volumes
;#                 that need to be dumped.  This information is contained within the array %DUMP_volumes
;#
;#                 This function supports both the "Accumulative"  and the  "Differential" incremental
;#		   dump methods
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   BuildDumpList  {

    my (@row, $major, $minor, $junk, $full_dump_date, $full_parent_dump_date);
    my ($image_cnt, $image_cnt_fulls, $size_of_fulls, $size_of_images);

    my ($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size, $backup_level, $dump_date, $parent_dump_date);

    my ($rw_server, $rw_partition, $rw_volume, $rw_vid, $rw_backup_vid, $rw_type, $rw_volume_create_time, $rw_volume_mod_time);
    my ($rw_volume_size, $rw_volume_quota, $rw_number_of_files, $rw_vnode_accesses);

    my ($bk_server, $bk_partition, $bk_volume, $bk_vid, $bk_backup_vid, $bk_type, $bk_volume_create_time, $bk_volume_mod_time);
    my ($bk_volume_size, $bk_volume_quota, $bk_number_of_files, $bk_vnode_accesses);

    my ($last_query_date, $increment_status, %status_VolumeNames);
    my ($recent_dump_date, $recent_full_dump_date);


    ;#
    ;#  Query the Images table to find the record for most recent backup for each AFS volume within the backup data base
    ;#
    DisplayAndLogIt("Query the Images table  -  find the record of the most recent backup for each AFS volume");


    $TH_VolumeNames = $DBH->prepare("SELECT * FROM VolumeNames");
    $TH_VolumeNames->execute();

    ;#
    ;#  For each volume within the VolumeNames table query the Images table and populate the arrayss %DB_full and %DB_recent
    ;#
    while (@row = $TH_VolumeNames->fetchrow_array()) {
	$volume = $row[0];
	$last_query_date = $row[1];
	$increment_status = $row[2];
	$status_VolumeNames{$volume} = $increment_status;

	$TH_Images = $DBH->prepare("SELECT * FROM Images WHERE volume = \'$volume\'");
	$TH_Images->execute();
	$recent_dump_date = 0;
	$recent_full_dump_date = 0;
	while (@row = $TH_Images->fetchrow_array()) {
	    $volume              = $row[0];
	    $vid                 = $row[1];
	    $volume_create_time  = $row[2];
	    $volume_mod_time     = $row[3];
	    $volume_size         = $row[4];
	    $backup_level        = $row[5];
	    $dump_date           = $row[6];
	    $parent_dump_date    = $row[7];
	    ;#
	    ;#  Store the record for most recent backup for the AFS volume
	    ;#
	    if ($dump_date  >  $recent_dump_date) {
		$DB_recent{$volume} = "$volume $vid $volume_create_time $volume_mod_time $volume_size $backup_level $dump_date $parent_dump_date";
		$recent_dump_date = $dump_date;
	    }
	    if ($backup_level  eq  "0-0") {
		;#
		;#  Store the record for most recent FULL backup for the AFS volume
		;#
		if ($dump_date  >  $recent_full_dump_date) {
		    $DB_full{$volume} = "$volume $vid $volume_create_time $volume_mod_time $volume_size $backup_level $dump_date $parent_dump_date";
		    $recent_full_dump_date = $dump_date;
		}
	    }
	}
    }


    if ($TH_Images)         {   $TH_Images->finish();        $TH_Images = "";        }
    if ($TH_VolumeNames)    {   $TH_VolumeNames->finish();   $TH_VolumeNames = "";   }

    DisplayAndLogIt("Examine each AFS volume within the array RW_volumes");

    foreach $volume (sort(keys %RW_volumes)) {
	;#
	;#  Get the information about the RW volume
	;#
	($rw_server, $rw_partition, $rw_volume, $rw_vid, $rw_backup_vid, $rw_type, $rw_volume_create_time, $rw_volume_mod_time, $rw_volume_size,
	 $rw_volume_quota, $rw_number_of_files, $rw_vnode_accesses) = split(/ /, $RW_volumes{$volume});
	;#
	;#  Get the information about the BK (backup) volume, create the BK volume if it does not exist
	;#
	$bk_volume = sprintf("%s.backup", $volume);
	if (!(exists $BK_volumes{$bk_volume})) {
	    ;#
	    ;#  The RW volumes does not appear to have a corresponding BK volume, need to create a backup volume
	    ;#
	    if (!(CreateBackUpVolume($volume, "Backup volume does not exist"))) {
		DisplayAndLogIt("Skip dump of $volume   unable to create backup volume");
		next;
	    }
	}
	else {
	    ;#
	    ;#  Make sure the BK volume is not stale (more than 24 hours old), If it is stale create a new backup volume
	    ;#
	    if (($This_Dump_Date - $ONE_DAY) > $bk_volume_create_time) {
		;#
		;#  Backup volume is more than 24 hours old, it is stale and needs to be recreated
		;#
		if (!(CreateBackUpVolume($volume, "Backup volume is stale"))) {
		    DisplayAndLogIt("Skip dump of $volume   unable to create backup volume");
		    next;
		}
	    }
	}

	;#  Get the information about the BK volume
	;#
	($bk_server, $bk_partition, $bk_volume, $bk_vid, $bk_backup_vid, $bk_type, $bk_volume_create_time, $bk_volume_mod_time, $bk_volume_size,
	 $bk_volume_quota, $bk_number_of_files, $bk_vnode_accesses) = split(/ /, $BK_volumes{$bk_volume});

	;#  Track the option -f from the command line to force a full dump on specific volumes.    For  $EXPLICIT_full_dump   a value of "1" indicates that the volume exists
	;#
	if (exists $EXPLICIT_full_dump{$volume})   {   $EXPLICIT_full_dump{$volume} = 1;   }


	if (!(exists $DB_recent{$volume})) {
	    ;#
	    ;#  The volume does NOT have a entry within the array DB_recent, that would indicate that it has not been recorded within the database.
	    ;#  Therefore it implies that this is the first time it is being backed up (a new volume) so force a full [level "0-0"]
	    ;#
	    $dump_date = $This_Dump_Date;
	    $parent_dump_date = $dump_date;
	    $backup_level = "0-0";
	    $DUMP_volumes{$volume} = sprintf("%s %s %s %s %s %s %s %s", $volume, $rw_vid, $rw_volume_create_time,
						 $bk_volume_mod_time, $bk_volume_size, $backup_level, $dump_date, $parent_dump_date);
	    $NEW_volumes{$volume} = $volume;
	    DisplayIt("Force FULL - new volume $volume");
	    LogIt("Force FULL - new volume:   $DUMP_volumes{$volume}");

	    ;#  Track the option to force a full dump.     For  $EXPLICIT_full_dump   a value of "2" indicates the volume will have a full dump
	    ;#
	    if (exists $EXPLICIT_full_dump{$volume})   {   $EXPLICIT_full_dump{$volume} = 2;   }
	    next;
	}

	if (!(exists $DB_full{$volume})) {
	    ;#
	    ;#  The volume does NOT have a FULL image recorded within the database, force a full dump (backup_level "0-0")
	    ;#
	    $dump_date = $This_Dump_Date;
	    $parent_dump_date = $dump_date;
	    $backup_level = "0-0";
	    $DUMP_volumes{$volume} = sprintf("%s %s %s %s %s %s %s %s", $volume, $rw_vid, $rw_volume_create_time,
						 $bk_volume_mod_time, $bk_volume_size, $backup_level, $dump_date, $parent_dump_date);
	    DisplayIt("Force FULL - no full record in db for $volume");
	    LogIt("Force FULL - no full record in db:   $DUMP_volumes{$volume}");

	    ;#  Track the option to force a full dump.     For  $EXPLICIT_full_dump   a value of "2" indicates the volume will have a full dump
	    ;#
	    if (exists $EXPLICIT_full_dump{$volume})   {   $EXPLICIT_full_dump{$volume} = 2;   }
	    next;
	}

	;#
	;#  Get the volume's most recent FULL image entry from the backup data base
	;#
	($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size, $backup_level,
	 $full_dump_date, $full_parent_dump_date) = split(/ /, $DB_full{$volume});

	;#
	;#  Get the volume's most recent entry from the backup data base
	;#
	($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size, $backup_level,
	 $dump_date, $parent_dump_date) = split(/ /, $DB_recent{$volume});

	;#
	;#  Get the status of the volume's usage of storage for incremental dumps
	;#
	$increment_status = $status_VolumeNames{$volume};

	if ($This_Dump_Date > ($full_dump_date + $MAX_DAYS_BETWEEN_FULLS)) {
	    ;#
	    ;#  If the volume has not had a FULL dump in the last ($MAX_DAYS_BETWEEN_FULLS) then force a full dump
	    ;#
	    $dump_date = $This_Dump_Date;
	    $parent_dump_date = $This_Dump_Date;
	    $backup_level = "0-0";
	    $DUMP_volumes{$volume} = sprintf ("%s %s %s %s %s %s %s %s", $volume, $rw_vid, $rw_volume_create_time,
					      $bk_volume_mod_time, $bk_volume_size, $backup_level, $dump_date, $parent_dump_date);
			
	    DisplayAndLogIt("Force FULL - last full for $volume is over $MAX_DAYS days old");
	    LogIt("Volume dump record:  $DUMP_volumes{$volume}");

	    ;#  Track the option to force a full dump.     For  $EXPLICIT_full_dump   a value of "2" indicates the volume will have a full dump
	    ;#
	    if (exists $EXPLICIT_full_dump{$volume})   {   $EXPLICIT_full_dump{$volume} = 2;   }
	}
	elsif ($bk_volume_mod_time > $dump_date) {
	    ;#
	    ;#  The volume has been modified (updated) since the last time it was dumped.
	    ;#
	    if (($increment_status  eq  "normal")  ||  ($increment_status  eq  "over_25_percent")  ||
		($increment_status  eq  "over_45_percent")  ||  ($increment_status  eq  "over_65_percent")  ||
		($increment_status  eq  "over_85_percent")) {
		;#
		;#  Now determine what the dump level should be
		;#
		($major, $minor) = split(/-/, $backup_level);
		if ($minor  >=  $MINOR_MAX) {
		    ;#
		    ;#  reset minor counter to zero and increment the major counter
		    $minor = 0;
		    if ($major  >=  $MAJOR_MAX) {
			;#
			;#  The scheduled incrementals have rolled over, force a full backup of the AFS volume
			$major = 0;
			$parent_dump_date = $This_Dump_Date;
		    }
		    else {
			$major++;
			if ($DUMP_STYLE  eq  "Accumulative") {
			    ;#
			    ;#  This MAJOR level dump will be an incremental based on the last full dump,
			    ;#  Set the "parent_dump_date" to the Full's dump date
			    ;#
			    $parent_dump_date = $full_parent_dump_date;
			}
			elsif ($DUMP_STYLE  eq  "Differential") {
			    ;#
			    ;#  This MAJOR level dump will be an incremental dump that will contain the changes since the previous dump
			    ;#     The effect of this will be to reduce the amount of space used on disk by the vos dump files.
			    ;#
			    $parent_dump_date = $dump_date;
			}
			else {
			    $parent_dump_date = $dump_date;
			    LogIt("WARN:  Variable DUMP_STYLE has unknown value [$DUMP_STYLE]  over ride and force a dump style of Differential");
			}
		    }
		}
		else {
		    $minor++;
		    if ($DUMP_STYLE  eq  "Accumulative") {
			;#
			;#  The next incremental dump will be based on the last MAJOR level dump
			;#     If doing the first MINOR level set the "parent_dump_date" to the dump date of the prevous MAJOR level
			;#
			if ($minor  ==  1) {   $parent_dump_date = $dump_date;   }
		    }
		    elsif ($DUMP_STYLE  eq  "Differential") {
			;#
			;#  The next incremental dump will contain the changes since the previous dump, this will reduce the
			;#  amount of space used on disk by the vos dump files.
			;#
			$parent_dump_date = $dump_date;
		    }
		    else {
			$parent_dump_date = $dump_date;
			LogIt("WARN:  Variable DUMP_STYLE has unknown value [$DUMP_STYLE]  over ride and assume dump style of Differential");
		    }
		}

		;#  Add this entry for volumes to be dumped
		;#
		$dump_date = $This_Dump_Date;
		$backup_level = sprintf("%s-%s", $major, $minor);
		$DUMP_volumes{$volume} = sprintf ("%s %s %s %s %s %s %s %s", $volume, $rw_vid, $rw_volume_create_time,
							     $bk_volume_mod_time, $bk_volume_size, $backup_level, $dump_date, $parent_dump_date);

		if ($backup_level  eq  "0-0") {
		    ;#
		    ;#  The scheduled incrementals have rolled over, force a full backup of the AFS volume
		    ;#
		    DisplayAndLogIt("Scheduled FULL dump for $volume");
		    LogIt("Volume dump record:  $DUMP_volumes{$volume}");

		    ;#
                    ;#  Track the option to force a full dump.     For  $EXPLICIT_full_dump   a value of "2" indicates the volume will have a full dump
		    ;#
		    if (exists $EXPLICIT_full_dump{$volume})   {   $EXPLICIT_full_dump{$volume} = 2;   }
		}
		else {
		    if (exists $EXPLICIT_full_dump{$volume}) {
			;#
		        ;#  Over ride the dump schedule and do a FULL
			;#
			;#      track the option to force a full dump.  A value of "2" indicates the volume will have a full dump
			;#
			$parent_dump_date = $This_Dump_Date;
			$backup_level = "0-0";
			$DUMP_volumes{$volume} = sprintf ("%s %s %s %s %s %s %s %s", $volume, $rw_vid, $rw_volume_create_time,
							     $bk_volume_mod_time, $bk_volume_size, $backup_level, $dump_date, $parent_dump_date);

			DisplayAndLogIt("Over ride schedule  -  User directed force FULL dump for $volume");
			LogIt("Volume dump record:  $DUMP_volumes{$volume}");
			$EXPLICIT_full_dump{$volume} = 2;
		    }
		}
	    }
	    elsif ($increment_status  eq  "over_max_size") {
	        ;###  elsif ($increment_status  eq  "over_85_percent")  ||  ($increment_status  eq  "over_max_size")) {
		;#
		;#
		;#  Force a full backup of the AFS volume
		;#
		$dump_date = $This_Dump_Date;
		$parent_dump_date = $This_Dump_Date;
		$backup_level = "0-0";
		$DUMP_volumes{$volume} = sprintf ("%s %s %s %s %s %s %s %s", $volume, $rw_vid, $rw_volume_create_time,
								  $bk_volume_mod_time, $bk_volume_size, $backup_level, $dump_date, $parent_dump_date);
		DisplayAndLogIt("Force FULL - storage utilization to high for $volume");
		LogIt("Volume dump record:  $DUMP_volumes{$volume}");

		;#
		;#  Track the option to force a full dump.     For  $EXPLICIT_full_dump   a value of "2" indicates the volume will have a full dump
		;#
		if (exists $EXPLICIT_full_dump{$volume})   {   $EXPLICIT_full_dump{$volume} = 2;   }
	    }
	    elsif (($increment_status  eq  "NEXT_level_over_25_percent")  ||
		   ($increment_status  eq  "NEXT_level_over_45_percent")  ||  ($increment_status  eq  "NEXT_level_over_65_percent")) {
		;#
		;#  Change the backup level to the next MAJOR level
		;#
		if ($DUMP_STYLE  eq  "Accumulative") {
		    ;#
		    ;#  Now determine what the MAJOR dump level should be.
		    ;#
		    ;#     The MINOR counter is reset to zero and the major counter will be incremented
		    ;#
		    ($major, $minor) = split(/-/, $backup_level);
		    $minor = 0;
		    if ($major  >=  $MAJOR_MAX) {
			;#
			;#  The scheduled incrementals have rolled over, force a full backup of the AFS volume
		        ;#
			$major = 0;
			$parent_dump_date = $This_Dump_Date;
		    }
		    else {
			;#
			;#  Do an incremental to the next MAJOR level based on the last full dump,
			;#
			;#  Set the "parent_dump_date" to the Full's dump date
			;#
			$major++;
			$parent_dump_date = $full_parent_dump_date;
		    }
		}
		else {
		    if ($DUMP_STYLE  ne  "Differential") {
			LogIt("WARN:  Variable DUMP_STYLE has unknown value [$DUMP_STYLE]  over ride and assume dump style of Differential");
		    }
		    ;#
		    ;#  The next incremental dump will contain the changes since the previous dump, this will reduce the
		    ;#  amount of space used on disk by the vos dump files.
		    ;#
		    $parent_dump_date = $dump_date;

		    ($major, $minor) = split(/-/, $backup_level);
		    if ($minor  >=  $MINOR_MAX) {
			$minor = 0;
			if ($major  >=  $MAJOR_MAX) {
			    ;#
			    ;#  The scheduled incrementals have rolled over, force a full backup of the AFS volume
			    ;#
			    $major = 0;
			    $parent_dump_date = $This_Dump_Date;
			}
			else {
			    $major++;
			}
		    }
		    else {
			$minor++;
		    }
		}

		;#
		;#  Add this entry for volumes to be dumped
		;#
		$dump_date = $This_Dump_Date;
		$backup_level = sprintf("%s-%s", $major, $minor);
		$DUMP_volumes{$volume} = sprintf ("%s %s %s %s %s %s %s %s", $volume, $rw_vid, $rw_volume_create_time,
							     $bk_volume_mod_time, $bk_volume_size, $backup_level, $dump_date, $parent_dump_date);
		if ($backup_level  eq  "0-0") {
		    ;#
		    ;#  The scheduled incrementals have rolled over, force a full backup of the AFS volume
		    ;#
		    DisplayAndLogIt("Scheduled FULL dump for $volume");
		    LogIt("Volume dump record:  $DUMP_volumes{$volume}");

		    ;#  Track the option to force a full dump.  A value of "2" indicates the volume will have a full dump
		    ;#
		    if (exists $EXPLICIT_full_dump{$volume})   {   $EXPLICIT_full_dump{$volume} = 2;   }
		}
		else {
		    if (exists $EXPLICIT_full_dump{$volume}) {
			;#
		        ;#  Over ride the dump schedule and do a FULL
			;#
			;#      track the option to force a full dump.     For  $EXPLICIT_full_dump   a value of "2" indicates the volume will have a full dump
			;#
			$parent_dump_date = $This_Dump_Date;
			$backup_level = "0-0";
			$DUMP_volumes{$volume} = sprintf ("%s %s %s %s %s %s %s %s", $volume, $rw_vid, $rw_volume_create_time,
							     $bk_volume_mod_time, $bk_volume_size, $backup_level, $dump_date, $parent_dump_date);

			DisplayAndLogIt("Over ride schedule  -  User directed force FULL dump for $volume");
			LogIt("Volume dump record:  $DUMP_volumes{$volume}");
			$EXPLICIT_full_dump{$volume} = 2;
		    }
		}
	    }
	    elsif (($increment_status  eq  "NEXT_full_over_85_percent")  ||  ($increment_status  eq  "NEXT_full_over_max_size")) {
		;#
		;#  Force a full backup of the AFS volume
		;#
		$dump_date = $This_Dump_Date;
		$parent_dump_date = $This_Dump_Date;
		$backup_level = "0-0";
		$DUMP_volumes{$volume} = sprintf ("%s %s %s %s %s %s %s %s", $volume, $rw_vid, $rw_volume_create_time,
								  $bk_volume_mod_time, $bk_volume_size, $backup_level, $dump_date, $parent_dump_date);
		DisplayAndLogIt("Force FULL - storage utilization to high for $volume");
		LogIt("Volume dump record:  $DUMP_volumes{$volume}");

		;#
		;#  Track the option to force a full dump.     For  $EXPLICIT_full_dump   a value of "2" indicates the volume will have a full dump
		;#
		if (exists $EXPLICIT_full_dump{$volume})   {   $EXPLICIT_full_dump{$volume} = 2;   }
	    }
	}
	else {
	    ;#
	    ;#  The RW volume is not going to backed up because it has not been modified since it was last backed up
	    ;#
	    ;#     But if the volume is listed within the EXPLICIT_full_dump array then force a full dump
	    ;#
	    if (exists $EXPLICIT_full_dump{$volume}) {
		$dump_date = $This_Dump_Date;
		$parent_dump_date = $This_Dump_Date;
		$backup_level = "0-0";
		$DUMP_volumes{$volume} = sprintf ("%s %s %s %s %s %s %s %s", $volume, $rw_vid, $rw_volume_create_time,
					      $bk_volume_mod_time, $bk_volume_size, $backup_level, $dump_date, $parent_dump_date);
			    
		DisplayAndLogIt("User directed force FULL dump for $volume");
		LogIt("Volume dump record:  $DUMP_volumes{$volume}");

		;#
		;#  Track the option to force a full dump.     For  $EXPLICIT_full_dump   a value of "2" indicates the volume will have a full dump
		;#
		$EXPLICIT_full_dump{$volume} = 2;
	    }
	}
    }

    foreach $volume (sort(keys %EXPLICIT_full_dump)) {
        if ($EXPLICIT_full_dump{$volume}  ==  0) {
	    DisplayAndLogIt("ERROR:   $volume does not exist  -  unable to do User directed force FULL dump");
	}
        if ($EXPLICIT_full_dump{$volume}  ==  1) {
	    DisplayAndLogIt("ERROR:   no backup volume for $volume  -  unable to do User directed force FULL dump");
	}
    }
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   CreateBackUpVolume
;#
;#   Purpose:      To create a backup volume for the specified read/write volume and update %BK_volumes
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   CreateBackUpVolume  {

    my ($volume, $message) = @_;

    my ($cmd, $line, $junk, $junk2, $return_flag);
    my ($bk_server, $bk_partition, $bk_volume, $bk_vid, $bk_backup_vid, $bk_type, $bk_volume_create_time, $bk_volume_mod_time);
    my ($bk_volume_size, $bk_volume_quota, $bk_number_of_files, $bk_vnode_accesses);


    DisplayAndLogIt("$message   Clone backup volume for $volume");

    $return_flag = 1;
    $bk_volume = sprintf("%s.backup", $volume);

    unlink ($STDout, $STDerr);
    $cmd = sprintf ("%s %s", $VOS_backup, $volume);
    `$cmd >$STDout 2>$STDerr`;	    
    if ($?) {
	;#
	;#  Unable to create the backup volume
	;#
	DisplayAndLogIt("CMD:  $cmd");
	DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to create the backup volume");
	$return_flag--;
    }
    else {
	;#
	;#  Do a vos examine on the newly created backup volume and stuff the record into %BK_volumes
	;#
	unlink ($STDout, $STDerr);
	$cmd = sprintf ("%s %s", $VOS_examine, $bk_volume);
	`$cmd >$STDout 2>$STDerr`;
	if ($?) {
	    ;#
	    ;#  Error from the vos examine on the backup volume
	    ;#
	    DisplayAndLogIt("CMD:  $cmd");
	    DisplayErrorAndLogIt("ERROR:   for volume $volume  -  unable to vos examine backup volume  $bk_volume");
	    $return_flag--;
	}
	else {
	    ;#
	    ;#  Read the output of the vos examine command, and stuff information about the backup volume into %BK_volumes
	    ;#
	    if (open(INPUT, $STDout)) {
		;#
		;#  Walk through the file looking for the needed information
		;#
		while ($line = <INPUT>) {
		    chomp($line);
		    $line  =~ s/\s{1,}/ /g;
		    if ($line =~ m/^name/i)            {  ($junk, $bk_volume) = split (/ /, $line);   next;  }
		    if ($line =~ m/^id/i)              {  ($junk, $bk_vid) = split (/ /, $line);   next;  }
		    if ($line =~ m/^serv/i)            {  ($junk, $junk2, $bk_server) = split (/ /, $line);   next;  }
		    if ($line =~ m/^part/i)            {  ($junk, $bk_partition) = split (/ /, $line);   $partition =~ s/\///;   next;  }
		    if ($line =~ m/^backupID/i)        {  ($junk, $bk_backup_vid) = split (/ /, $line);   next;  }
		    if ($line =~ m/^type/i)            {  ($junk, $bk_type) = split (/ /, $line);   next;  }
		    if ($line =~ m/^creationDate/i)    {  ($junk, $bk_volume_create_time) = split (/ /, $line);   next;  }
		    if ($line =~ m/^updateDate/i)      {  ($junk, $bk_volume_mod_time) = split (/ /, $line);   next;  }
		    if ($line =~ m/^diskused/i)        {  ($junk, $bk_volume_size) = split (/ /, $line);   next;  }
		    if ($line =~ m/^maxquota/i)        {  ($junk, $bk_volume_quota) = split (/ /, $line);   next;  }
		    if ($line =~ m/^filecount/i)       {  ($junk, $bk_number_of_files) = split (/ /, $line);   next;  }
		    if ($line =~ m/^dayUse/i)          {  ($junk, $bk_vnode_accesses) = split (/ /, $line);   next;  }
		}
		close(INPUT);
	    }
	    else {
		unlink ($STDout, $STDerr);
		DisplayErrorAndLogIt("ERROR:    for volume $volume  -  unable to read the vos examine output from the file $STDout:  $!");
		$return_flag--;
	    }
	}
    }

    ;#  Update %BK_volumes accordingly 
    if ($return_flag) {
	$line = sprintf ("%s %s %s %s %s %s %s %s %s %s %s %s", $bk_server, $bk_partition, $bk_volume, $bk_vid, $bk_backup_vid, $bk_type, 
                          $bk_volume_create_time, $bk_volume_mod_time, $bk_volume_size, $bk_volume_quota, $bk_number_of_files, $bk_vnode_accesses);
	$BK_volumes{$bk_volume} = $line;
    }
    else {
	delete $BK_volumes{$bk_volume};
    }

    return($return_flag);
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   Get_RW_VolumeInfo
;#
;#   Purpose:      To get the read/write volumes on the file servers
;#
;#        BEGIN_OF_ENTRY
;#        name            user.joe
;#        id              2038406616
;#        serv            129.74.223.44   curtis.helios.nd.edu
;#        part            /vicepa
;#        status          OK
;#        backupID        2038406618
;#        parentID        2038406616
;#        cloneID         0
;#        inUse           Y
;#        needsSalvaged   N
;#        destroyMe       N
;#        type            RW
;#        creationDate    1027614810      Thu Jul 25 11:33:30 2002
;#        accessDate      0               Wed Dec 31 19:00:00 1969
;#        updateDate      1050953601      Mon Apr 21 14:33:21 2003
;#        backupDate      1164002450      Mon Nov 20 01:00:50 2006
;#        copyDate        1163213266      Fri Nov 10 21:47:46 2006
;#        flags           0       (Optional)
;#        diskused        8217
;#        maxquota        500000
;#        minquota        0       (Optional)
;#        filecount       1026
;#        dayUse          1
;#        weekUse         1339    (Optional)
;#        spare2          0       (Optional)
;#        spare3          0       (Optional)
;#        END_OF_ENTRY
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   Get_RW_VolumeInfo  {

    my ($cmd, $line, $start_flag, $junk, $junk2);
    my ($queried_RW_volume_count, $server_RW_volume_count, $current_volumes_within_DB_counter, $old_volumes_within_DB_counter);

    my ($server, $partition, $volume, $vid, $backup_vid, $type);
    my ($volume_create_time, $volume_mod_time, $volume_size, $volume_quota, $number_of_files, $vnode_accesses);


    $queried_RW_volume_count = 0;
    foreach $server (@File_Servers) {
	DisplayAndLogIt("Query AFS file server: $server");
	unlink ($STDout, $STDerr);

	$cmd = sprintf ("%s %s", $VOS_listvol, $server);
	`$cmd >$STDout 2>$STDerr`;
	if ($?) {
	    DisplayAndLogIt("CMD:  $cmd");
	    DisplayErrorAndLogIt("Unable to list volumes on $server");
	}
	else {
	    ;#
	    ;#  Read the output from the vos listvol command
	    ;#
	    open(INPUT, $STDout) || do {
		unlink ($STDout, $STDerr);
		FatalError("Unable to read $STDout:  $!");
	    }
	    ;#
	    ;#  Walk through the file looking for the tags "BEGIN_OF_ENTRY"  and  "END_OF_ENTRY"
	    ;#
	    $server_RW_volume_count = 0;
	    $start_flag = 0;
	    while ($line = <INPUT>) {
		chomp($line);
		$line  =~ s/\s{1,}/ /g;
		if ($line =~ m/^BEGIN_OF_ENTRY/i)  {  $start_flag = 1;  next;  }
		if (!($start_flag))                {  next;  }

		if ($line =~ m/^name/i)            {  ($junk, $volume) = split (/ /, $line);   next;  }
		if ($line =~ m/^id/i)              {  ($junk, $vid) = split (/ /, $line);   next;  }
		if ($line =~ m/^serv/i)            {  ($junk, $junk2, $server) = split (/ /, $line);   next;  }
		if ($line =~ m/^part/i)            {  ($junk, $partition) = split (/ /, $line);   $partition =~ s/\///;   next;  }
		if ($line =~ m/^backupID/i)        {  ($junk, $backup_vid) = split (/ /, $line);   next;  }
		if ($line =~ m/^type/i)            {  ($junk, $type) = split (/ /, $line);   next;  }
		if ($line =~ m/^creationDate/i)    {  ($junk, $volume_create_time) = split (/ /, $line);   next;  }
		if ($line =~ m/^updateDate/i)      {  ($junk, $volume_mod_time) = split (/ /, $line);   next;  }
		if ($line =~ m/^diskused/i)        {  ($junk, $volume_size) = split (/ /, $line);   next;  }
		if ($line =~ m/^maxquota/i)        {  ($junk, $volume_quota) = split (/ /, $line);   next;  }
		if ($line =~ m/^filecount/i)       {  ($junk, $number_of_files) = split (/ /, $line);   next;  }
		if ($line =~ m/^dayUse/i)          {  ($junk, $vnode_accesses) = split (/ /, $line);   next;  }

		if ($line =~ m/^END_OF_ENTRY/i) {
		    $start_flag = 0;
		    $line = sprintf ("%s %s %s %s %s %s %s %s %s %s %s %s", $server, $partition, $volume, $vid, $backup_vid, $type, 
                                      $volume_create_time, $volume_mod_time, $volume_size, $volume_quota, $number_of_files, $vnode_accesses);
		    if ($type =~ m/RW/) {
			$RW_volumes{$volume} = $line;
			$server_RW_volume_count++;
		    }
		    elsif ($type =~ m/BK/) {
			$BK_volumes{$volume} = $line;
		    }
		}
	    }
	    DisplayAndLogIt("AFS file server: $server  has $server_RW_volume_count RW volumes");
	    $queried_RW_volume_count += $server_RW_volume_count;
	}
    }

    ;#
    ;#  Look for "restore" volumes, these volumes start with a unique prefix as defined by $Restore_Volume_PREFIX
    ;#  Add each one to the array @EXCLUDE_VOLUMES.  Unless the User has explicitly decided to create a full "$EXPLICIT_full_dump"
    ;#
    foreach $volume (sort(keys %RW_volumes)) {
	if ($volume =~ /^$Restore_Volume_PREFIX/ ) {
	    if (!($EXPLICIT_full_dump{$volume})) {
		push(@EXCLUDE_VOLUMES, $volume);
	    }
	}


	;# TMM Feb 12th 2008  just skip volumes that start with  "N."

	if ($volume =~ /^$Skip_Volume_PREFIX/ ) {
	    if (!($EXPLICIT_full_dump{$volume})) {
		push(@EXCLUDE_VOLUMES, $volume);
	    }
	}
    }

    ;#
    ;#  Run down the exclude volume list
    ;#
    foreach $volume (@EXCLUDE_VOLUMES) {
	if (exists $RW_volumes{$volume}) {
	    delete $RW_volumes{$volume};
	    delete $BK_volumes{$volume};
	    DisplayAndLogIt("Exclude the volume: $volume");
	}
    }


    ;#
    ;#  Compare the number of RW volumes that we have just queried from the AFS file servers
    ;#  to the the number of volumes within the VolumeNames table in the database.
    ;#
    $current_volumes_within_DB_counter = 0;
    $old_volumes_within_DB_counter = 0;

    if ($TH_VolumeNames)    {   $TH_VolumeNames->finish();   }
    $TH_VolumeNames = $DBH->prepare("SELECT * FROM VolumeNames");
    $TH_VolumeNames->execute();

    while (@row = $TH_VolumeNames->fetchrow_array()) {
	$volume = $row[0];
	$last_query_date = $row[1];
	$increment_status = $row[2];
	if ((($last_query_date + (4 * $ONE_DAY)))  >  $This_Dump_Date) {
	    $current_volumes_within_DB_counter++;
	}
	else {
	    $old_volumes_within_DB_counter++;
	} 
    }
    $TH_VolumeNames->finish();    $TH_VolumeNames = "";

    $junk = sprintf ("Queried  %d  RW volumes from the AFS file servers", $queried_RW_volume_count);
    DisplayAndLogIt("$junk");

    $junk = sprintf ("Database has  %d  current volumes, it also has  %d  volumes that have not been queried in the last 4 days",
		     $current_volumes_within_DB_counter, $old_volumes_within_DB_counter);
    DisplayAndLogIt("$junk");
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   GetFileServers
;#
;#   Purpose:      To get the list of file servers that will be queried for volumes to dump
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   GetFileServers  {

    my ($cmd, @list, @server_list, $server, $exclude_flag, $exclude, $found_flag, $req_server);

    unlink ($STDout, $STDerr);

    ;#  Disaster Recovery implementation requirement for Notre Dame, filter out the "DR" nodes as specified by naming convention
    ;#
    $cmd = sprintf ("%s listaddrs | %s -v -i \"dr-\"", $VOS, $GREP);
    `$cmd >$STDout 2>$STDerr`;
    if ($?) {
	DisplayAndLogIt("CMD:  $cmd");
	FatalError("Unable to list file servers in cell?");
    }

    ;#
    ;#  Read the output from the vos listaddrs command
    ;#
    open(INPUT, $STDout) || do {
	unlink ($STDout, $STDerr);
	FatalError("Unable to read $STDout:  $!");
    };

    @list = ();
    while (<INPUT>) {
	push(@list, $_);
    }
    close(INPUT);

    FatalError("No file servers found in cell?") unless $#list >= 0;
    unlink ($STDout, $STDerr);

    foreach $server (@list) {
	$server =~ s/\s.+//g;
	chomp($server);
	;#  Filter out the excluded file servers
	$exclude_flag = 0;
	foreach $exclude (@EXCLUDE_FILE_SERVERS) {
	    if ($server =~ m/^$exclude/) {
		$exclude_flag++;
		last;
	    }
	}
	if ($exclude_flag) {
	    DisplayAndLogIt("Skipping excluded AFS file server: $server");
	}
	else {
	    push(@server_list, $server);
	}
    }
    FatalError("All file servers excluded?") unless $#server_list >= 0;

    if (dnz(@EXPLICIT_FILE_SERVERS)) {
	foreach $req_server (@EXPLICIT_FILE_SERVERS) {
	    $found_flag = 0;
	    foreach $server (@server_list){
		if ($server  eq  $req_server) {
		    $found_flag++;
		    last;
		}
	    }

	    FatalError("Requested AFS file server: $req_server NOT FOUND or EXCLUDED. Aborting.") unless $found_flag;
	}
	@server_list = @EXPLICIT_FILE_SERVERS;
    }

    return(@server_list);
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   GetSubDirectoryPath
;#
;#   Purpose:      To figure out what the directory path is where the image file should be stored
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   GetSubDirectoryPath  {

    my ($volume) = @_;

    my (@dirparts, @letters, $subdir);
 
    @dirparts = split(/[\W_]/, $volume);
    if ($#dirparts > 0) {
	if ($dirparts[0]  eq  "user") {
	    @letters = split(//, $dirparts[1]);
	    $subdir = sprintf ("%s/%s/%s", $dirparts[0], $letters[0], $dirparts[1]);
	}
	else {
	    $subdir = sprintf ("%s/%s", $dirparts[0], $dirparts[1]);
	}
    }
    else {
	$subdir = $dirparts[0];
    }

    return($subdir);
}



;# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
;#   Subroutine:   FatalError
;#
;#   Purpose:      To handle the termination of a process upon a fatal error.
;#                 This function will write to the respective processes log file(s) and
;#                 exit the process and return a (+) 1 to the command shell
;#
;#                 As part of the output it will also read any messages in the standard error file $STDerr
;#                 and format them and display and log them as well
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

    ;#  Close the Backup data base
    if ($DBH) {
	if ($TH_Images)        {   $TH_Images->finish();   }
	if ($TH_DumpDates)     {   $TH_DumpDates->finish();   }
	if ($TH_VolumeNames)   {   $TH_VolumeNames->finish();   }
	if ($TH_History)       {   $TH_History->finish();   }
	$DBH->disconnect;
    }
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



;# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
;# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub   DisplayErrorAndLogIt  {

    my ($message) = @_;


    my ($package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask);

    my ($tag, $indent, $blanks);

	
    ($package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask) = caller(1);

    $subroutine = "main" unless dnz($subroutine);
    $subroutine =~ s/^main:://;

    $indent = IndentSpacing();
    $tag = sprintf("%s%s[%s]", GetTimeStamp(), $indent, $subroutine);

    $output = sprintf ("%s %s\n", $tag, $message);

    ;#
    ;#  If any text in $STDerr then display and log it as well,
    ;#  Also remove any carriage return characters [ hex: 0d  -->  "\r"   also referenced display as ^M
    ;#
    if ( -s $STDerr ) {
	$blanks = $tag;
        $blanks =~ tr/A-Za-z0-9_:-/ /;
        $blanks =~ tr/+/ /;
        $blanks =~ tr/[/ /;
        $blanks =~ tr/]/ /;

	foreach $line (`$CAT $STDerr | $TR -d '\r'`) {
	    $output = sprintf ("%s%s %s", $output, $blanks, $line);
	}
    }

    printf ("%s", $output);
    printf (LOG  "%s", $output);
    flush(LOG);
    unlink ($STDout, $STDerr);
}



;# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
;# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub   DisplayAndLogIt  {

    my ($message) = @_;


    my ($package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask);

    my ($tag, $indent);

	
    ($package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask) = caller(1);

    $subroutine = "main" unless dnz($subroutine);
    $subroutine =~ s/^main:://;

    $indent = IndentSpacing();
    $tag = sprintf("%s%s[%s]", GetTimeStamp(), $indent, $subroutine);

    printf ("%s %s\n", $tag, $message);
    printf (LOG  "%s %s\n", $tag, $message);
    flush(LOG);
}



;# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
;# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub   DisplayIt  {

    my ($message) = @_;

    my ($package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask);
    my ($tag, $indent);
	
    ($package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask) = caller(1);
    $subroutine = "main" unless dnz($subroutine);
    $subroutine =~ s/^main:://;
    $indent = IndentSpacing();
    $tag = sprintf("%s%s[%s]", GetTimeStamp(), $indent, $subroutine);

    printf ("%s %s\n", $tag, $message);
}



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

    if (dnz($opts{'D'})) {
	$DUMPDIR = $opts{'D'};
    }
    FatalError("DUMPDIR undefined") unless dnz($DUMPDIR);
    $DUMPDIR =~ s/\/+/\//g if dnz($DUMPDIR);
    $DUMPDIR =~ s/\/$//g if dnz($DUMPDIR);
    FatalError("DUMPDIR [$DUMPDIR] does not exist!") unless -d $DUMPDIR;

    if (dnz($opts{'E'})) {
	;# Do not dump volumes from these AFS file servers
	@EXCLUDE_FILE_SERVERS = split(/,/, $opts{'E'});
    }

    if (dnz($opts{'F'})) {
	;#  Dump volumes only from these AFS file servers
	@EXPLICIT_FILE_SERVERS = split(/,/, $opts{'F'});
    }

    if (dnz($opts{'V'})) {
	;#  Do NOT dump volumes in this list
	@EXCLUDE_VOLUMES = split(/,/, $opts{'V'});
    }


    if (dnz($opts{'f'})) {
	;#  This file has list of volumes to force a full dump (one volume per line)
	if (open(INPUT, $opts{'f'})) {
            $FORCE_full_dump_filename = $opts{'f'};
	    ;#
	    ;#  Walk through the file that contains the list of volume to force a full dump
	    ;#
	    while (<INPUT>) {
		split(//);
		if ($_[0]  eq  "#")    {   next;   }
		if ($_[0]  eq  ";")    {   next;   }
		if ($_[0]  eq  ",")    {   next;   }
		if ($_[0]  eq  ":")    {   next;   }
		s/\n//;
		tr/ / /s;
		s/^ //;

		@_ = split(/ /);
		if ($#_  ==  -1)    {   next;   }

		$volume = $_[0];

                ;#  Track the option to force a full dump.     For  $EXPLICIT_full_dump   a value of "0" indicates we have read the name from the input file
		$EXPLICIT_full_dump{$volume} = 0;
	    }
	    close(INPUT);
	}
	else {
	    FatalError("ERROR:  Unable to read $opts{'f'}   $!");
	}
    }




    if (dnz($opts{'K'})) {
	;#  This file has list of volumes that we do not want any of their corresponding backup files removed from the IMAGE archive
	if (open(INPUT, $opts{'K'})) {
            $KEEP_volumes_filename = $opts{'K'};
	    ;#
	    ;#  Walk through the file that contains the list of volumes that we do not want any of their backup files removed from the IMAGE archive
	    ;#
	    while (<INPUT>) {
		split(//);
		if ($_[0]  eq  "#")    {   next;   }
		if ($_[0]  eq  ";")    {   next;   }
		if ($_[0]  eq  ",")    {   next;   }
		if ($_[0]  eq  ":")    {   next;   }
		s/\n//;
		tr/ / /s;
		s/^ //;

		@_ = split(/ /);
		if ($#_  ==  -1)    {   next;   }

		$volume = $_[0];
		$KEEP_volumes{$volume} = 0;
	    }
	    close(INPUT);
	}
	else {
	    FatalError("ERROR:  Unable to read $opts{'K'}   $!");
	}
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

	print ("\t -D  [directory path]              Specify the dump directory [DUMPDIR]\n\n");
	print ("\t -E  [afs1.nd.edu, afs2.nd.edu]    AFS file servers to exclude \(Comma separated list, FQDN\)\n\n");
	print ("\t -F  [afs4.nd.edu, afs6.nd.edu]    Dump volumes only on these AFS file servers \(Comma sep list, FQDN\)\n\n");
	print ("\t -V  [user.tom, sys.sun4x_56]      Do NOT dump these volumes \(Comma sep list\)\n\n");
	print ("\t -f  [file name]                   Force full dumps on these volumes \(one volume per line\)\n\n");
	print ("\t -K  [file name]                   For the volumes listed in the file do not remove their\n");
        print ("\t                                   dump files from the IMAGE archive \(one volume per line\)\n\n");
	print ("\t -d      Debug mode\n");
	print ("\t -h      This Help\n");


	;# Print Current defaults


	print ("\n\n\n\t\t Defaults:\n");

	print "\t\t             DUMPDIR: $DUMPDIR\n"              if dnz($DUMPDIR);
	print "\t\tEXCLUDE_FILE_SERVERS: " . join(',', @EXCLUDE_FILE_SERVERS) . "\n" if @EXCLUDE_FILE_SERVERS;
	print "\t\tEXPLICIT_FILE_SERVERS: " . join(',', @EXPLICIT_FILE_SERVERS) . "\n" if @EXPLICIT_FILE_SERVERS;

	exit(0);
}



;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
;#   Subroutine:   CreateDataBase
;#
;#   Purpose:      To get the database laid out and populated with test data
;#
;# -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -   -
sub   CreateDataBase  {

    my ($backup_level, $last_query_date);
    my ($dump_date, $image_cnt, $image_cnt_fulls, $size_of_fulls, $size_of_images);
    my ($server, $partition, $volume, $vid, $backup_vid, $type);
    my ($volume_create_time, $volume_mod_time, $volume_size, $volume_quota, $number_of_files, $vnode_accesses);


    $backup_level = "0-0";                 ;#  indicate the backup level full: 0-0  
    $dump_date = $This_Dump_Date - (4 * $ONE_DAY);

    $image_cnt = 0;
    $image_cnt_fulls = 0;
    $size_of_images = 0;
    $size_of_fulls = 0;

    ;#
    ;#   If the the structure of the Images table is modified also update $SQL_insert_Images
    ;#
    DisplayAndLogIt("Create the  Images  table");
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
    DisplayAndLogIt("Create the  DumpDates  table");
    $DBH->do( "CREATE TABLE DumpDates (dump_date           INTEGER NOT NULL,
                                       number_of_images    INTEGER NOT NULL,
                                       number_of_fulls     INTEGER NOT NULL,
                                       size_of_fulls       INTEGER NOT NULL,
                                       total_size          INTEGER NOT NULL)" );


    ;#
    ;#   If the the structure of the VolumeNames table is modified also update $SQL_insert_VolumeNames
    ;#
    DisplayAndLogIt("Create the  VolumeNames  table");
    $DBH->do( "CREATE TABLE VolumeNames (volume            CHAR NOT NULL UNIQUE,
                                         last_query_date   INTEGER NOT NULL,
                                         increment_status  CHAR NOT NULL)" );



    ;#
    ;#   If the the structure of the History table is modified also update $SQL_insert_History
    ;#
    DisplayAndLogIt("Create the  History  table");
    $DBH->do( "CREATE TABLE History (volume                CHAR NOT NULL,
                                    volume_id              INTEGER NOT NULL,
                                    create_time            INTEGER NOT NULL,
                                    mod_time               INTEGER NOT NULL,
                                    image_size             INTEGER NOT NULL,
                                    backup_level           CHAR NOT NULL,
                                    dump_date              INTEGER NOT NULL,
                                    parent_dump_date       INTEGER NOT NULL,
                                    delete_date            INTEGER NOT NULL)" );



    ;#
    ;#  Create an index for the Images table using the volume column
    ;#
    DisplayAndLogIt("Index the Images table  -  by the volume name");
    $TH_Images = $DBH->prepare("CREATE INDEX Images_volume ON Images(volume)");
    $TH_Images->execute();
    $TH_Images->finish();
    $TH_Images = "";


    ;#
    ;#  Create an index for the History table using the volume column
    ;#
    DisplayAndLogIt("Index the History table  -  by the volume name");
    $TH_History = $DBH->prepare("CREATE INDEX History_volume ON History(volume)");
    $TH_History->execute();
    $TH_History->finish();
    $TH_History = "";





    ;#
    ;#  Seed the database with one record
    ;#

	$server              = "seed_server";
	$partition           = "vicepa";
	$volume              = "seed_volume";
	$vid                 = 111;
	$backup_vid          = 222;
	$type                = "RW";
	$volume_create_time  = 1163513403;
	$volume_mod_time     = 1163713403;
	$volume_size         = 333; 
	$volume_quota        = 444;
	$number_of_files     = 1;
	$vnode_accesses      = 1;
	;#
	;#  Initialize table "Images"
	;#
	$TH_Images = $DBH->prepare($SQL_insert_Images);
	$TH_Images->execute($volume, $vid, $volume_create_time, $volume_mod_time, $volume_size, $backup_level, $dump_date, $dump_date);

	;#
	;#  Initialize table "VolumeNames"
	;#
	$last_query_date = $dump_date;
	$TH_VolumeNames = $DBH->prepare($SQL_insert_VolumeNames);
        $TH_VolumeNames->execute($volume, $last_query_date, "normal");

	;#
	;#  Update table "DumpDates" with info about this dump
	;#
	$image_cnt = 1;
	$image_cnt_fulls = 1;
	$size_of_images = 333;
	$size_of_fulls = 333;

	$TH_DumpDates = $DBH->prepare($SQL_insert_DumpDates);
	$TH_DumpDates->execute($dump_date, $image_cnt, $image_cnt_fulls, $size_of_fulls, $size_of_images);
	return;
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


