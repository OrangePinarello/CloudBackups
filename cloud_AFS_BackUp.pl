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
;# Source Code:      sysadmin/Private/AFS/AFS_tools/AutoBackups/cloud_AFS_BackUp.pl
;#
;#
;# Local location:   /usr/bin/cloud_AFS_BackUp.pl
;#
;#
;# Propose:
;#
;#   This Perl program will run on the Unix server afsbk-cloud1.  This Perl program is invoked
;#   via cron and is a wrapper around the call to the Perl program  cloud_afsdumps.pl.  This is
;#   the program that determines which AFS volumes need to dumped and and what the dump level
;#   should be etc...
;#
;#   The Perl program  cloud_afsdumps.pl  will also farm out the actiual running of the vos dump
;#   command to these three Unix servers  afsbk-cloud1, afsbk-cloud2 and afsbkcloud3.  The output
;#   of these vos dumps are stored temporarily and the Perl program  cloud_afsdumps.pl  will then
;#   have these vos dump image files deduped and compress into an object store.
;#
;#   By farming out the vos dumps to other servers we can dump the AFS cell faster than using a
;#   single client.
;#
;#   
;#
;#
;# History:
;#
;#   Version 1.1     TMM   04/07/2015
;#
;#      Initial code drop
;#
;#      Note that this code draws on development from the older version AFS_BackUp.  Where the vos
;#      dump images where saved without modification on premise.
;#
;#
;#
;#
;#
;#=   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =



local($return_code);

$STDout="/tmp/AFSdump.stdout_$$";
$STDerr="/tmp/AFSdump.stderr_$$";




system ("/usr/bin/kinit -k opafsadm >$STDout 2>$STDerr");
$return_code = $? / 256;
if ($return_code != 0) {
    print "Error could not kinit for opafsadm\n";
}
else {
    ;#  TMM  problem with the -setpag option
    ;#      system ("/usr/bin/aklog -setpag >$STDout 2>$STDerr");
    system ("/usr/bin/aklog >$STDout 2>$STDerr");

    unlink ($STDout, $STDerr);
    system ("/afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/cloud_afsdump.pl -K /AFS_backups_in_Cloud/DailyDump/LOG/VOLUMES_to_KEEP -f /AFS_backups_in_Cloud/DailyDump/LOG/FORCE_full_dump > $STDout"); 

    ;#  Filter out expected output on standard output
    ;#
    `/bin/cat $STDout | /bin/egrep -v "\[Get_RW_VolumeInfo\] Query AFS file server|\[Get_RW_VolumeInfo\] AFS file server|\[Get_RW_VolumeInfo\] Exclude the volume|\[BuildDumpList\] Force FULL|\[ArchiveDumpedVolumes\] debug message|\[ArchiveDumpedVolumes\] Moving |\[BuildDumpList\] Query the Images table|\[BuildDumpList\] Examine each AFS|\[BuildDumpList\] Volume dump record|\[BuildDumpList\] Scheduled FULL dump|\[ListVolumes2Dump\] |\[CreateDumpCommands\] Display the|\[CreateDumpCommands\] /usr/sbin/vos |\[ArchiveDumpedVolumes\] CMD:  /bin/cp |\[Update_VolumeNames_last_query_date\] Update the last query date |\[Update_VolumeNames_last_query_date\] Update the last query date|\[Update_VolumeNames_last_query_date\] Update the last |\[RemoveImagesOfPurgedAFSvolumes\] As specified by the|\[RemoveOlderImages\] Query the Images table|\[RemoveOlderImages\] As specified by the |\[RemoveOlderImages\] The volume| incrementals over | incrementals exceed MAX size|\[RemoveOlderImages\] Add to the History|\[RemoveOlderImages\] Purge |\[RemoveOlderImages\] Moved meta file|\[RemoveOlderImages\] Delete from the|\[RemoveImagesOfPurgedAFSvolumes\] Look for dump images to remove|\[RemoveImagesOfPurgedAFSvolumes\] No dump images need to be PURGED|\[RemoveOlderImages\] Update the status of volumes moving to the next level in the backup schedule|\[RemoveOlderImages\] Begin to purge old vos dump files from the IMAGE archive" `; 


    system ("/afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/cloud_dumpdb.pl -q");


    system ("/usr/bin/unlog >$STDout 2>$STDerr");
    system ("/usr/bin/kdestroy >$STDout 2>$STDerr");
    system ("/bin/rm -f $STDout $STDerr");
}
