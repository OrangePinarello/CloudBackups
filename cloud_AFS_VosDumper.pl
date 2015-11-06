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
;# Source Code:      sysadmin/Private/AFS/AFS_tools/CloudBackups/cloud_AFS_VosDumper.pl
;#
;#
;# Local location:   /usr/bin/cloud_AFS_VosDumper.pl
;#
;#
;# Propose:
;#
;#   This Perl program will run on these three Unix servers
;#
;#         master    afsbk-cloud1.cc.nd.edu
;#         slave     afsbk-cloud2.cc.nd.edu    
;#         slave     afsbk-cloud3.cc.nd.edu
;#
;#   This Perl program is invoked via cron and is a wrapper around the call to the
;#   Perl program  cloud_vosdumpers.pl.   
;#
;#
;#
;# Command Line Parameters:
;#
;#   This program takes a numerical value as its only command line parameter.  This vaule
;#   is used to specify which queue the Perl program  cloud_vosdumpers.pl  will be using.
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
;#=   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =




local($return_code);

$STDout="/tmp/AFSdump.stdout_$$";
$STDerr="/tmp/AFSdump.stderr_$$";


if (($ARGV[0] < 1)  ||  ($ARGV[0] > 6)) {
    printf ("Error the specified queue value [%s] not in expected range of 1- 6\n", $ARGV[0]);
    exit 1;
}

system ("/usr/bin/kinit -k opafsadm >$STDout 2>$STDerr");
$return_code = $? / 256;
if ($return_code != 0) {
    print "Error could not kinit for opafsadm\n";
}
else {
    ;#  TMM  problem with the -setpag option
    ;#      system ("/usr/bin/aklog -setpag >$STDout 2>$STDerr");
    system ("/usr/bin/aklog >$STDout 2>$STDerr");

    system ("/afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/cloud_vosdumpers.pl -q $ARGV[0]"); 
    system ("/usr/bin/unlog >$STDout 2>$STDerr");
    system ("/usr/bin/kdestroy >$STDout 2>$STDerr");
    system ("/bin/rm -f $STDout $STDerr");
}

