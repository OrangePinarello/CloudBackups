#!/bin/sh
#
#
#
#  Copyright (C) 2015 Terry McCoy     (terry@nd.edu)
# 
#   This program is free software; you can redistribute it and/or
#   modify it under the terms of the GNU General Public License 
#   as published by the Free Software Foundation; either version 2
#   of the License, or (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful, 
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License 
#   along with this program; if not, write to the Free Software
#   Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
# 
#
#
#
#   This software was designed and written by:
#
#       Terry McCoy                            (terry@nd.edu)
#       University of Notre Dame
#       Office of Information Technologies
#
#
#
# Source Code:      sysadmin/Private/AFS/AFS_tools/CloudBackups/StartVOSdumpSlicing.sh
#
#
# Local location:   
#
#
# Propose:
#
#   This is a wrapper shell script that is invoked from within the cron job:
#
#           /usr/sbin/cloud_AFS_BackUp.pl
#
#   It's purpose is to take the VOS dump files that have been created and slice
#   them up and store them away within an Object Store
#
#   
#
#
# History:
#
#   Version 1.1     TMM   11/16/2015
#
#      Initial code drop
#
#
#=   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =



DATE_STRING=`/bin/date +'%Y_%m_%d'`


if [ -f /AFS_backups_in_Cloud/DailyDump/LOG/afsdump_log_${DATE_STRING}__[0-2][0-9]:[0-5][0-9]:[0-5][0-9] ]
then
    LOG_FILE=`/bin/ls /AFS_backups_in_Cloud/DailyDump/LOG/afsdump_log_${DATE_STRING}__[0-2][0-9]:[0-5][0-9]:[0-5][0-9]`
    echo "The log file is:  ${LOG_FILE}"
    ADD_LIST=`echo $LOG_FILE | /bin/sed 's/\/afsdump_log_/\/ADD_list_/g'`
    if [ ! -f "$ADD_LIST" ]
    then
        /bin/grep " ADD " $LOG_FILE | /usr/bin/tr -s ' ' ' ' | /bin/cut -d':' -f7  >$ADD_LIST
        BASENAME=`/bin/basename $ADD_LIST`
        STDout="/AFS_backups_in_Cloud/Log/${BASENAME}.stdout"
        STDerr="/AFS_backups_in_Cloud/Log/${BASENAME}.stderr"
        SUMMARY="/AFS_backups_in_Cloud/Log/${BASENAME}.SUMMARY"
        if [ ! -f "$STDout" ]
        then
            if [ ! -f "$STDerr" ]
            then
                /AFS_backups_in_Cloud/bin/Begin_AFSbackup_Slice_and_Dice.py --file $ADD_LIST >$STDout 2>$STDerr

                if [ ! -s "$STDerr" ]
                then
                    NUMBER_EXPECTED=`/bin/cat $ADD_LIST | /usr/bin/wc -l`
                    NUMBER_PROCESS=`/bin/ls /AFS_backups_in_Cloud/Log | /bin/grep "^Slice_VOS_Dump_File__" | /bin/grep "__${DATE_STRING}_" | /usr/bin/wc -l`
                    if [ "$NUMBER_EXPECTED"  -eq   "$NUMBER_PROCESS" ]
                    then
                        #  Find out how may of the VOS dump files were successfully sliced and stored in the Object Store
                        /bin/ls /AFS_backups_in_Cloud/Log | /bin/grep "^Slice_VOS_Dump_File__" | /bin/grep "__${DATE_STRING}_" | while read SLICE_FNAME
                        do
                            /bin/grep "INFO  SUCCESS   All done..." /AFS_backups_in_Cloud/Log/${SLICE_FNAME} >/dev/null
                            if [ $?  -ne  0 ]
                            then
                                echo "Check for error within  ${SLICE_FNAME}"  >>${SUMMARY}
                                echo "Check for error within  ${SLICE_FNAME}"
                            else
                                /bin/rm -f /AFS_backups_in_Cloud/Log/${SLICE_FNAME}
                            fi
                        done

                        if [ -s "${SUMMARY}" ]
                        then
                            NUMBER_FAILED=`/bin/cat ${SUMMARY} | /usr/bin/wc -l`
                            echo "Had a total of  $NUMBER_FAILED  VOS dump files that did not get sliced and added tot he object store"  >>${SUMMARY}
                            echo "Had a total of  $NUMBER_FAILED  VOS dump files that did not get sliced and added tot he object store"
                        else 
                            #  Complete success rename standard output as the "success" file, then do some general cleanup
                            #  remove files that are not needed for any possible trouble shooting efforts
                            /bin/mv $STDout /AFS_backups_in_Cloud/Log/Success__${DATE_STRING}
                            /bin/rm -f $STDerr
                        fi
                    else
                        if [ "$NUMBER_EXPECTED"  -gt   "$NUMBER_PROCESS" ]
                        then
                            #  Find out which AFS VOS volume dumps did not get sliced
                            /bin/cat $ADD_LIST | /bin/grep -o '[^/]*$' | while read VOS_DUMP_FNAME
                            do
                                if [ ! -f /AFS_backups_in_Cloud/Log/Slice_VOS_Dump_File__${VOS_DUMP_FNAME} ]
                                then
                                    echo "Unable to find a slice log file for the vos dump  $VOS_DUMP_FNAME"  >>${SUMMARY}
                                    echo "Unable to find a slice log file for the vos dump  $VOS_DUMP_FNAME"
                                else
                                    #  If the VOS dump file did get sliced up, vaildate if the process was successful
                                    SLICE_FNAME="Slice_VOS_Dump_File__${VOS_DUMP_FNAME}"
                                    /bin/grep "INFO  SUCCESS   All done..." /AFS_backups_in_Cloud/Log/${SLICE_FNAME} >/dev/null
                                    if [ $?  -ne  0 ]
                                    then
                                        echo "Check for error within  ${SLICE_FNAME}"  >>${SUMMARY}
                                        echo "Check for error within  ${SLICE_FNAME}"
                                    else
                                        /bin/rm -f /AFS_backups_in_Cloud/Log/${SLICE_FNAME}
                                    fi
                                fi
                            done
                        else
                            echo "Fatal Error  the number of slice log files \($NUMBER_PROCESS\)  exceeds the number we were expecting \($NUMBER_EXPECTED\)"  >>${SUMMARY}
                            echo "Fatal Error  the number of slice log files \($NUMBER_PROCESS\)  exceeds the number we were expecting \($NUMBER_EXPECTED\)"
                        fi
                    fi
                else
                    echo ""
                    echo "     >>>>>     ERROR   showing contents of standard error"
                    echo ""
                    /bin/cat "$STDerr"
                fi
            else
                echo "ERROR the standard error file already exist   $STDerr"
            fi
        else
            echo "ERROR the standard output file already exist   $STDout"
        fi
    else
        echo "ERROR the add list file already exist   $ADD_LIST"
    fi
else
    echo "ERROR could not find the \"afsdump_log\" file for today"
fi


