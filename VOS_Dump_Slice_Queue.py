#!/usr/bin/env python
#
#    $Header: /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/RCS/VOS_Dump_Slice_Queue.py,v 1.1 2015/07/20 19:09:02 root Exp $
#
#    $Revision: 1.1 $
#
#    $Date: 2015/07/20 19:09:02 $
#    $Locker:  $
#    $Author: root $
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
#
#   This software was designed and written by:
#
#       Terry McCoy                            (terry@nd.edu)
#       University of Notre Dame
#       Office of Information Technologies
#
#
#
# Source code location:     /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/VOS_Dump_Slice_Queue.py
#
# Local location:           /AFS_backups_in_Cloud/bin/VOS_Dump_Slice_Queue.py
#
#
# Propose:
#
#   This Python program will be directed by a calling control program  Begin_AFSbackup_Slice_and_Dice.py  to begin to
#   process a series of AFS vos dump files.
#
#
#
#
# Logic overview:
#
#   After this program has been started the calling control program  Begin_AFSbackup_Slice_and_Dice.py  communicates which
#   AFS vos dump files to process via command requests through PIPEd stdin.
#
#   Passing via stdin the following 3 data elements:
#
#        The full path to the vos dump file that needs to be processed
#        The path to the directory where the database of the slice information will be created
#        The size in bytes of the vos dump file

#
#
#   After this program has dispatched the vos dump file to to be slice and diced into the Object Store, it will communicate back
#   to the calling control program  Begin_AFSbackup_Slice_and_Dice.py  via a PIPEd stdout the results
#
#   Passing via stdout the following 3 data elements:
#
#          The status code for the process a zero is success
#          The path to the vos dump file that was processed
#          The path to the dirctory path where the database of the slice information was stored
#
#
#
# Command Line Parameters:
#
#
#
#
#
# History:
#
#   Version 0.x     TMM   04/22/2015   code development started
#
#   Version 1.1     TMM   mm/dd/2015
#
#        Completed
#
#
#
#=   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =

import os
import errno
import sys
import getopt
import hashlib
import base64
import zlib
import binascii
import time
import random
import select
import shutil

import subprocess
 





#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==


if __name__ == "__main__":


    # TMM  to enable debugging  (set to True)
    debug_on = False

    debug_on = True


    Slice_VOS_Dump_File =  '/AFS_backups_in_Cloud/bin/Slice_VOS_Dump_File.py'


    #  Defines the boundaries for the processing parameters (number of queues and the depth of the queue)
    value_10000mb  = 10000000000
    value_5000mb   = 5000000000
    value_1000mb   = 1000000000
    value_500mb    = 500000000
    value_100mb    = 100000000
    value_500mb    = 50000000
    value_10mb     = 10000000
    value_1mb      = 1000000

    path_list = os.path.split(str(sys.argv[0:1])[2:-2])
    program_name = str(path_list[1:2])[2:-3]
    short_program_name = program_name.replace('.py', '')
    my_pid = os.getpid()

    if debug_on:
        #  Since this program's  stdin and stdout is setup as a PIPE the only way
        #  to debug it is to log everything a debug log file in /tmp
        debug_log = '/tmp/' + short_program_name + '__' + str(my_pid) + '.debug_log'
        debug_log_fh = open(debug_log, "w")
        debug_log_fh.write('Starting...\n')
        debug_log_fh.flush()


    #  If there's input ready, then read in the input record (slice offset, slice number, slice length).  If there is nothing to read
    #  then sleep for one scond and try again. Note with the slect statement the timeout is zero so select won't block at all.
    #
    flag_keep_waiting = True

    while flag_keep_waiting:
        if debug_on:
            debug_msg = 'reading from stdin\n'
            debug_log_fh.write(debug_msg)
            debug_log_fh.flush()

        line = sys.stdin.readline()

        if debug_on:
            debug_msg = 'stdin contents:   ==>' + line + '<==\n'
            debug_log_fh.write(debug_msg)
            debug_log_fh.flush()

        if not line:
            if debug_on:
                debug_log_fh.write('empty read buffer...\n')
                debug_log_fh.flush()

            time.sleep(1.5)
        elif 'STOP' in line:
            if debug_on:
                debug_log_fh.write('Received STOP command...\n')
                debug_log_fh.flush()

            flag_keep_waiting = False
        else:
            line = line.rstrip('\r|\n')
            input_record = line.split(' :: ')
            vos_dump_file = input_record[0]
            slice_db_directory_path = input_record[1]
            size_of_dump_file = int(input_record[2])
            afs_volume_name = input_record[3]


            #  Based on the size of the vos dump file that is going to be processed; the number of queues that will be allocated
            #  and the depth of the queue will be tuned when the program  Slice_VOS_Dump_File.py  is envoked
            if size_of_dump_file  >  int(value_10000mb):
                number_of_queues = 8
                queue_depth = 5000
            elif size_of_dump_file  >  int(value_5000mb):
                number_of_queues = 6
                queue_depth = 4000
            elif size_of_dump_file  >  int(value_1000mb):
                number_of_queues = 5
                queue_depth = 1500
            elif size_of_dump_file  >  int(value_500mb):
                number_of_queues = 4
                queue_depth = 1000
            elif size_of_dump_file  >  int(value_100mb):
                number_of_queues = 3
                queue_depth = 800
            elif size_of_dump_file  >  int(value_10mb):
                number_of_queues = 3
                queue_depth = 400
            elif size_of_dump_file  >  int(value_1mb):
                number_of_queues = 2
                queue_depth = 400
            else:
                number_of_queues = 1
                queue_depth = 400               

 
            if not os.path.isfile(vos_dump_file):
                error_msg = 'ERROR   Unable to find the vos dump file:  ' + vos_dump_file + '\n'
                if debug_on:
                    debug_log_fh.write(error_msg)
                    debug_log_fh.flush()

                sys.stdout.write(error_msg)
                sys.stdout.flush()
                sys.exit(1)


            if debug_on:
                debug_log_fh.write('Found vos dump file: ' + str(vos_dump_file) + '\n')
                debug_log_fh.flush()

            path_list = os.path.split(vos_dump_file)
            vosdump_file_name = str(path_list[1:2])[2:-3]

            if debug_on:
                command_to_run = Slice_VOS_Dump_File + ' --file ' + vos_dump_file + ' --Meta --db ' + slice_db_directory_path + ' --queues ' + str(number_of_queues) + ' --depth ' + str(queue_depth)
                debug_msg = 'Call   ' + command_to_run + '\n'
                debug_log_fh.write(debug_msg)
                debug_log_fh.flush()

            #  Open up file handles to 2 files to be used as standard output and standard error  when the Slice_VOS_Dump_File.py  program is called
            path_stdout = '/tmp/' + short_program_name + '__' + vosdump_file_name + '__' + str(my_pid) + '.stdout'
            path_stderr = '/tmp/' + short_program_name + '__' + vosdump_file_name + '__' + str(my_pid) + '.stderr'
            fd_stdout = open(path_stdout, "w")
            fd_stderr = open(path_stderr, "w")


            try:
                slicer = subprocess.check_call([Slice_VOS_Dump_File, "--file" , vos_dump_file , "--Meta" , "--db" , slice_db_directory_path , "--queues" , str(number_of_queues) , "--depth" , str(queue_depth)], stdin=None, stdout=fd_stdout, stderr=fd_stderr, shell=False)
                fd_stdout.close()
                fd_stderr.close()
                dump_info = '0 :: ' + vos_dump_file + ' :: ' +  slice_db_directory_path + ' :: ' +  afs_volume_name + '\n'
                os.remove(path_stdout)
                os.remove(path_stderr)

            except subprocess.CalledProcessError:
                #  An error occurred then copy the stdout and stderr files into the temporary scratch directory for this processing node
                fd_stdout.close()
                fd_stderr.close()

                if debug_on:
                    debug_msg = 'A error was returned from  Slice_VOS_Dump_File.py'
                    debug_log_fh.write(debug_msg)
                    debug_log_fh.flush()

                fully_qualified_name = os.uname()[1]
                processing_node = fully_qualified_name.split(".")[0]
                temp_directory = '/AFS_backups_in_Cloud/Scratch/' + processing_node
                if not os.path.exists(temp_directory):
                    try:
                        os.makedirs(temp_directory)
                    except OSError as exception:
                        if exception.errno != errno.EEXIST:
                            raise

                shutil.move(path_stdout, temp_directory)
                shutil.move(path_stderr, temp_directory)

                dump_info = '1 :: ' + vos_dump_file + ' :: ' +  slice_db_directory_path + ' :: ' +  afs_volume_name + '\n'

            except OSError as exception:
                raise

            #  Write the result of running the program Slice_VOS_Dump_File.py to standard out.  Which is being read by the calling
            #  program  Begin_AFSbackup_Slice_and_Dice.py
            if debug_on:
                debug_msg = 'Write the result of calling  Slice_VOS_Dump_File.py  to stdout:  ==>' + dump_info + '<=='
                debug_log_fh.write(debug_msg)
                debug_log_fh.flush()

            sys.stdout.write(dump_info)
            sys.stdout.flush()

    if debug_on:
        debug_log_fh.close()


    sys.exit(0)
