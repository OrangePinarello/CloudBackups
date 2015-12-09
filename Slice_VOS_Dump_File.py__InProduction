#!/usr/bin/env python
#
#    $Header: /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/RCS/Slice_VOS_Dump_File.py,v 1.1 2015/07/20 18:55:11 root Exp $
#
#    $Revision: 1.1 $
#
#    $Date: 2015/07/20 18:55:11 $
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
# Source code location:     /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/Slice_VOS_Dump_File.py
#
# Local location:           /AFS_backups_in_Cloud/bin/Slice_VOS_Dump_File.py
#
#
# Propose:
#
#   This Python program will take a file, that is the output from the AFS "vos dump" process, and slice
#   it into multiple files.  These slice files will have checksum value calculated using 2 methods, these
#   checksum values will be used for lookup within the backup database and the ObjectStore (AWS S3).
#
#   These slice files will then be compressed using gzip before being stored within the Object Store (AWS S3)
#
#
#
# Logic overview:
#
#
#
#
# Command Line Parameters:
#
#   This program takes these additional optional parameters
#
#
#         --help
#
#         --file                    <full path to the vos dump file>
#
#         --meta       (optional)   <full path to the vos dump meta file>
#
#         --Meta       (optional)   Switch meta file is the vos dump file with ".meta" extendsion
#
#         --db         (optional)   <directory path to the location of the vos dump file's slice database>
#                                   Or as a default use the directory where the vos dump file is located
#
#         --salt                    <Do Not Forget this encryption string>
#
#         --queues     (optional)   <number of processing queues>      Default is 3 the value range is 1 - 8
#
#         --depth      (optional)   <depth of the processing queue>    Default is 200 the value range is 10 - 5000
#
#
#
#
#
# History:
#
#   Version 0.x     TMM   12/18/2014   code development started
#
#   Version 1.1     TMM   07/14/2015
#
#        Initail code drop, the program replaces the program  Slice_AFS_Volume.py.  It used version 1.7 as starting
#        point.  The number of processing queues and the depth of those queues were now defined at run time as command
#        line parameters.
#
#
#=   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =

import os
import errno
import sys
import getopt
import hashlib
import logging
import logging.handlers
import base64
import zlib
import binascii
import time
import subprocess
import random
import shutil

from datetime import datetime
from sys import argv

from subprocess import PIPE, Popen
from threading  import Thread

try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x


ON_POSIX = 'posix' in sys.builtin_module_names


#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

def  enqueue_output(out, queue):

    for line in iter(out.readline, b''):
        queue.put(line)
    out.close()




#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

def  ProcessCommandLine(argv, program_name):

    use_vos_dump_filename = False
    input_file_path = ''
    meta_file_path = ''
    db_dir_path = ''
    salt = ''
    queues = ''
    depth = ''

    help01 = '  --help\n'
    help02 = '  --file                    <full path to the vos dump file>\n\n'
    help03 = '  --meta       (optional)   <full path to the vos dump meta file>\n\n'
    help04 = '  --Meta       (optional)   Switch meta file is the vos dump file with ".meta" extendsion\n\n'
    help05 = '  --db         (optional)   <directory path to the location of the vos dump file\'s slice database>\n'
    help06 = '                            Or as a default use the directory where the vos dump file is located\n\n'
    help07 = '  --salt                    <Do Not Forget this encryption string>\n\n'
    help08 = '  --queues     (optional)   <number of processing queues>      Default is 3 the value range is 1 - 8\n\n'
    help09 = '  --depth      (optional)   <depth of the processing queue>    Default is 200 the value range is 10 - 5000\n\n'

    help_msg = help01 + help02 + help03 + help04 + help05 + help06 + help07 + help08 + help09

    try:
        opts, args = getopt.getopt(argv,"hMf:m:p:s:q:d:",["help","Meta","file=","meta=","db=","salt=","queues=","depth="])
    except getopt.GetoptError:
        print ' ' + program_name + '\n\n' + help_msg
        sys.exit(1)



    dryrun = False
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print ' ' + program_name + '\n\n' + help_msg
            sys.exit(0)
        elif opt in ("-M", "--Meta"):
            use_vos_dump_filename = True
        elif opt in ("-f", "--file"):
            input_file_path = arg
        elif opt in ("-m", "--meta"):
            meta_file_path = arg
        elif opt in ("-s", "--salt"):
            salt = arg
        elif opt in ("-q", "--queues"):
            queues = int(arg)
        elif opt in ("-d", "--depth"):
            depth = int(arg)
        elif opt in ("-p", "--db"):
            db_dir_path = arg        

    if not input_file_path:
        msg = 'Must specify the name of the vos dump file'
        print msg + '\n ' + program_name + '\n\n' + help_msg
        logger.critical(msg)
        sys.exit(1)

    if not meta_file_path:
        if use_vos_dump_filename:
            meta_file_path = input_file_path + '.meta'

    if not queues:
        queues = 3
    else:
        if queues < 1  or  queues > 8:
            msg = 'Number of queues must be from 1 to 8'
            print msg + '\n ' + program_name + '\n\n' + help_msg
            logger.critical(msg)
            sys.exit(1)           

    if not depth:
        depth = 200
    else:
        if depth < 10  or  depth > 5000:
            msg = 'Queue depth must be from 10 to 5000'
            print msg + '\n ' + program_name + '\n\n' + help_msg
            logger.critical(msg)
            sys.exit(1)

    if not db_dir_path:
        #  as a default the slice database is located where the vos dump file is
        db_dir_path = os.path.dirname(input_file_path)

    return(input_file_path, meta_file_path, db_dir_path, salt, queues, depth)




#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

def  StartMultipleSubProcesses(subprocess_name):

    global  debug_on
    global  input_file_path
    global  object_store
    global  sodium
    global  queue_count
    global  queue_depth
    global  list_of_slice_numbers
    global  list_of_slice_offsets
    global  list_of_slice_lengths
    global  list_of_slice_info


    
    global  spawn_CalculateSliceChecksum_processing_start_time
    global  spawn_CreateSlice_processing_start_time
    global  calculate_checksums_processing_start_time
    global  creating_slices_processing_start_time
    global  waiting_on_last_slices_to_calculate_processing_start_time
    global  waiting_on_last_slices_to_create_processing_start_time
    global  stop_CalculateChecksum_processing_start_time
    global  stop_CreateSlice_processing_start_time

    global  checksum_2_slice_numbers
    global  duplicate_checksum_keyed_by_slice_number
    global  unique_checksum_keyed_by_slice_number

    global  was_created_keyed_by_checksum
    global  was_created_keyed_by_slice_number
    global  file_annotation

    global  CalculateChecksum
    global  CreateSlice


    flag_CalculateChecksum = False
    flag_CreateSlice = False

    if subprocess_name  ==  'Calculate_Slice_Checksum':
        flag_CalculateChecksum = True
        spawn_CalculateSliceChecksum_processing_start_time = time.time()
        if debug_on:
            print 'Spawn the  Calculate_Slice_Checksum  subprocesses...\n'

    elif subprocess_name  ==  'Create_Slice':
        flag_CreateSlice = True
        spawn_CreateSlice_processing_start_time = time.time()
        if debug_on:
            print 'Spawn the  Create_Slice  subprocesses...\n'

    else:
        print 'ERROR:  ' + subprocess_name +  '  is an unknown subprocess name\n'
        sys.exit(1)

    proc1_write_count = 0
    proc2_write_count = 0
    proc3_write_count = 0
    proc4_write_count = 0
    proc5_write_count = 0
    proc6_write_count = 0
    proc7_write_count = 0
    proc8_write_count = 0

    flag_proc1_enabled = False
    flag_proc2_enabled = False
    flag_proc3_enabled = False
    flag_proc4_enabled = False
    flag_proc5_enabled = False
    flag_proc6_enabled = False
    flag_proc7_enabled = False
    flag_proc8_enabled = False

    number_of_slices_to_process = len(list_of_slice_numbers)

    if flag_CalculateChecksum:
        # if doing a checksum calculation force that process to be done with 8 threads and a queue depth of 3000
        subprocess_thread_count = 8
        write_queue_depth = 3000

        flag_proc1_enabled = True
        flag_proc2_enabled = True
        flag_proc3_enabled = True
        flag_proc4_enabled = True
        flag_proc5_enabled = True
        flag_proc6_enabled = True
        flag_proc7_enabled = True
        flag_proc8_enabled = True
    else:
        # Assign the number of threads and the depth of the processing queues
        subprocess_thread_count = queue_count
        write_queue_depth = queue_depth

        flag_proc1_enabled = True
        if subprocess_thread_count >= 2:
            flag_proc2_enabled = True
        if subprocess_thread_count >= 3:
            flag_proc3_enabled = True
        if subprocess_thread_count >= 4:
            flag_proc4_enabled = True
        if subprocess_thread_count >= 5:
            flag_proc5_enabled = True
        if subprocess_thread_count >= 6:
            flag_proc6_enabled = True
        if subprocess_thread_count >= 7:
            flag_proc7_enabled = True
            if subprocess_thread_count == 8:
                flag_proc8_enabled = True
            else:
                print 'ERROR:  To many threads (queues) are being specified the max value is 8\n'
                sys.exit(1)

    MAX_QUEUED = write_queue_depth * subprocess_thread_count


    #  Configure the number of threads (subprocess) that will be used slice up the vos dump image
    #
    if flag_proc1_enabled:
        if flag_CalculateChecksum:
            slicer_proc1 = Popen([CalculateChecksum , "--file" , input_file_path], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)
        elif flag_CreateSlice:
            slicer_proc1 = Popen([CreateSlice , "--file" , input_file_path , "--obj" , object_store , "--salt" , sodium], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)

        q_proc1 = Queue()
        t_proc1 = Thread(target=enqueue_output, args=(slicer_proc1.stdout, q_proc1))
        t_proc1.daemon = True
        t_proc1.start()

    if flag_proc2_enabled:
        if flag_CalculateChecksum:
            slicer_proc2 = Popen([CalculateChecksum , "--file" , input_file_path], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)
        elif flag_CreateSlice:
            slicer_proc2 = Popen([CreateSlice , "--file" , input_file_path , "--obj" , object_store , "--salt" , sodium], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)

        q_proc2 = Queue()
        t_proc2 = Thread(target=enqueue_output, args=(slicer_proc2.stdout, q_proc2))
        t_proc2.daemon = True
        t_proc2.start()

    if flag_proc3_enabled:
        if flag_CalculateChecksum:
            slicer_proc3 = Popen([CalculateChecksum , "--file" , input_file_path], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)
        elif flag_CreateSlice:
            slicer_proc3 = Popen([CreateSlice , "--file" , input_file_path , "--obj" , object_store , "--salt" , sodium], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)

        q_proc3 = Queue()
        t_proc3 = Thread(target=enqueue_output, args=(slicer_proc3.stdout, q_proc3))
        t_proc3.daemon = True
        t_proc3.start()

    if flag_proc4_enabled:
        if flag_CalculateChecksum:
            slicer_proc4 = Popen([CalculateChecksum , "--file" , input_file_path], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)
        elif flag_CreateSlice:
            slicer_proc4 = Popen([CreateSlice , "--file" , input_file_path , "--obj" , object_store , "--salt" , sodium], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)

        q_proc4 = Queue()
        t_proc4 = Thread(target=enqueue_output, args=(slicer_proc4.stdout, q_proc4))
        t_proc4.daemon = True
        t_proc4.start()

    if flag_proc5_enabled:
        if flag_CalculateChecksum:
            slicer_proc5 = Popen([CalculateChecksum , "--file" , input_file_path], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)
        elif flag_CreateSlice:
            slicer_proc5 = Popen([CreateSlice , "--file" , input_file_path , "--obj" , object_store , "--salt" , sodium], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)

        q_proc5 = Queue()
        t_proc5 = Thread(target=enqueue_output, args=(slicer_proc5.stdout, q_proc5))
        t_proc5.daemon = True
        t_proc5.start()

    if flag_proc6_enabled:
        if flag_CalculateChecksum:
            slicer_proc6 = Popen([CalculateChecksum , "--file" , input_file_path], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)
        elif flag_CreateSlice:
            slicer_proc6 = Popen([CreateSlice , "--file" , input_file_path , "--obj" , object_store , "--salt" , sodium], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)

        q_proc6 = Queue()
        t_proc6 = Thread(target=enqueue_output, args=(slicer_proc6.stdout, q_proc6))
        t_proc6.daemon = True
        t_proc6.start()

    if flag_proc7_enabled:
        if flag_CalculateChecksum:
            slicer_proc7 = Popen([CalculateChecksum , "--file" , input_file_path], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)
        elif flag_CreateSlice:
            slicer_proc7 = Popen([CreateSlice , "--file" , input_file_path , "--obj" , object_store , "--salt" , sodium], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)

        q_proc7 = Queue()
        t_proc7 = Thread(target=enqueue_output, args=(slicer_proc7.stdout, q_proc7))
        t_proc7.daemon = True
        t_proc7.start()

    if flag_proc8_enabled:
        if flag_CalculateChecksum:
            slicer_proc8 = Popen([CalculateChecksum , "--file" , input_file_path], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)
        elif flag_CreateSlice:
            slicer_proc8 = Popen([CreateSlice , "--file" , input_file_path , "--obj" , object_store , "--salt" , sodium], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = False)
 
        q_proc8 = Queue()
        t_proc8 = Thread(target=enqueue_output, args=(slicer_proc8.stdout, q_proc8))
        t_proc8.daemon = True
        t_proc8.start()


    current_write_count = 0
    list_of_records = []


    #  Now go and create the slices that have been identified as needing to be created  (dictionary:  create_this_slice)
    #
    #  After the slice file has been created then save its information  (dictionary:  was_created_keyed_by_slice_number)
    #
    #  Also identify new records to will need to be added to the All Slice database  (dictionary:  was_created_keyed_by_checksum)


    if flag_CalculateChecksum:
        calculate_checksums_processing_start_time = time.time()
        if debug_on:
            print 'Start calculating checksums for  ' + str(number_of_slices_to_process) + '  slices...\n'

    if flag_CreateSlice:
        creating_slices_processing_start_time = time.time()
        if debug_on:
            print 'Start creating  ' + str(number_of_slices_to_process) + '  slices...\n'

    while number_of_slices_to_process  >  0:
        if current_write_count  !=  MAX_QUEUED:
            #  Process is available to process the current slice
            send_slice_info = ''

            if flag_proc1_enabled:
                while number_of_slices_to_process  >  0  and  proc1_write_count < write_queue_depth  and  current_write_count  !=  MAX_QUEUED:
                    if flag_CalculateChecksum:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_offset = list_of_slice_offsets.pop(0)
                        slice_length = list_of_slice_lengths.pop(0)
                        send_slice_info = send_slice_info + str(slice_number) + ' :: ' +  str(slice_offset) + ' :: ' +  str(slice_length) + '\n'
                    elif flag_CreateSlice:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_info = list_of_slice_info.pop(0)
                        record = slice_info.split(' :: ')
                        checksum_key = record[0] + ' :: ' + record[1] + ' :: ' + record[2]           
                        send_slice_info = send_slice_info + str(slice_info) + ' :: ' +  str(slice_number) + '\n'

                    proc1_write_count += 1
                    current_write_count += 1
                    number_of_slices_to_process -= 1
                else:
                    if send_slice_info:
                        slicer_proc1.stdin.write(send_slice_info)
                        slicer_proc1.stdin.flush()
                        send_slice_info = ''

            if flag_proc2_enabled:
                while number_of_slices_to_process  >  0  and  proc2_write_count < write_queue_depth  and  current_write_count  !=  MAX_QUEUED:
                    if flag_CalculateChecksum:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_offset = list_of_slice_offsets.pop(0)
                        slice_length = list_of_slice_lengths.pop(0)
                        send_slice_info = send_slice_info + str(slice_number) + ' :: ' +  str(slice_offset) + ' :: ' +  str(slice_length) + '\n'
                    elif flag_CreateSlice:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_info = list_of_slice_info.pop(0)
                        record = slice_info.split(' :: ')
                        checksum_key = record[0] + ' :: ' + record[1] + ' :: ' + record[2]           
                        send_slice_info = send_slice_info + str(slice_info) + ' :: ' +  str(slice_number) + '\n'

                    proc2_write_count += 1
                    current_write_count += 1
                    number_of_slices_to_process -= 1
                else:
                    if send_slice_info:
                        slicer_proc2.stdin.write(send_slice_info)
                        slicer_proc2.stdin.flush()
                        send_slice_info = ''

            if flag_proc3_enabled:
                while number_of_slices_to_process  >  0  and  proc3_write_count < write_queue_depth  and  current_write_count  !=  MAX_QUEUED:
                    if flag_CalculateChecksum:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_offset = list_of_slice_offsets.pop(0)
                        slice_length = list_of_slice_lengths.pop(0)
                        send_slice_info = send_slice_info + str(slice_number) + ' :: ' +  str(slice_offset) + ' :: ' +  str(slice_length) + '\n'
                    elif flag_CreateSlice:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_info = list_of_slice_info.pop(0)
                        record = slice_info.split(' :: ')
                        checksum_key = record[0] + ' :: ' + record[1] + ' :: ' + record[2]           
                        send_slice_info = send_slice_info + str(slice_info) + ' :: ' +  str(slice_number) + '\n'

                    proc3_write_count += 1
                    current_write_count += 1
                    number_of_slices_to_process -= 1
                else:
                    if send_slice_info:
                        slicer_proc3.stdin.write(send_slice_info)
                        slicer_proc3.stdin.flush()
                        send_slice_info = ''

            if flag_proc4_enabled:
                while number_of_slices_to_process  >  0  and  proc4_write_count < write_queue_depth  and  current_write_count  !=  MAX_QUEUED:
                    if flag_CalculateChecksum:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_offset = list_of_slice_offsets.pop(0)
                        slice_length = list_of_slice_lengths.pop(0)
                        send_slice_info = send_slice_info + str(slice_number) + ' :: ' +  str(slice_offset) + ' :: ' +  str(slice_length) + '\n'
                    elif flag_CreateSlice:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_info = list_of_slice_info.pop(0)
                        record = slice_info.split(' :: ')
                        checksum_key = record[0] + ' :: ' + record[1] + ' :: ' + record[2]           
                        send_slice_info = send_slice_info + str(slice_info) + ' :: ' +  str(slice_number) + '\n'

                    proc4_write_count += 1
                    current_write_count += 1
                    number_of_slices_to_process -= 1
                else:
                    if send_slice_info:
                        slicer_proc4.stdin.write(send_slice_info)
                        slicer_proc4.stdin.flush()
                        send_slice_info = ''

            if flag_proc5_enabled:
                while number_of_slices_to_process  >  0  and  proc5_write_count < write_queue_depth  and  current_write_count  !=  MAX_QUEUED:
                    if flag_CalculateChecksum:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_offset = list_of_slice_offsets.pop(0)
                        slice_length = list_of_slice_lengths.pop(0)
                        send_slice_info = send_slice_info + str(slice_number) + ' :: ' +  str(slice_offset) + ' :: ' +  str(slice_length) + '\n'
                    elif flag_CreateSlice:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_info = list_of_slice_info.pop(0)
                        record = slice_info.split(' :: ')
                        checksum_key = record[0] + ' :: ' + record[1] + ' :: ' + record[2]           
                        send_slice_info = send_slice_info + str(slice_info) + ' :: ' +  str(slice_number) + '\n'

                    proc5_write_count += 1
                    current_write_count += 1
                    number_of_slices_to_process -= 1
                else:
                    if send_slice_info:
                        slicer_proc5.stdin.write(send_slice_info)
                        slicer_proc5.stdin.flush()
                        send_slice_info = ''

            if flag_proc6_enabled:
                while number_of_slices_to_process  >  0  and  proc6_write_count < write_queue_depth  and  current_write_count  !=  MAX_QUEUED:
                    if flag_CalculateChecksum:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_offset = list_of_slice_offsets.pop(0)
                        slice_length = list_of_slice_lengths.pop(0)
                        send_slice_info = send_slice_info + str(slice_number) + ' :: ' +  str(slice_offset) + ' :: ' +  str(slice_length) + '\n'
                    elif flag_CreateSlice:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_info = list_of_slice_info.pop(0)
                        record = slice_info.split(' :: ')
                        checksum_key = record[0] + ' :: ' + record[1] + ' :: ' + record[2]           
                        send_slice_info = send_slice_info + str(slice_info) + ' :: ' +  str(slice_number) + '\n'

                    proc6_write_count += 1
                    current_write_count += 1
                    number_of_slices_to_process -= 1
                else:
                    if send_slice_info:
                        slicer_proc6.stdin.write(send_slice_info)
                        slicer_proc6.stdin.flush()
                        send_slice_info = ''

            if flag_proc7_enabled:
                while number_of_slices_to_process  >  0  and  proc7_write_count < write_queue_depth  and  current_write_count  !=  MAX_QUEUED:
                    if flag_CalculateChecksum:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_offset = list_of_slice_offsets.pop(0)
                        slice_length = list_of_slice_lengths.pop(0)
                        send_slice_info = send_slice_info + str(slice_number) + ' :: ' +  str(slice_offset) + ' :: ' +  str(slice_length) + '\n'
                    elif flag_CreateSlice:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_info = list_of_slice_info.pop(0)
                        record = slice_info.split(' :: ')
                        checksum_key = record[0] + ' :: ' + record[1] + ' :: ' + record[2]           
                        send_slice_info = send_slice_info + str(slice_info) + ' :: ' +  str(slice_number) + '\n'

                    proc7_write_count += 1
                    current_write_count += 1
                    number_of_slices_to_process -= 1
                else:
                    if send_slice_info:
                        slicer_proc7.stdin.write(send_slice_info)
                        slicer_proc7.stdin.flush()
                        send_slice_info = ''

            if flag_proc8_enabled:
                while number_of_slices_to_process  >  0  and  proc8_write_count < write_queue_depth  and  current_write_count  !=  MAX_QUEUED:
                    if flag_CalculateChecksum:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_offset = list_of_slice_offsets.pop(0)
                        slice_length = list_of_slice_lengths.pop(0)
                        send_slice_info = send_slice_info + str(slice_number) + ' :: ' +  str(slice_offset) + ' :: ' +  str(slice_length) + '\n'
                    elif flag_CreateSlice:
                        slice_number = list_of_slice_numbers.pop(0)
                        slice_info = list_of_slice_info.pop(0)
                        record = slice_info.split(' :: ')
                        checksum_key = record[0] + ' :: ' + record[1] + ' :: ' + record[2]           
                        send_slice_info = send_slice_info + str(slice_info) + ' :: ' +  str(slice_number) + '\n'

                    proc8_write_count += 1
                    current_write_count += 1
                    number_of_slices_to_process -= 1
                else:
                    if send_slice_info:
                        slicer_proc8.stdin.write(send_slice_info)
                        slicer_proc8.stdin.flush()
                        send_slice_info = ''


            #  See if any database records are in the stack that need to be processed

            while len(list_of_records)  !=  0:
                returned_record = list_of_records.pop(0)
                record = returned_record.split(' :: ')

                if flag_CalculateChecksum:
                    sha1_value = record[0]
                    md5_value = record[1]
                    slice_file_size = int(record[2])
                    slice_offset = int(record[3])
                    slice_number = int(record[4])

                    #  Yes the checksum_key is going to be a very long string varying between 84 and 86 characters
                    #      SHA1 - 40 characters     MD5 - 32 characters     Slice length - between 4 and 6 digits     Two delimiters - 8 charactes
                    checksum_key = sha1_value + ' :: ' + md5_value + ' :: ' + str(slice_file_size)
                    temp_slice_record = checksum_key + ' :: ' + str(slice_offset)
                    slice_list = []

                    if checksum_2_slice_numbers.has_key(checksum_key):
                        duplicate_checksum_keyed_by_slice_number[int(slice_number)] = str(temp_slice_record)
                        slice_list = checksum_2_slice_numbers.get(checksum_key)
                    else:
                        unique_checksum_keyed_by_slice_number[int(slice_number)] = str(temp_slice_record)

                    slice_list.append(str(slice_number))
                    checksum_2_slice_numbers[checksum_key] = slice_list
                elif flag_CreateSlice:
                    sha1_value = record[0]
                    md5_value = record[1]
                    slice_file_size = int(record[2])
                    encrypted_file_size = int(record[3])
                    slice_file_block_cnt = int(record[4])
                    encrypted_file_block_cnt = int(record[5])
                    slice_offset = int(record[6])
                    slice_number = int(record[7])
                    status = record[8]
                    slice_name = record[9]

                    #  Create a record for the slice that was just processed to be placed into the database for this vos dump 
                    checksum_key = sha1_value + ' :: ' + md5_value + ' :: ' + str(slice_file_size)

                    if file_annotation.has_key(str(slice_offset)):
                        returned_record = returned_record + ' :: ' + str(file_annotation[str(slice_offset)])

                    was_created_keyed_by_slice_number[slice_number] = returned_record

                    #  Create the initial record within the all slice database for this newly created slice 
                    link_count = 1
                    part_1 = str(encrypted_file_size) + ' :: ' + str(slice_file_block_cnt) + ' :: ' + str(encrypted_file_block_cnt)
                    db_record = part_1 + ' :: ' + slice_name + ' :: ' + str(link_count)

                    was_created_keyed_by_checksum[checksum_key] = db_record


            #  Check on the subprocesses wait on them until they have something to read
            #
            flag_waiting_to_read = True
            previous_write_count = current_write_count

            while flag_waiting_to_read:
                if previous_write_count  !=  current_write_count:
                    #  One subprocess had something which we read, take one more pass thur to find any others
                    flag_waiting_to_read = False

                if flag_proc1_enabled:
                    while proc1_write_count != 0  and  current_write_count  !=  0:
                        ###  try:  returned_record = q_proc1.get_nowait()
                        try:  returned_record = q_proc1.get(timeout=0.1)
                        except Empty:
                            break
                        else:
                            if 'ERROR '  in returned_record:
                                # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                                print returned_record
                                sys.exit(1)
                            elif 'DEBUG '  in returned_record:
                                print returned_record
                            else:
                                #  Take the database record that was passed back from the subprocess  CreateSlice.py
                                #  and temporarily place it in a list for further processing
                                proc1_write_count -= 1
                                current_write_count -= 1
                                list_of_records.append(returned_record.rstrip('\r|\n'))

                    if current_write_count  ==  0:
                        #  All of the subprocess are waiting for more slices to process
                        flag_waiting_to_read = False
                        break

                if flag_proc2_enabled:
                    while proc2_write_count != 0  and  current_write_count  !=  0:
                        ###  try:  returned_record = q_proc2.get_nowait()
                        try:  returned_record = q_proc2.get(timeout=0.1)
                        except Empty:
                            break
                        else:
                            if 'ERROR '  in returned_record:
                                # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                                print returned_record
                                sys.exit(1)
                            elif 'DEBUG '  in returned_record:
                                print returned_record
                            else:
                                #  Take the database record that was passed back from the subprocess  CreateSlice.py
                                #  and temporarily place it in a list for further processing
                                proc2_write_count -= 1
                                current_write_count -= 1
                                list_of_records.append(returned_record.rstrip('\r|\n'))

                    if current_write_count  ==  0:
                        #  All of the subprocess are waiting for more slices to process
                        flag_waiting_to_read = False
                        break

                if flag_proc3_enabled:
                    while proc3_write_count != 0  and  current_write_count  !=  0:
                        ###  try:  returned_record = q_proc3.get_nowait()
                        try:  returned_record = q_proc3.get(timeout=0.1)
                        except Empty:
                            break
                        else:
                            if 'ERROR '  in returned_record:
                                # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                                print returned_record
                                sys.exit(1)
                            elif 'DEBUG '  in returned_record:
                                print returned_record
                            else:
                                #  Take the database record that was passed back from the subprocess  CreateSlice.py
                                #  and temporarily place it in a list for further processing
                                proc3_write_count -= 1
                                current_write_count -= 1
                                list_of_records.append(returned_record.rstrip('\r|\n'))

                    if current_write_count  ==  0:
                        #  All of the subprocess are waiting for more slices to process
                        flag_waiting_to_read = False
                        break

                if flag_proc4_enabled:
                    while proc4_write_count != 0  and  current_write_count  !=  0:
                        ###  try:  returned_record = q_proc4.get_nowait()
                        try:  returned_record = q_proc4.get(timeout=0.1)
                        except Empty:
                            break
                        else:
                            if 'ERROR '  in returned_record:
                                # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                                print returned_record
                                sys.exit(1)
                            elif 'DEBUG '  in returned_record:
                                print returned_record
                            else:
                                #  Take the database record that was passed back from the subprocess  CreateSlice.py
                                #  and temporarily place it in a list for further processing
                                proc4_write_count -= 1
                                current_write_count -= 1
                                list_of_records.append(returned_record.rstrip('\r|\n'))

                    if current_write_count  ==  0:
                        #  All of the subprocess are waiting for more slices to process
                        flag_waiting_to_read = False
                        break

                if flag_proc5_enabled:
                    while proc5_write_count != 0  and  current_write_count  !=  0:
                        ###  try:  returned_record = q_proc5.get_nowait()
                        try:  returned_record = q_proc5.get(timeout=0.1)
                        except Empty:
                            break
                        else:
                            if 'ERROR '  in returned_record:
                                # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                                print returned_record
                                sys.exit(1)
                            elif 'DEBUG '  in returned_record:
                                print returned_record
                            else:
                                #  Take the database record that was passed back from the subprocess  CreateSlice.py
                                #  and temporarily place it in a list for further processing
                                proc5_write_count -= 1
                                current_write_count -= 1
                                list_of_records.append(returned_record.rstrip('\r|\n'))

                    if current_write_count  ==  0:
                        #  All of the subprocess are waiting for more slices to process
                        flag_waiting_to_read = False
                        break

                if flag_proc6_enabled:
                    while proc6_write_count != 0  and  current_write_count  !=  0:
                        ###  try:  returned_record = q_proc6.get_nowait()
                        try:  returned_record = q_proc6.get(timeout=0.1)
                        except Empty:
                            break
                        else:
                            if 'ERROR '  in returned_record:
                                # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                                print returned_record
                                sys.exit(1)
                            elif 'DEBUG '  in returned_record:
                                print returned_record
                            else:
                                #  Take the database record that was passed back from the subprocess  CreateSlice.py
                                #  and temporarily place it in a list for further processing
                                proc6_write_count -= 1
                                current_write_count -= 1
                                list_of_records.append(returned_record.rstrip('\r|\n'))

                    if current_write_count  ==  0:
                        #  All of the subprocess are waiting for more slices to process
                        flag_waiting_to_read = False
                        break

                if flag_proc7_enabled:
                    while proc7_write_count != 0  and  current_write_count  !=  0:
                        ###  try:  returned_record = q_proc7.get_nowait()
                        try:  returned_record = q_proc7.get(timeout=0.1)
                        except Empty:
                            break
                        else:
                            if 'ERROR '  in returned_record:
                                # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                                print returned_record
                                sys.exit(1)
                            elif 'DEBUG '  in returned_record:
                                print returned_record
                            else:
                                #  Take the database record that was passed back from the subprocess  CreateSlice.py
                                #  and temporarily place it in a list for further processing
                                proc7_write_count -= 1
                                current_write_count -= 1
                                list_of_records.append(returned_record.rstrip('\r|\n'))

                    if current_write_count  ==  0:
                        #  All of the subprocess are waiting for more slices to process
                        flag_waiting_to_read = False
                        break

                if flag_proc8_enabled:
                    while proc8_write_count != 0  and  current_write_count  !=  0:
                        ### try:  returned_record = q_proc8.get_nowait()
                        try:  returned_record = q_proc8.get(timeout=0.1)
                        except Empty:
                            break
                        else:
                            if 'ERROR '  in returned_record:
                                # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                                print returned_record
                                sys.exit(1)
                            elif 'DEBUG '  in returned_record:
                                print returned_record
                            else:
                                #  Take the database record that was passed back from the subprocess  CreateSlice.py
                                #  and temporarily place it in a list for further processing
                                proc8_write_count -= 1
                                current_write_count -= 1
                                list_of_records.append(returned_record.rstrip('\r|\n'))

                    if current_write_count  ==  0:
                        #  All of the subprocess are waiting for more slices to process
                        flag_waiting_to_read = False
                        break


    #  Clean up at the end, now check on the subprocesses that are processing the last slices
    #
    if flag_CalculateChecksum:
        waiting_on_last_slices_to_calculate_processing_start_time = time.time()
        if debug_on:
            print 'Waiting on the checksum calculations of the last slices...\n'

    elif flag_CreateSlice:
        waiting_on_last_slices_to_create_processing_start_time = time.time()
        if debug_on:
            print 'Waiting on the last slices to be created...\n'

    if debug_on:
        remaining_to_process = proc1_write_count + proc2_write_count + proc3_write_count + proc4_write_count + proc5_write_count + proc6_write_count + proc7_write_count + proc8_write_count
        print 'Debug it:   current_write_count (' + str(current_write_count) + ') and the number of outstanding slices to be process (' + str(remaining_to_process) + ')\n'


    while current_write_count  >  0:
        if flag_proc1_enabled:
            while proc1_write_count != 0  and  current_write_count  !=  0:
                ###  try:  returned_record = q_proc1.get_nowait()
                try:  returned_record = q_proc1.get(timeout=0.1)
                except Empty:
                    break
                else:
                    if 'ERROR '  in returned_record:
                        # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                        print returned_record
                        sys.exit(1)
                    elif 'DEBUG '  in returned_record:
                        print returned_record
                    else:
                        #  Take the database record that was passed back from the subprocess  CreateSlice.py
                        #  and temporarily place it in a list for further processing
                        proc1_write_count -= 1
                        current_write_count -= 1
                        list_of_records.append(returned_record.rstrip('\r|\n'))

            if current_write_count  ==  0:
                #  All of the subprocess are waiting for more slices to process
                break

        if flag_proc2_enabled:
            while proc2_write_count != 0  and  current_write_count  !=  0:
                ###  try:  returned_record = q_proc2.get_nowait()
                try:  returned_record = q_proc2.get(timeout=0.1)
                except Empty:
                    break
                else:
                    if 'ERROR '  in returned_record:
                        # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                        print returned_record
                        sys.exit(1)
                    elif 'DEBUG '  in returned_record:
                        print returned_record
                    else:
                        #  Take the database record that was passed back from the subprocess  CreateSlice.py
                        #  and temporarily place it in a list for further processing
                        proc2_write_count -= 1
                        current_write_count -= 1
                        list_of_records.append(returned_record.rstrip('\r|\n'))

            if current_write_count  ==  0:
                #  All of the subprocess are waiting for more slices to process
                break

        if flag_proc3_enabled:
            while proc3_write_count != 0  and  current_write_count  !=  0:
                ###  try:  returned_record = q_proc3.get_nowait()
                try:  returned_record = q_proc3.get(timeout=0.1)
                except Empty:
                    break
                else:
                    if 'ERROR '  in returned_record:
                        # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                        print returned_record
                        sys.exit(1)
                    elif 'DEBUG '  in returned_record:
                        print returned_record
                    else:
                        #  Take the database record that was passed back from the subprocess  CreateSlice.py
                        #  and temporarily place it in a list for further processing
                        proc3_write_count -= 1
                        current_write_count -= 1
                        list_of_records.append(returned_record.rstrip('\r|\n'))

            if current_write_count  ==  0:
                #  All of the subprocess are waiting for more slices to process
                break

        if flag_proc4_enabled:
            while proc4_write_count != 0  and  current_write_count  !=  0:
                ###  try:  returned_record = q_proc4.get_nowait()
                try:  returned_record = q_proc4.get(timeout=0.1)
                except Empty:
                    break
                else:
                    if 'ERROR '  in returned_record:
                        # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                        print returned_record
                        sys.exit(1)
                    elif 'DEBUG '  in returned_record:
                        print returned_record
                    else:
                        #  Take the database record that was passed back from the subprocess  CreateSlice.py
                        #  and temporarily place it in a list for further processing
                        proc4_write_count -= 1
                        current_write_count -= 1
                        list_of_records.append(returned_record.rstrip('\r|\n'))

            if current_write_count  ==  0:
                #  All of the subprocess are waiting for more slices to process
                break

        if flag_proc5_enabled:
            while proc5_write_count != 0  and  current_write_count  !=  0:
                ###  try:  returned_record = q_proc5.get_nowait()
                try:  returned_record = q_proc5.get(timeout=0.1)
                except Empty:
                    break
                else:
                    if 'ERROR '  in returned_record:
                        # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                        print returned_record
                        sys.exit(1)
                    elif 'DEBUG '  in returned_record:
                        print returned_record
                    else:
                        #  Take the database record that was passed back from the subprocess  CreateSlice.py
                        #  and temporarily place it in a list for further processing
                        proc5_write_count -= 1
                        current_write_count -= 1
                        list_of_records.append(returned_record.rstrip('\r|\n'))

            if current_write_count  ==  0:
                #  All of the subprocess are waiting for more slices to process
                break

        if flag_proc6_enabled:
            while proc6_write_count != 0  and  current_write_count  !=  0:
                ###  try:  returned_record = q_proc6.get_nowait()
                try:  returned_record = q_proc6.get(timeout=0.1)
                except Empty:
                    break
                else:
                    if 'ERROR '  in returned_record:
                        # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                        print returned_record
                        sys.exit(1)
                    elif 'DEBUG '  in returned_record:
                        print returned_record
                    else:
                        #  Take the database record that was passed back from the subprocess  CreateSlice.py
                        #  and temporarily place it in a list for further processing
                        proc6_write_count -= 1
                        current_write_count -= 1
                        list_of_records.append(returned_record.rstrip('\r|\n'))

            if current_write_count  ==  0:
                #  All of the subprocess are waiting for more slices to process
                break

        if flag_proc7_enabled:
            while proc7_write_count != 0  and  current_write_count  !=  0:
                ###  try:  returned_record = q_proc7.get_nowait()
                try:  returned_record = q_proc7.get(timeout=0.1)
                except Empty:
                    break
                else:
                    if 'ERROR '  in returned_record:
                        # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                        print returned_record
                        sys.exit(1)
                    elif 'DEBUG '  in returned_record:
                        print returned_record
                    else:
                        #  Take the database record that was passed back from the subprocess  CreateSlice.py
                        #  and temporarily place it in a list for further processing
                        proc7_write_count -= 1
                        current_write_count -= 1
                        list_of_records.append(returned_record.rstrip('\r|\n'))

            if current_write_count  ==  0:
                #  All of the subprocess are waiting for more slices to process
                break

        if flag_proc8_enabled:
            while proc8_write_count != 0  and  current_write_count  !=  0:
                ###  try:  returned_record = q_proc8.get_nowait()
                try:  returned_record = q_proc8.get(timeout=0.1)
                except Empty:
                    break
                else:
                    if 'ERROR '  in returned_record:
                        # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                        print returned_record
                        sys.exit(1)
                    elif 'DEBUG '  in returned_record:
                        print returned_record
                    else:
                        #  Take the database record that was passed back from the subprocess  CreateSlice.py
                        #  and temporarily place it in a list for further processing
                        proc8_write_count -= 1
                        current_write_count -= 1
                        list_of_records.append(returned_record.rstrip('\r|\n'))

            if current_write_count  ==  0:
                #  All of the subprocess are waiting for more slices to process
                break

        if current_write_count  >  0:
            #  Verify that that there are still subprocess that we suspect still are processing the last slice(s)
            #  this is like a fail safe so we don't end up looping forever
            remaining_to_be_read = proc1_write_count + proc2_write_count + proc3_write_count + proc4_write_count + proc5_write_count + proc6_write_count + proc7_write_count + proc8_write_count
            if remaining_to_be_read  !=  current_write_count:
                print 'ERROR:  stuck in endless loop mismatch between  current_write_count (' + str(current_write_count) + ') and the number of outstanding slices to be read (' + str(remaining_to_be_read) + ')\n'
                sys.exit(1)
            else:
                #  Go sleep and and then check the subprocesses again
                time.sleep(0.2)


    #  Clean up at the end, process the last of the created database records in the stack
    #
    while len(list_of_records)  !=  0:
        returned_record = list_of_records.pop(0)
        record = returned_record.split(' :: ')

        if flag_CalculateChecksum:
            sha1_value = record[0]
            md5_value = record[1]
            slice_file_size = int(record[2])
            slice_offset = int(record[3])
            slice_number = int(record[4])

            #  Yes the checksum_key is going to be a very long string varying between 84 and 86 characters
            #      SHA1 - 40 characters     MD5 - 32 characters     Slice length - between 4 and 6 digits     Two delimiters - 8 charactes
            checksum_key = sha1_value + ' :: ' + md5_value + ' :: ' + str(slice_file_size)
            temp_slice_record = checksum_key + ' :: ' + str(slice_offset)
            slice_list = []

            if checksum_2_slice_numbers.has_key(checksum_key):
                duplicate_checksum_keyed_by_slice_number[int(slice_number)] = str(temp_slice_record)
                slice_list = checksum_2_slice_numbers.get(checksum_key)
            else:
                unique_checksum_keyed_by_slice_number[int(slice_number)] = str(temp_slice_record)

            slice_list.append(str(slice_number))
            checksum_2_slice_numbers[checksum_key] = slice_list
        elif flag_CreateSlice:
            sha1_value = record[0]
            md5_value = record[1]
            slice_file_size = int(record[2])
            encrypted_file_size = int(record[3])
            slice_file_block_cnt = int(record[4])
            encrypted_file_block_cnt = int(record[5])
            slice_offset = int(record[6])
            slice_number = int(record[7])
            status = record[8]
            slice_name = record[9]

            #  Create a record for the slice that was just processed to be placed into the database for this vos dump 
            checksum_key = sha1_value + ' :: ' + md5_value + ' :: ' + str(slice_file_size)

            if file_annotation.has_key(str(slice_offset)):
                returned_record = returned_record + ' :: ' + str(file_annotation[str(slice_offset)])

            was_created_keyed_by_slice_number[slice_number] = returned_record

            #  Create the initial record within the all slice database for this newly created slice 
            link_count = 1
            part_1 = str(encrypted_file_size) + ' :: ' + str(slice_file_block_cnt) + ' :: ' + str(encrypted_file_block_cnt)
            db_record = part_1 + ' :: ' + slice_name + ' :: ' + str(link_count)

            was_created_keyed_by_checksum[checksum_key] = db_record


    #  Send a STOP message to all the subprocesses
    #
    if flag_CalculateChecksum:
        stop_CalculateChecksum_processing_start_time = time.time()
        if debug_on:
            print 'Send STOP message to the  CalculateSliceChecksum  subprocesses...\n'        
    elif flag_CreateSlice:
        stop_CreateSlice_processing_start_time = time.time()
        if debug_on:
            print 'Send STOP message to the  CreateSlice  subprocesses...\n'

    stop_message = 'STOP\n'

    if flag_proc1_enabled:
        slicer_proc1.stdin.write(stop_message)
        slicer_proc1.stdin.flush()
    if flag_proc2_enabled:
        slicer_proc2.stdin.write(stop_message)
        slicer_proc2.stdin.flush()
    if flag_proc3_enabled:
        slicer_proc3.stdin.write(stop_message)
        slicer_proc3.stdin.flush()
    if flag_proc4_enabled:
        slicer_proc4.stdin.write(stop_message)
        slicer_proc4.stdin.flush()
    if flag_proc5_enabled:
        slicer_proc5.stdin.write(stop_message)
        slicer_proc5.stdin.flush()
    if flag_proc6_enabled:
        slicer_proc6.stdin.write(stop_message)
        slicer_proc6.stdin.flush()
    if flag_proc7_enabled:
        slicer_proc7.stdin.write(stop_message)
        slicer_proc7.stdin.flush()
    if flag_proc8_enabled:
        slicer_proc8.stdin.write(stop_message)
        slicer_proc8.stdin.flush()




#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

if __name__ == "__main__":


    main_processing_start_time = time.time()

    debug_on = False
    debug_level_1 = False    # available unused in the program
    debug_level_2 = False    # available unused in the program
    debug_level_3 = False    # available unused in the program
    debug_level_4 = False    # debug the reading of the slice info file
    debug_level_5 = False    # debug the reading of the vos dump meta data file   
    debug_CalculateSliceChecksum = False

    # External programs that are started via subprocess
    ParseVnodes        = '/AFS_backups_in_Cloud/bin/Vnode_Parsing_VOS_Dump_File.py'
    CalculateChecksum  = '/AFS_backups_in_Cloud/bin/Calculate_Slice_Checksum.py'
    CreateSlice        = '/AFS_backups_in_Cloud/bin/Create_Slice.py'

    object_store       = '/AFS_backups_in_Cloud/ObjectStore'

    path_list = os.path.split(str(sys.argv[0:1])[2:-2])
    program_name = str(path_list[1:2])[2:-3]
    short_program_name = program_name.replace('.py', '')

    #  Set up the logfile
    my_pid = os.getpid()
    logger = logging.getLogger(short_program_name)

    #  Set up the log files in the temporary scratch directory for this processing node
    fully_qualified_name = os.uname()[1]
    processing_node = fully_qualified_name.split(".")[0]
    temp_directory = '/AFS_backups_in_Cloud/Scratch/' + processing_node
    if not os.path.exists(temp_directory):
        try:
            os.makedirs(temp_directory)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    log_file_name = temp_directory + '/' + short_program_name + '__' + str(my_pid) + '.log'
    handler = logging.FileHandler(log_file_name, mode='w', encoding=None, delay=0)

    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler) 
    logger.setLevel(logging.INFO)

    input_file_path, meta_file_path, db_directory, sodium, queue_count, queue_depth = ProcessCommandLine(sys.argv[1:], program_name)

    if not os.path.isfile(input_file_path):
        msg = 'Unable to find the file:  ' + input_file_path
        logger.critical(msg)
        sys.exit(1)

    if meta_file_path:
        if not os.path.isfile(meta_file_path):
            msg = 'Unable to find the file:  ' + meta_file_path
            logger.critical(msg)
            sys.exit(1)



    # Possible expected input file formats:
    #
    #      /dumpinfo/AFS_backups_to_AWS/user.pharvey__2014_08_21_06:27__1-5
    #
    #      /dumpinfo/AFS_backups_to_AWS/user.pharvey__2014_08_21_06:27__1-5.meta
    #
    path_list = os.path.split(input_file_path)
    vosdump_file_name = str(path_list[1:2])[2:-3]

    msg = 'file name:  ' + vosdump_file_name
    logger.info(msg)
    if debug_on:
        print msg + '\n'

    #  Create a flat file database for each AFS vos dump file
    if not os.path.exists(db_directory):
        try:
            os.makedirs(db_directory)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    db_file_name = db_directory + '/' + 'DB__' + vosdump_file_name
    database_fh = open(db_file_name, 'w')

    afs_volume_name = str(vosdump_file_name.split('__')[0:1])[2:-2]
    time_stamp = str(vosdump_file_name.split('__')[1:2])[2:-2]
    dump_level = str(vosdump_file_name.split('__')[2:3])[2:-2]
    # concationate the timpe stang into a 12 character string (yyyymmddHHMM)
    year = str(time_stamp.split('_')[0:1])[2:-2]
    month = str(time_stamp.split('_')[1:2])[2:-2]
    date = str(time_stamp.split('_')[2:3])[2:-2]
    hh_mm = str(time_stamp.split('_')[3:4])[2:-2]
    HH = str(hh_mm.split(':')[0:1])[2:-2]
    MM = str(hh_mm.split(':')[1:2])[2:-2]

    slice_file_stub_name = afs_volume_name + '__' + year + '_' + month + '_' + date + '_' + HH + MM + '__' + dump_level
    slice_offset_file = temp_directory + '/' + slice_file_stub_name + '__slice_offsets'

    if debug_on:
        print 'AFS volume name:  ' + afs_volume_name + '\n'
        print 'Year: ' + year + '  Month: ' + month + '  Date: ' + date + '\n'
        print 'stub name:  ' + slice_file_stub_name + '\n'


    # Define the optimal size of the slice files to create (32KB --> 32768    128KB --> 131072)
    #   slice_buffer_size = 131072
    slice_buffer_size = 32768

    st = os.stat(input_file_path)
    file_size_in_bytes = st.st_size

    # figure out the number of disk blocks the AFS vos dump file is using
    disk_block_size = 1024
    quotient, remainder = divmod(file_size_in_bytes, disk_block_size)
    if int(remainder)  ==  0:
        total_blocks_uncompressed = int(quotient)
    else:
        total_blocks_uncompressed = int(quotient) + 1

    total_blocks_compressed = 0

    spawn_CalculateSliceChecksum_processing_start_time = time.time()
    spawn_CreateSlice_processing_start_time = time.time()
    calculate_checksums_processing_start_time = time.time()
    creating_slices_processing_start_time = time.time()
    waiting_on_last_slices_to_calculate_processing_start_time = time.time()
    waiting_on_last_slices_to_create_processing_start_time = time.time()
    stop_CalculateChecksum_processing_start_time = time.time()
    stop_CreateSlice_processing_start_time = time.time()


    #   This dictionary holds the names of the dumped files.  Note that theses names are taken from
    #   the corresponding meta file for the vos dump.
    #   It is keyed by the file count (as listed within the meta file)
    dumped_file_names = {}

    #   This dictionary is the companion of the  dumped_file_names  dictionary
    #   
    #   It is keyed by the file(s) byte offset location within the vos dump image
    file_annotation = {}

    ###   These dictionaries are keyed by the checksum_key   (sha1_value, md5_value, slice_length)

    #   This dictionary stores the mapping of the checksum_key to the slice number(s) where those keys are used
    checksum_2_slice_numbers = {}

    #   This dictionary is populate from the contents of the All Slice database (for this AFS volume)
    database_of_all_slices = {}

    #   This dictionary has the new slice(s) that need to be added to the All Slice database (for this AFS volume)
    was_created_keyed_by_checksum = {}

    #   This dictionary has the information about which slices need to be created.  Note that this is the same data
    #   as is stored in the dictionary  create_this_slice   Just using differnt look up keys
    need_to_create = {}


    ###   These dictionaries are keyed by the slice number

    #   These three dictionaries have five fields:
    #      The first 3 fields are lumped together and known as the checksum_key:   sha1_value, md5_value, slice_length
    #      The 4th and 5th fileds are store values for:  slice_offset  and  slice_length
    #
    #       ** Note:  Yes the record stores the length of the slice in the 3rd and 5th field
    #
    #   The dictionary  unique_checksum_keyed_by_slice_number  has the information about which slices are unique
    #   note that unique slice may already exist within the object store.
    #
    #   And the other dictionary  duplicate_checksum_keyed_by_slice_number  has the information about which
    #   slices do NOT need to be created because they are duplicates
    #
    #   Lastly the dictionary  create_this_slice  has the information about which slices need to be created
    unique_checksum_keyed_by_slice_number = {}
    duplicate_checksum_keyed_by_slice_number = {}
    create_this_slice = {}

    #   This dictionary stores information for duplicate slices
    duplicate_slices = {}

    #   This dictionary stores information for slices that already exist
    #   within the object store and do not need to be created again
    already_have_slice = {}

    #   This dictionary stores information for slices that have successfully been created
    was_created_keyed_by_slice_number = {}

   # Process the All Slices database populate the dictionary  database_of_all_slices  with its contents
    dbfile_all_slices = db_directory + '/DB_' + afs_volume_name + '__All_Slices'

    read_all_slice_db_processing_start_time = time.time()
    if os.path.exists(dbfile_all_slices):
        if debug_on:
            print 'Reading the All Slice Database...\n'
        fh_all_slices = open(dbfile_all_slices, 'r')

        for line in fh_all_slices:
            line = line.rstrip('\r|\n')
            # skip over the HEADER
            tokens = line.split()
            if 'HEADER:' != tokens[0]:
                all_slice_record = line.split(' :: ')
                #  The format of this record used within the All Slice database
                #
                #  The first 3 fields are used as a lookup key  (checksum_key)
                #
                #     SHA1 checksum of the uncompressed and unencrypted slice file
                #     MD5 checksum of the uncompressed and unencrypted slice file
                #     Slice file size in terms of bytes
                #
                #     Compressed and encrypted file size in terms of bytes
                #     Slice file in terms of 1KB blocks
                #     Compressed and encrypted file size in terms of 1KB blocks
                #
                #     --
                #     --
                #     --
                #
                #     The name of the slice file within the object store
                #
                #     Link count number of times this object (slice) is used within the databases for vos dump image files
                #
                sha1_value = all_slice_record[0]
                md5_value = all_slice_record[1]
                slice_file_size = all_slice_record[2]
                encrypted_file_size = int(all_slice_record[3])
                slice_file_block_cnt = int(all_slice_record[4])
                encrypted_file_block_cnt = int(all_slice_record[5])
                slice_name = all_slice_record[6]
                link_count = int(all_slice_record[7])

                checksum_key = sha1_value + ' :: ' + md5_value + ' :: ' + str(slice_file_size)

                # load the dictionary with the contents of the All Slices database  
                database_of_all_slices[checksum_key] = str(encrypted_file_size) + ' :: ' + str(slice_file_block_cnt) + ' :: ' + str(encrypted_file_block_cnt) + ' :: ' + slice_name + ' :: ' + str(link_count)

        fh_all_slices.close()


    meta_file_processing_start_time = time.time()
    if meta_file_path:
        if debug_on:
            print 'Reading meta file...\n'

        #  If we were supplied the vos dumps meta file than open if and extract the
        #  list of files within the vos dump
        meta_file_fh = open(meta_file_path, "r")
 
        skip_line = True
        file_count = 1
        for line in meta_file_fh:
            if skip_line:
                if debug_level_5:
                    print 'skipping: ' + line
                if 'Extracting files' in line:
                    skip_line = False
            else:
                line = line.rstrip('\r|\n')
                if len(line.strip()) == 0:
                    continue

                single_spaced_line = ' '.join(line.split())
                record = single_spaced_line.split(' ')
                if len(record)  <  8:
                    continue

                file_size = str(record[3])
                file_path = '/' + str(''.join(line.split('/', 1)[1:2]))

                dumped_file_names[str(file_count)] = file_path + ' : ' + file_size
                if debug_level_5:
                    print 'file number: ' + str(file_count) + '       name: ' + str(dumped_file_names[str(file_count)]) + '\n'
                file_count += 1



    parse_vnodes_processing_start_time = time.time()
    if debug_on:
        print 'Parsing Vnodes...\n'

    #  Call this program to walk through the VOS dump file and unroll all of the vnodes.
    #  Then package the vnodes into slices.  The product of this program will be an output
    #  file that has the all of the slices calculated.
    #
    #  Below is the format that the each line (slice) file will have.
    #
    #    slice number  :  offset where slice starts  :  length of the slice
    #
    subprocess.call([ParseVnodes, "--input" , input_file_path , "--output" , str(slice_offset_file) , "--size" , str(slice_buffer_size)])

    read_slice_offset_processing_start_time = time.time()
    if debug_on:
        print 'Reading the slice offsets and calculating there checksums...\n'

    #  Open the slice offset file for reading
    slice_info_file_fh = open(slice_offset_file, "r")

    #  Open the VOS dump file for reading 
    input_file_fh = open(input_file_path, "rb")
 
    list_of_slice_numbers = []
    list_of_slice_offsets = []
    list_of_slice_lengths = []

    file_index = 1
    for line in slice_info_file_fh:
        if 'ERROR' in line:
            print 'ERROR:  fatal error within  ' + ParseVnodes + '\n'
            print line
            sys.exit(1)

        line = line.rstrip('\r|\n')
        record = line.split(':')

        slice_number = int(record[0])
        slice_offset = int(record[1])
        slice_length = int(record[2])

        list_of_slice_numbers.append(slice_number)
        list_of_slice_offsets.append(slice_offset)
        list_of_slice_lengths.append(slice_length)

        if len(record)  ==  4:
            file_info = str(record[3])

            if debug_level_4:
                print 'record: ' + line + '       file index: ' + str(file_index) + '\n'

            if file_info  == 'SOF':
                file_annotation[str(slice_offset)] = 'Start of file : ' + str(dumped_file_names[str(file_index)])
                file_index += 1
            elif file_info  == 'EOF':
                file_annotation[str(slice_offset)] = 'EOF'
            elif file_info == 'SingleFile':
                file_annotation[str(slice_offset)] = 'File in slice : ' + str(dumped_file_names[str(file_index)])
                file_index += 1
            elif 'MultipleFiles' in file_info:
                multi_file_info = file_info.split(' ')
                file_count = int(multi_file_info[1])
                file_info = ''
                for counter in range(1, (file_count + 1)):
                    if counter == 1:
                        file_info = str(dumped_file_names[str(file_index)])
                    else:
                        file_info = file_info + ' ; ' + str(dumped_file_names[str(file_index)])

                    file_index += 1

                file_annotation[str(slice_offset)] = 'Multiple files in slice : ' + file_info

            elif 'StartMultipleFiles' in file_info:
                multi_file_info = file_info.split(' ')
                file_count = int(multi_file_info[1])
                file_info = ''
                for counter in range(1, (file_count + 1)):
                    if counter == 1:
                        file_info = str(dumped_file_names[str(file_index)])
                    else:
                        file_info = file_info + ' ; ' + str(dumped_file_names[str(file_index)])

                    file_index += 1

                file_annotation[str(slice_offset)] = 'Start of multiple files : ' + file_info
            else:
                msg = 'Unknown file info:  ' + file_info
                logger.critical(msg)
                sys.exit(1)


    StartMultipleSubProcesses('Calculate_Slice_Checksum')


    dumped_file_names = {}
    checksum_2_slice_numbers = {}
    slice_info_file_fh.close()
    number_of_slices = slice_number


    #  For the unique slices see if any of them already exist within the object store
    #
    #  Compare the slices checksum_key to those within the All Slice database  (dictionary:  database_of_all_slices)
    #
    #  If a slice already exists then save its information in the dictionary  already_have_slice
    #
    #     Then also update the  link_count  field in the slices All Slice database record  (dictionary:  database_of_all_slices)

    dedupe_unique_processing_start_time = time.time()
    if debug_on:
        print 'Deduping unique slices...\n'

    for slice_number in range(1, (number_of_slices + 1)):
        if unique_checksum_keyed_by_slice_number.has_key(slice_number):
            line = unique_checksum_keyed_by_slice_number.get(slice_number)
            unique_record = line.split(' :: ')

            checksum_key = unique_record[0] + ' :: ' + unique_record[1] + ' :: ' + unique_record[2]
            slice_length = int(unique_record[2])
            slice_offset = int(unique_record[3])

            if database_of_all_slices.has_key(checksum_key):
                line = database_of_all_slices.get(checksum_key)
                all_slice_record = line.split(' :: ')
                #  The format of this record used within the All Slice database
                #
                #  The first 3 fields are used as a lookup key  (checksum_key)
                #
                #     --   SHA1 checksum of the uncompressed and unencrypted slice file
                #     --   MD5 checksum of the uncompressed and unencrypted slice file
                #     --   Slice file size in terms of bytes
                #
                #     Compressed and encrypted file size in terms of bytes
                #     Slice file in terms of 1KB blocks
                #     Compressed and encrypted file size in terms of 1KB blocks
                #
                #     The name of the slice file within the object store
                #     Link count number of times this object (slice) is used within the databases for vos dump image files
                #
                encrypted_file_size        = int(all_slice_record[0])
                slice_file_block_cnt       = int(all_slice_record[1])
                encrypted_file_block_cnt   = int(all_slice_record[2])
                slice_name                 = all_slice_record[3]
                link_count                 = int(all_slice_record[4])

                #  Update the link count then update the record within in the All Slice database
                link_count += 1
                
                updated_all_slice_record = all_slice_record[0] + ' :: ' + all_slice_record[1] + ' :: ' + all_slice_record[2] + ' :: ' + all_slice_record[3] + ' :: ' + str(link_count)
                
                database_of_all_slices[checksum_key] = updated_all_slice_record


                # Now recorded this slice information in the  already_have_slice  dictionary
                # 
                #    Note this slice already exists within the object store as matched by its checksum_key
                #    therefore we do not need to read it from the vos dump image, compress it and encrypted it
                status = 'Deduped'

                part_1 = checksum_key + ' :: ' + str(encrypted_file_size) + ' :: ' + str(slice_file_block_cnt) + ' :: ' + str(encrypted_file_block_cnt)
                db_record = part_1 + ' :: ' + str(slice_offset) + ' :: ' + str(slice_number) + ' :: ' + status + ' :: ' + slice_name

                if file_annotation.has_key(str(slice_offset)):
                    db_record = db_record + ' :: ' + str(file_annotation[str(slice_offset)])

                already_have_slice[slice_number] = db_record
            else:
                if not need_to_create.has_key(checksum_key):
                    need_to_create[checksum_key] = str(slice_number)
                    create_this_slice[slice_number] = checksum_key + ' :: ' + str(slice_offset)
                else:
                    print 'ERROR:  Suppose to be an unique slice   should not be listed as needing to be created again\n'
                    sys.exit(1)


    unique_checksum_keyed_by_slice_number = {}

    #  For the duplicate slices see if any of them already exist within the object store
    #
    #  Compare the slices checksum_key to those within the All Slice database  (dictionary:  database_of_all_slices)
    #
    #  If a slice already exists then save its information in the dictionary  already_have_slice
    #
    #     Then also update the  link_count  field in the slices All Slice database record  (dictionary:  database_of_all_slices)

    dedupe_duplicate_processing_start_time = time.time()
    if debug_on:
        print 'Deduping the duplicate slices...\n'

    for slice_number in range(1, (number_of_slices + 1)):
        if duplicate_checksum_keyed_by_slice_number.has_key(slice_number):
            line = duplicate_checksum_keyed_by_slice_number.get(slice_number)
            duplicate_record = line.split(' :: ')

            checksum_key = duplicate_record[0] + ' :: ' + duplicate_record[1] + ' :: ' + duplicate_record[2]
            slice_length = int(duplicate_record[2])
            slice_offset = int(duplicate_record[3])

            if database_of_all_slices.has_key(checksum_key):
                line = database_of_all_slices.get(checksum_key)
                all_slice_record = line.split(' :: ')

                encrypted_file_size        = int(all_slice_record[0])
                slice_file_block_cnt       = int(all_slice_record[1])
                encrypted_file_block_cnt   = int(all_slice_record[2])
                slice_name                 = all_slice_record[3]
                link_count                 = int(all_slice_record[4])

                #  Update the link count then update the record within in the All Slice database
                link_count += 1
                
                updated_all_slice_record = all_slice_record[0] + ' :: ' + all_slice_record[1] + ' :: ' + all_slice_record[2] + ' :: ' + all_slice_record[3] + ' :: ' + str(link_count)
                
                database_of_all_slices[checksum_key] = updated_all_slice_record

                # Now recorded this slice information in the  already_have_slice  dictionary
                # 
                #    Note this slice already exists within the object store as matched by its checksum_key
                #    therefore we do not need to read it from the vos dump image, compress it and encrypted it

                status = 'DUP'

                part_1 = checksum_key + ' :: ' + str(encrypted_file_size) + ' :: ' + str(slice_file_block_cnt) + ' :: ' + str(encrypted_file_block_cnt)
                db_record = part_1 + ' :: ' + str(slice_offset) + ' :: ' + str(slice_number) + ' :: ' + status + ' :: ' + slice_name

                if file_annotation.has_key(str(slice_offset)):
                    db_record = db_record + ' :: ' + str(file_annotation[str(slice_offset)])

                already_have_slice[slice_number] = db_record
            else:
                if not need_to_create.has_key(checksum_key):
                    print 'ERROR:  Suppose to be a duplicate slice   so it should already be listed as needing to be created\n'
                    sys.exit(1)

    need_to_create = {}


    load_two_lists_processing_start_time = time.time()
    if debug_on:
        print 'Start unloading  create_this_slice  into two lists...\n'

    list_of_slice_numbers = []
    list_of_slice_info = []

    for slice_number, slice_info in create_this_slice.iteritems():
        if was_created_keyed_by_slice_number.has_key(slice_number):
            print 'ERROR:  The slice should not have already been created\n'
            sys.exit(1)

        if was_created_keyed_by_checksum.has_key(checksum_key):
            print 'ERROR:  Slice with this key should not have already been created\n'
            sys.exit(1)

        list_of_slice_numbers.append(slice_number)
        list_of_slice_info.append(slice_info)

    create_this_slice = {}


    StartMultipleSubProcesses('Create_Slice')


    #  For the duplicate slices that have NOT already been identified within the dictionary  already_have_slice
    #
    #  Ensure that they appear within the dictionary  was_created_keyed_by_checksum
    #
    #  From which we can create a record to save in the dictionary  duplicate_slices
    #
    verify_duplicate_slices_processing_start_time = time.time()
    if debug_on:
        print 'Verify duplicate slices...\n'

    for slice_number in range(1, (number_of_slices + 1)):
        if duplicate_checksum_keyed_by_slice_number.has_key(slice_number):
            line = duplicate_checksum_keyed_by_slice_number.get(slice_number)
            duplicate_record = line.split(' :: ')

            checksum_key = duplicate_record[0] + ' :: ' + duplicate_record[1] + ' :: ' + duplicate_record[2]
            slice_length = int(duplicate_record[2])
            slice_offset = int(duplicate_record[3])
            

            if database_of_all_slices.has_key(checksum_key):
                if not already_have_slice.has_key(slice_number):
                    print 'ERROR:  The checksum_key for this duplicate slice exists within  database_of_all_slices    but not within  already_have_slice\n'
                    sys.exit(1)
            else:
                if already_have_slice.has_key(slice_number):
                    print 'ERROR:  Duplicate slice exists within  already_have_slice    but not within  database_of_all_slices\n'
                    sys.exit(1)

                #  So the duplicate must exist within the dictionary  was_created_keyed_by_checksum
                if not was_created_keyed_by_checksum.has_key(checksum_key):
                    print 'ERROR:  The checksum_key for this duplicate slice does not exist within the dictionary  was_created_keyed_by_checksum'
                    sys.exit(1)

                line = was_created_keyed_by_checksum.get(checksum_key)
                all_slice_record = line.split(' :: ')

                encrypted_file_size        = int(all_slice_record[0])
                slice_file_block_cnt       = int(all_slice_record[1])
                encrypted_file_block_cnt   = int(all_slice_record[2])
                slice_name                 = all_slice_record[3]
                link_count                 = int(all_slice_record[4])

                #  Update the link count then update the record within in the All Slice database
                link_count += 1
                
                updated_all_slice_record = all_slice_record[0] + ' :: ' + all_slice_record[1] + ' :: ' + all_slice_record[2] + ' :: ' + all_slice_record[3] + ' :: ' + str(link_count)
                
                was_created_keyed_by_checksum[checksum_key] = updated_all_slice_record

                # Now recorded this slice information in the  duplicate_slices  dictionary
                # 
                #    Note this slice was just ceated during this dump
                #    therefore we do not need to read it from the vos dump image, compress it and encrypted it

                status = 'DUP'

                part_1 = checksum_key + ' :: ' + str(encrypted_file_size) + ' :: ' + str(slice_file_block_cnt) + ' :: ' + str(encrypted_file_block_cnt)

                db_record = part_1 + ' :: ' + str(slice_offset) + ' :: ' + str(slice_number) + ' :: ' + status + ' :: ' + slice_name

                if file_annotation.has_key(str(slice_offset)):
                    db_record = db_record + ' :: ' + str(file_annotation[str(slice_offset)])

                duplicate_slices[slice_number] = db_record



    #  Now combined the slice information from these dictionaries:
    #
    #         was_created_keyed_by_slice_number
    #         already_have_slice
    #         duplicate_slices       (only those that were created there maybe be duplicate slices within  already_have_slice)
    #
    #  The first for loop verifies that all the slices exist and that the dictionaries are not corrupted
    #
    #  The second for loop then does the processing
    #      Track the utilization counters for this vos dump image
    #          uncompressed blocks used verse the number of compressed blocks
    #          the deduplicated storage savings 
    #
    #      Create the database for this vos dump image file
    #
    verify_slice_info_processing_start_time = time.time()
    if debug_on:
        print 'Verify all of the slices information...\n'

    total_blocks_compressed = 0
    total_blocks_uncompressed = 0

    unique_blocks_compressed = 0
    unique_blocks_uncompressed = 0
    unique_slice_counter = 0
    duplicate_blocks_compressed = 0
    duplicate_blocks_uncompressed = 0
    duplicate_slice_counter = 0
    deduped_blocks_compressed = 0
    deduped_blocks_uncompressed = 0
    deduped_slice_counter = 0

    for slice_number in range(1, (number_of_slices + 1)):
        #  Check that the slice number eixsts and that it exist in only 1 of the 3 dictionaries
        if not was_created_keyed_by_slice_number.has_key(slice_number):
            if not already_have_slice.has_key(slice_number):
                if not duplicate_slices.has_key(slice_number):
                    print 'ERROR:  No information for slice number: ' + str(slice_number) + '\n'
                    sys.exit(1)
            else:
                if duplicate_slices.has_key(slice_number):
                    print 'ERROR:  For slice number: ' + str(slice_number) + '   entries in both  already_have_slice   and   duplicate_slices\n'
                    sys.exit(1)
        else:
            if already_have_slice.has_key(slice_number):
                if duplicate_slices.has_key(slice_number):
                    print 'ERROR:  For slice number: ' + str(slice_number) + '   entries in all 3 dictionaries\n'
                    sys.exit(1)
                else:
                    print 'ERROR:  For slice number: ' + str(slice_number) + '   entries in both  was_created_keyed_by_slice_number   and   already_have_slice\n'
                    sys.exit(1)
            else:
                if duplicate_slices.has_key(slice_number):
                    print 'ERROR:  For slice number: ' + str(slice_number) + '   entries in both  was_created_keyed_by_slice_number   and   duplicate_slices\n'
                    sys.exit(1)

        # Going to do the calculations to track the utilization
        # So that they can be posted in the header of the slice database for this vos dump image file
        if was_created_keyed_by_slice_number.has_key(slice_number):
            line = was_created_keyed_by_slice_number.get(slice_number)
            slice_record = line.split(' :: ')

            checksum_key                = slice_record[0] + ' :: ' + slice_record[1] + ' :: ' + slice_record[2]
            slice_file_block_cnt        = int(slice_record[4])
            encrypted_file_block_cnt    = int(slice_record[5])
            record_slice_number         = int(slice_record[7])
            status                      = slice_record[8]

            if record_slice_number  !=  slice_number:
                print 'ERROR:  Slice number mismatch between the key used in  was_created_keyed_by_slice_number  and the value stored in the record\n'
                print 'ERROR:  slice number key: ' + str(slice_number) + '     the records slice number value: ' + str(record_slice_number) + '\n'
                sys.exit(1)

            compressed_blocks = encrypted_file_block_cnt
            uncompressed_blocks = slice_file_block_cnt

            total_blocks_compressed += compressed_blocks
            total_blocks_uncompressed += uncompressed_blocks

            unique_slice_counter += 1
            unique_blocks_compressed += compressed_blocks
            unique_blocks_uncompressed += uncompressed_blocks

        elif already_have_slice.has_key(slice_number):
            line = already_have_slice.get(slice_number)
            slice_record = line.split(' :: ')

            checksum_key                = slice_record[0] + ' :: ' + slice_record[1] + ' :: ' + slice_record[2]
            slice_file_block_cnt        = int(slice_record[4])
            encrypted_file_block_cnt    = int(slice_record[5])
            record_slice_number         = int(slice_record[7])
            status                      = slice_record[8]

            if record_slice_number  !=  slice_number:
                print 'ERROR:  Slice number mismatch between the key used in  already_have_slice  and the value stored in the record\n'
                print 'ERROR:  slice number key: ' + str(slice_number) + '     the records slice number value: ' + str(record_slice_number) + '\n'
                sys.exit(1)

            compressed_blocks = encrypted_file_block_cnt
            uncompressed_blocks = slice_file_block_cnt

            total_blocks_compressed += compressed_blocks
            total_blocks_uncompressed += uncompressed_blocks

            if status == 'DUP':
                duplicate_slice_counter += 1
                duplicate_blocks_compressed += compressed_blocks
                duplicate_blocks_uncompressed += uncompressed_blocks
            else:
                deduped_slice_counter += 1
                deduped_blocks_compressed += compressed_blocks
                deduped_blocks_uncompressed += uncompressed_blocks

        elif duplicate_slices.has_key(slice_number):
            line = duplicate_slices.get(slice_number)
            slice_record = line.split(' :: ')

            checksum_key                = slice_record[0] + ' :: ' + slice_record[1] + ' :: ' + slice_record[2]
            slice_file_block_cnt        = int(slice_record[4])
            encrypted_file_block_cnt    = int(slice_record[5])
            record_slice_number         = int(slice_record[7])
            status                      = slice_record[8]

            if record_slice_number  !=  slice_number:
                print 'ERROR:  Slice number mismatch between the key used in  duplicate_slices  and the value stored in the record\n'
                print 'ERROR:  slice number key: ' + str(slice_number) + '     the records slice number value: ' + str(record_slice_number) + '\n'
                sys.exit(1)

            compressed_blocks = encrypted_file_block_cnt
            uncompressed_blocks = slice_file_block_cnt

            total_blocks_compressed += compressed_blocks
            total_blocks_uncompressed += uncompressed_blocks

            duplicate_slice_counter += 1
            duplicate_blocks_compressed += compressed_blocks
            duplicate_blocks_uncompressed += uncompressed_blocks

        else:
            print 'ERROR:  Should not happen but there is no information for this slice number: ' + str(slice_number) + '\n'
            sys.exit(1)


    # Get the md5 checksum of the vos dump file
    vosdump_hash_handle = hashlib.md5()
    vosdump_hash_handle.update(vosdump_file_name)
    value_md5 = vosdump_hash_handle.hexdigest()

    # Write out the header for the vos dump image file's database
    line = 'HEADER:   VOS dump file:  '+ vosdump_file_name + '\n'
    database_fh.write(line)
    line = 'HEADER:   file size: '+ str(file_size_in_bytes) + '\n'
    database_fh.write(line)
    line = 'HEADER:   MD5 value: ' + value_md5 + '\n'
    database_fh.write(line)
    line = 'HEADER:   Number of slices:  ' + str(number_of_slices).rjust(10) + '\n'
    database_fh.write(line)
    line = 'HEADER:   Slices created:    ' + str(unique_slice_counter).rjust(10) + '\n'
    database_fh.write(line)
    line = 'HEADER:   Deduped slices:    ' + str(deduped_slice_counter).rjust(10) + '\n'
    database_fh.write(line)
    line = 'HEADER:   Duplicate slices:  ' + str(duplicate_slice_counter).rjust(10) + '\n'
    database_fh.write(line)
    line = 'HEADER:\n'
    database_fh.write(line)

    savings = format((100 * (1 - float(total_blocks_compressed) / total_blocks_uncompressed)), '.0f')
    string1 = str(total_blocks_uncompressed).rjust(10)
    string2 = str(total_blocks_compressed).rjust(10)
    #   line = 'HEADER:   Uncompressed blocks: ' + str(total_blocks_uncompressed) + '          Compressed blocks: ' + str(total_blocks_compressed) + '     savings: ' + str(savings) + '%   \n'
    line = 'HEADER:   Uncompressed blocks: ' + string1 + '       Compressed blocks: ' + string2 + '     savings: ' + str(savings) + '%   \n'
    database_fh.write(line)

    line = 'HEADER:\n'
    database_fh.write(line)

    amount_used = format((100 * (float(unique_blocks_compressed) / total_blocks_compressed)), '.0f')
    string1 = str(unique_blocks_uncompressed).rjust(10)
    string2 = str(unique_blocks_compressed).rjust(10)
    #   line = 'HEADER:        Created       uncompressed blocks: ' + str(unique_blocks_uncompressed) + '          compressed: ' + str(unique_blocks_compressed) + '     amount used: ' + str(amount_used) + '%   \n'
    line = 'HEADER:        Created       uncompressed blocks: ' + string1 + '          compressed: ' + string2 + '     amount used: ' + str(amount_used) + '%   \n'
    database_fh.write(line)

    amount_used = format((100 * (float(deduped_blocks_compressed) / total_blocks_compressed)), '.0f')
    string1 = str(deduped_blocks_uncompressed).rjust(10)
    string2 = str(deduped_blocks_compressed).rjust(10)
    #   line = 'HEADER:        Deduped       uncompressed blocks: ' + str(deduped_blocks_uncompressed) + '          compressed: ' + str(deduped_blocks_compressed) + '     amount used: ' + str(amount_used) + '%   \n'
    line = 'HEADER:        Deduped       uncompressed blocks: ' + string1 + '          compressed: ' + string2 + '     amount used: ' + str(amount_used) + '%   \n'
    database_fh.write(line)

    amount_used = format((100 * (float(duplicate_blocks_compressed) / total_blocks_compressed)), '.0f')
    string1 = str(duplicate_blocks_uncompressed).rjust(10)
    string2 = str(duplicate_blocks_compressed).rjust(10)
    #   line = 'HEADER:        Duplicate     uncompressed blocks: ' + str(duplicate_blocks_uncompressed) + '          compressed: ' + str(duplicate_blocks_compressed) + '     amount used: ' + str(amount_used) + '%   \n'
    line = 'HEADER:        Duplicate     uncompressed blocks: ' + string1 + '          compressed: ' + string2 + '     amount used: ' + str(amount_used) + '%   \n'
    database_fh.write(line)



    #  Second for loop
    #
    writeout_dump_db_processing_start_time = time.time()
    if debug_on:
        print 'Write out the database for this vos dump...\n'

    for slice_number in range(1, (number_of_slices + 1)):
        if was_created_keyed_by_slice_number.has_key(slice_number):
            db_record = was_created_keyed_by_slice_number.get(slice_number)
        elif already_have_slice.has_key(slice_number):
            db_record = already_have_slice.get(slice_number)
        elif duplicate_slices.has_key(slice_number):
            db_record = duplicate_slices.get(slice_number)
        else:
            # This condition was check in the first loop but I will check again
            print 'ERROR:  Should not happen but there is no information for this slice number: ' + str(slice_number) + '\n'
            sys.exit(1)

        line = db_record + '\n'
        database_fh.write(line)


    database_fh.close()


    #  Now write out the All Slices database file, this is going to take the execution of 3 for loops
    #
    #     First add the new unique records that were created for this vos dump image to
    #     the dictionary  database_of_all_slices
    #
    #     Second loop through  database_of_all_slices  and calculate the amount of storage used ...etc
    #
    #     Save the current All Slices database file and write out the contents of the
    #     the dictionary  database_of_all_slices as the new version of the All Slices databaase
    #
    add_all_slice_db_processing_start_time = time.time()
    if debug_on:
        print 'Add new records to the All Slice database...\n'

    for checksum_key, line in was_created_keyed_by_checksum.iteritems():
        if database_of_all_slices.has_key(checksum_key):
            print 'ERROR:  This checksum_key should be unique:   ' + str(checksum_key) + '\n'
            sys.exit(1)

        database_of_all_slices[checksum_key] = line


    actual_blocks_compressed = 0
    actual_blocks_uncompressed = 0
    actual_slice_file_count = 0

    realized_blocks_compressed = 0
    realized_blocks_uncompressed = 0
    realized_slice_file_count = 0

    unique_blocks_compressed = 0
    unique_blocks_uncompressed = 0
    unique_slice_file_counter = 0

    update_all_slice_db_processing_start_time = time.time()
    if debug_on:
        print 'Begin proceesing updates to the All Slice database...\n'

    for checksum_key, line in database_of_all_slices.iteritems():
        all_slice_record = line.split(' :: ')

        encrypted_file_size        = int(all_slice_record[0])
        slice_file_block_cnt       = int(all_slice_record[1])
        encrypted_file_block_cnt   = int(all_slice_record[2])
        slice_name                 = all_slice_record[3]
        link_count                 = int(all_slice_record[4])

        compressed_blocks = encrypted_file_block_cnt
        uncompressed_blocks = slice_file_block_cnt

        actual_blocks_compressed += compressed_blocks
        actual_blocks_uncompressed += uncompressed_blocks
        actual_slice_file_count += 1

        realized_blocks_compressed += (compressed_blocks * link_count)
        realized_blocks_uncompressed += (uncompressed_blocks * link_count)
        realized_slice_file_count += link_count

        if link_count  ==  1:
            unique_slice_file_counter += 1
            unique_blocks_compressed += compressed_blocks
            unique_blocks_uncompressed += uncompressed_blocks



    # Save the current All Slice databse and then update it (re write a new one)
    #
    save_all_slice_db_processing_start_time = time.time()
    if debug_on:
        print 'Save previous copy of the All Slice database...\n'

    time_stamp = datetime.now().strftime('%Y_%m_%d_%H%M')
    previous_dbfile_all_slices = dbfile_all_slices + '__' + time_stamp

    if os.path.exists(dbfile_all_slices):
        os.rename(dbfile_all_slices, previous_dbfile_all_slices)

    fh_all_slices = open(dbfile_all_slices, 'w')


    # Write out the informational header for the All Slice database file
    #
    writeout_all_slice_db_processing_start_time = time.time()
    if debug_on:
        print 'Write out a new version of the All Slice database...\n'

    line = 'HEADER:    AFS volume name:  '+ afs_volume_name + '\n'
    fh_all_slices.write(line)

    line = 'HEADER:\n'
    fh_all_slices.write(line)

    line = 'HEADER:    Actual number of slices:    '+ str(actual_slice_file_count).rjust(10) + '\n'
    fh_all_slices.write(line)
    line = 'HEADER:    Realized number of slices:  '+ str(realized_slice_file_count).rjust(10) + '\n'
    fh_all_slices.write(line)

    line = 'HEADER:\n'
    fh_all_slices.write(line)

    savings = format((100 * (1 - float(actual_blocks_compressed) / actual_blocks_uncompressed)), '.0f')
    string1 = str(actual_blocks_uncompressed).rjust(10)
    string2 = str(actual_blocks_compressed).rjust(10)
    #   line = 'HEADER:   Uncompressed blocks: ' + str(total_blocks_uncompressed) + '          Compressed blocks: ' + str(total_blocks_compressed) + '     savings: ' + str(savings) + '%   \n'
    line = 'HEADER:    Actual uncompressed blocks:    ' + string1 + '       Compressed blocks: ' + string2 + '     savings: ' + str(savings) + '%   \n'
    fh_all_slices.write(line)

    string1 = str(realized_blocks_uncompressed).rjust(10)
    string2 = str(realized_blocks_compressed).rjust(10)
    line = 'HEADER:    Realized blocks uncompressed:  ' + string1 + '       Compressed blocks: ' + string2 + '\n'
    fh_all_slices.write(line)

    line = 'HEADER:\n'
    fh_all_slices.write(line)

    storage_efficiency = format((100 * (1 - float(actual_blocks_compressed) / realized_blocks_compressed)), '.2f')
    line = 'HEADER:    Storage efficiency: ' + str(storage_efficiency) + '% \n'
    fh_all_slices.write(line)

    line = 'HEADER:\n'
    fh_all_slices.write(line)


    for checksum_key, line in database_of_all_slices.iteritems():
        db_record = str(checksum_key) + ' :: ' + str(line) + '\n'
        fh_all_slices.write(db_record)


    fh_all_slices.close()

    end_of_processing_start_time = time.time()

    msg = ' SUCCESS   All done...'
    logger.info(msg)
    if debug_on:
        print msg + '\n\n'

    reading_all_slice_db_time = meta_file_processing_start_time - read_all_slice_db_processing_start_time
    reading_meta_file_time = parse_vnodes_processing_start_time - meta_file_processing_start_time
    parsing_vnode_time = read_slice_offset_processing_start_time - parse_vnodes_processing_start_time
    reading_slice_offsets_time = spawn_CalculateSliceChecksum_processing_start_time - read_slice_offset_processing_start_time

    total_checksum_calculations_time = dedupe_unique_processing_start_time - spawn_CalculateSliceChecksum_processing_start_time
    #
    spawning_CalculateChecksum_time = calculate_checksums_processing_start_time - spawn_CalculateSliceChecksum_processing_start_time
    checksum_calculations_time = waiting_on_last_slices_to_calculate_processing_start_time - calculate_checksums_processing_start_time
    waiting_for_checksum_calculations_time = stop_CalculateChecksum_processing_start_time - waiting_on_last_slices_to_calculate_processing_start_time
    stopping_checksum_calculations_time = dedupe_unique_processing_start_time - stop_CalculateChecksum_processing_start_time
    #

    deduping_time = load_two_lists_processing_start_time - dedupe_unique_processing_start_time
    loading_two_lists_time = spawn_CreateSlice_processing_start_time - load_two_lists_processing_start_time

    total_creating_slices_time = verify_duplicate_slices_processing_start_time - spawn_CreateSlice_processing_start_time
    #
    spawning_CreateSlice_time = creating_slices_processing_start_time - spawn_CreateSlice_processing_start_time
    creating_slices_time = waiting_on_last_slices_to_create_processing_start_time - creating_slices_processing_start_time
    waiting_for_slice_creation_time = stop_CreateSlice_processing_start_time - waiting_on_last_slices_to_create_processing_start_time
    stopping_slice_creation_time = verify_duplicate_slices_processing_start_time - stop_CreateSlice_processing_start_time
    #

    verification_time = writeout_dump_db_processing_start_time - verify_duplicate_slices_processing_start_time
    writing_dump_db_time = add_all_slice_db_processing_start_time - writeout_dump_db_processing_start_time
    updating_all_slice_db_time = end_of_processing_start_time - add_all_slice_db_processing_start_time
    total_elapsed_time = end_of_processing_start_time - main_processing_start_time 

    msg = 'Total run time...  ' + str(round(total_elapsed_time)) + '\n'
    msg = msg + '    read all slice db:            ' + str(round(reading_all_slice_db_time,4)) + '\n'
    msg = msg + '    read meta file:               ' + str(round(reading_meta_file_time,4)) + '\n'
    msg = msg + '    parse vnodes:                 ' + str(round(parsing_vnode_time,4)) + '\n'
    msg = msg + '    read slice offsets:           ' + str(round(reading_slice_offsets_time,4)) + '\n'

    msg = msg + '    total checksum calculations:  ' + str(round(total_checksum_calculations_time,4)) + '\n'
    msg = msg + '                                            ' +  str(round(spawning_CalculateChecksum_time,4)) + '   spawning subprocesses to calculate checksum\n'
    msg = msg + '                                            ' +  str(round(checksum_calculations_time,4)) + '   feeding checksum calculations\n'
    msg = msg + '                                            ' +  str(round(waiting_for_checksum_calculations_time,4)) + '   waiting on last calculations\n'
    msg = msg + '                                            ' +  str(round(stopping_checksum_calculations_time,4)) + '   stopping subprocesses\n'

    msg = msg + '    deduping:                     ' + str(round(deduping_time,4)) + '\n'
    msg = msg + '    load 2 lists:                 ' + str(round(loading_two_lists_time,4)) + '\n'

    msg = msg + '    total creating slices:        ' + str(round(total_creating_slices_time,4)) + '\n'
    msg = msg + '                                            ' +  str(round(spawning_CreateSlice_time,4)) + '   spawning subprocesses to create slices\n'
    msg = msg + '                                            ' +  str(round(creating_slices_time,4)) + '   feeding slice creation\n'
    msg = msg + '                                            ' +  str(round(waiting_for_slice_creation_time,4)) + '   waiting on creation of last slices\n'
    msg = msg + '                                            ' +  str(round(stopping_slice_creation_time,4)) + '   stopping subprocesses\n'

    msg = msg + '    verification:                 ' + str(round(verification_time,4)) + '\n'
    msg = msg + '    writing dump db:              ' + str(round(writing_dump_db_time,4)) + '\n'
    msg = msg + '    updating all slice db:        ' + str(round(updating_all_slice_db_time,4)) + '\n'

    logger.info(msg)
    if debug_on:
        print msg + '\n'

    if not debug_on:
        #  On success rename and move the log file from temporary scratch directory to the local cloud center  (/AFS_backups_in_Cloud/Log) 
        new_log = '/AFS_backups_in_Cloud/Log/' + short_program_name + '__' + vosdump_file_name
        shutil.move(log_file_name, new_log)

        if os.path.isfile(slice_offset_file):
            os.remove(slice_offset_file)


    sys.exit(0)

