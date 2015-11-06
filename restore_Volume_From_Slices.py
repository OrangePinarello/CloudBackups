#!/usr/bin/env python
#
#    $Header$
#
#    $Revision$
#
#    $Date$
#    $Locker$
#    $Author$
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
# Source code location:     /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/restore_Volume_From_Slices.py
#
# Local location:           /AFS_backups_in_Cloud/bin/restore_Volume_From_Slices.py
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
#         --volume                  <Name of the AFS volume being restored>
#
#         --dirpath                 <Directory where the VOS dump database files are located>
#
#         --list                    <List of dump files that will be required>
#
#
#
#
#
# History:
#
#   Version 0.x     TMM   09/15/2015   code development started
#
#   Version 1.1     TMM   10/16/2015
#
#        Initail code drop, working code that gathers slices for a list of VOS dump files.
#        Also verified error handling of erros encounterd within the subprocesses:
#
#               Decrypt_One_Slice.py
#               Decrypt_Multiple_Slices.py
#
#
#=   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =

import os
import sys
import getopt
import time
import subprocess
import logging
import logging.handlers

import pickle

from sys import argv

#   from datetime import datetime

from subprocess import PIPE, Popen
from threading  import Thread

try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x


ON_POSIX = 'posix' in sys.builtin_module_names


#  define this class so we can assign stdout to it...   Then all "print" statments get flushed aka not buffered
class flushfile(object):
    def __init__(self, f):
        self.f = f
    def write(self, x):
        self.f.write(x)
        self.f.flush()


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

def  ReadFromTheSubProcessQueue(queue_name, queue_write_count, overall_write_count, subprocess_q):

    global  debug_on
    global  debug_empty_queue

    global  dict_of_items_read_from_queue
    global  dict_of_written_items

    global  unknown_error_read_from_queue


    if overall_write_count  !=  0:
        flag_unexpected_error = False
        while queue_write_count != 0  and  overall_write_count  !=  0:
            ###  try:  returned_record = subprocess_q.get_nowait()

            try:  returned_record = subprocess_q.get(timeout=0.1)
            except Empty:
                if debug_empty_queue:
                    print 'Empty no record on ' + queue_name + '\n'

                break
            else:
                if 'SUCCESS :: '  in returned_record:
                    #  Take the database record that was passed back from the subprocess  and temporarily place it in a list for further processing
                    queue_write_count -= 1
                    returned_record = returned_record.rstrip('\r|\n')

                elif 'ERROR :: '  in returned_record:
                    # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                    queue_write_count = -666
                    returned_record = returned_record.rstrip('\r|\n')

                else:
                    unknown_error_read_from_queue = 'Unexpected output read from ' + queue_name + '\n--->' + returned_record + '<---\n'
                    flag_unexpected_error = True
                    queue_write_count = -666


                overall_write_count -= 1

                if not flag_unexpected_error:
                    if debug_on:
                        print 'read from ' + queue_name + '  -->' + returned_record + '<--\n'

                    record = returned_record.split(' :: ')
                    item_identifier = record[1]
                    record_count = len(record)
                    output = ''

                    for index in range(2, record_count):
                        if not output:
                            output = record[index]
                        else:
                            output += ' :: ' + record[index]

                    dict_of_items_read_from_queue[item_identifier] = output

                    if queue_write_count == -666:
                       unknown_error_read_from_queue = output 

                    del dict_of_written_items[item_identifier]


    return(queue_write_count, overall_write_count)




#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

def  WriteDumpInfoToTheSubProcessQueue(queue_name, queue_write_count, number_of_items_to_process,  depth_of_queue, overall_write_count, max_queued, slicer_subprocess):

    global  debug_on

    global  list_of_item_identifiers
    global  list_of_items_to_write
    global  dict_of_written_items


    while number_of_items_to_process  >  0  and  queue_write_count < depth_of_queue  and  overall_write_count  !=  max_queued:

        item_to_write = list_of_items_to_write.pop(0)
        item_identifier = list_of_item_identifiers.pop(0)

        if dict_of_written_items.has_key(item_identifier):
            list_of_items_to_write.append(item_to_write)
            list_of_item_identifiers.append(item_identifier)
            break
        else:
            dict_of_written_items[item_identifier] = 1
            queue_write_count += 1
            overall_write_count += 1
            number_of_items_to_process -= 1

            write_buffer = str(item_identifier) + ' :: ' + item_to_write + '\n'
            slicer_subprocess.stdin.write(write_buffer)
            slicer_subprocess.stdin.flush()

        if debug_on:
            print 'Wrote this to ' + queue_name + ' -->' + str(item_identifier) + ' :: ' + item_to_write + '<--\n'
            print queue_name + ' write count: '  + str(queue_write_count) + '     Over All write count: '  + str(overall_write_count) + '     remaining dump files: ' + str(number_of_items_to_process) + '\n'

    return(queue_write_count, number_of_items_to_process, overall_write_count)




#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

def  Write_Concatenation_Cmds():

    global  debug_on

    global  vos_dump_file
    global  concatenation_pipe
    global  dict_of_items_read_from_queue


    for item_identifier, returned_record in dict_of_items_read_from_queue.iteritems():
        record = returned_record.split(' :: ')
        chunk_index = record[0]
        chunk_file_name = record[1]

        write_buffer = 'CHUNK :: ' + vos_dump_file + ' :: ' + chunk_index + ' :: ' + chunk_file_name + '\n'
        concatenation_pipe.stdin.write(write_buffer)
        concatenation_pipe.stdin.flush()

        if debug_on:
            print 'Wrote this to concatenation pipe -->CHUNK :: ' + vos_dump_file + ' :: ' + chunk_index + ' :: ' + chunk_file_name + '<--\n'

    dict_of_items_read_from_queue = {}




#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

def  Read_Concatenation_Results():

    global  debug_empty_concatenation_queue

    global  unknown_error_read_from_queue
    global  concatenation_queue


    ###  TMM  losing data when reading results from Concatenation_Process.py
    ###       change the call from a "get" with a time out of 0.1 seconds to a "get_nowait"
    ###
    ###   try:  returned_record = concatenation_queue.get_nowait()
    ###
    ###   try:  returned_record = concatenation_queue.get(timeout=0.1)

    try:  returned_record = concatenation_queue.get(timeout=0.1)
    except Empty:
        if debug_empty_concatenation_queue:
            print 'Empty no message from concatenation pipe\n'

        name_of_vos_dump_file = ''
        success_flag = True
        return(success_flag, name_of_vos_dump_file)


    else:
        if 'SUCCESS :: '  in returned_record:
            returned_record = returned_record.rstrip('\r|\n')
            record = returned_record.split(' :: ')
            name_of_vos_dump_file = record[1]
            success_flag = True


        elif 'ERROR :: '  in returned_record:
            # On an error condition from trying to concatenate chunk files into the vos dump file
            returned_record = returned_record.rstrip('\r|\n')
            record = returned_record.split(' :: ')
            name_of_vos_dump_file = record[1]
            unknown_error_read_from_queue = record[2]
            success_flag = False


        else:
            unknown_error_read_from_queue = 'Unexpected output read from concatenation pipe\n--->' + returned_record + '<---\n'
            name_of_vos_dump_file = ''
            success_flag = False
            
    return(success_flag, name_of_vos_dump_file)





#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

def  StartMultipleSubProcesses(number_of_items_to_process, subprocess_count, write_queue_depth):

    global  debug_on

    global  flag_process_duplicate_slices
    global  flag_process_unique_slices
    global  unknown_error_read_from_queue

    global  dict_of_vos_dump_files

    flag_fatal_error = False
    list_processing_nodes = []

    if flag_process_duplicate_slices:
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud1.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_One_Slice.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud2.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_One_Slice.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud3.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_One_Slice.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud4.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_One_Slice.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud5.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_One_Slice.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud6.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_One_Slice.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud7.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_One_Slice.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud8.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_One_Slice.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud9.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_One_Slice.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud10.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_One_Slice.py')
    elif flag_process_unique_slices:
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud1.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_Multiple_Slices.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud2.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_Multiple_Slices.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud3.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_Multiple_Slices.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud4.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_Multiple_Slices.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud5.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_Multiple_Slices.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud6.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_Multiple_Slices.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud7.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_Multiple_Slices.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud8.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_Multiple_Slices.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud9.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_Multiple_Slices.py')
        list_processing_nodes.append('/usr/bin/ssh root@afsbk-cloud10.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Decrypt_Multiple_Slices.py')
    else:
        print 'ERROR the type of processing has not be identified\n'
        sys.exit(1)


    #  Configured the subprocess that will be used

    list_proc_write_count = []
    list_pipe_proc = []
    list_q_proc = []
    list_t_proc = []

    for thread_index in range(0, subprocess_count):
        proc_write_count = 0

        # Do a pop and then an append becuase the  "subprocess_count"  is likely large than the number of items in the  "list_processing_nodes"
        ProcessingNode = list_processing_nodes.pop(0)
        list_processing_nodes.append(ProcessingNode)


        pipe_proc = Popen([ProcessingNode], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc = Queue()
        t_proc = Thread(target=enqueue_output, args=(pipe_proc.stdout, q_proc))
        t_proc.daemon = True
        t_proc.start()

        list_proc_write_count.append(proc_write_count)
        list_pipe_proc.append(pipe_proc)
        list_q_proc.append(q_proc)
        list_t_proc.append(t_proc)


    MAX_QUEUED = write_queue_depth * subprocess_count

    current_write_count = 0


    #  Now go and do stuff

    while number_of_items_to_process  >  0:
        if current_write_count  !=  MAX_QUEUED:

         for thread_index in range(0, subprocess_count):
            proc_write_count = list_proc_write_count.pop(0)
            pipe_proc = list_pipe_proc.pop(0)

            queue_name = "queue " + str((thread_index + 1))

            proc_write_count, number_of_items_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue(queue_name, proc_write_count, number_of_items_to_process, write_queue_depth, current_write_count, MAX_QUEUED, pipe_proc)

            list_proc_write_count.append(proc_write_count)
            list_pipe_proc.append(pipe_proc)


        #  Check on the subprocesses wait on them until they have something to read
        #
        flag_waiting_to_read = True
        previous_write_count = current_write_count

        while flag_waiting_to_read:
            if previous_write_count  !=  current_write_count:
                #  One subprocess had something which we read, take one more pass thur to find any others then go write something to the sub processibng queues
                flag_waiting_to_read = False

            for thread_index in range(0, subprocess_count):
                proc_write_count = list_proc_write_count[thread_index]
                q_proc = list_q_proc[thread_index]
                queue_name = "queue " + str((thread_index + 1))

                proc_write_count, current_write_count = ReadFromTheSubProcessQueue(queue_name, proc_write_count, current_write_count, q_proc)

                if proc_write_count == -666:
                    print '\n\nFATAL ERROR was received on ' + queue_name + '   graceful exit in process\n\nError message:\n' + unknown_error_read_from_queue + '\n\n\n'
                    current_write_count = 0
                    flag_fatal_error = True
                    flag_waiting_to_read = False
                    break
                else:
                    list_proc_write_count[thread_index] = proc_write_count
                    list_q_proc[thread_index] = q_proc

                    if flag_process_unique_slices:
                        #  Send the chunk file information onto the piped process that will concatenate files together to create the vos dump file
                        Write_Concatenation_Cmds()

                        #  See if the Concatenation_Process.py has written  back any status for the vos dump files that MAY have been created
                        #  a read on an empty pipe returns SUCCESS but the name of the "created_dump_file" is null
                        success_flag, created_dump_file = Read_Concatenation_Results()
                        if success_flag:
                            if created_dump_file:
                                dict_of_vos_dump_files[created_dump_file] = 'CREATED'

                                dump_file_name = os.path.basename(created_dump_file)
                                if dump_file_name.startswith('DB__'):
                                    dump_file_name = dump_file_name[4:]
                                if dump_file_name.endswith('__vosdumpfile'):
                                    dump_file_name = dump_file_name[:-13]
                                print 'Created the vos dump file: ' + dump_file_name

                                if debug_on:
                                    number_of_files = len(dict_of_vos_dump_files)
                                    for file_name, status_string in dict_of_vos_dump_files.iteritems():
                                        if status_string  ==  'CREATED':
                                            number_of_files -= 1

                                    print 'Successfully created  ' + created_dump_file + '    ' + str(number_of_files) + ' vos dump files still need to be created\n'
                        else:
                            if created_dump_file:
                                print '\n\nFATAL ERROR for the vos dump file: ' + created_dump_file + '  was received on the concatenation pipe\n\nError message:\n' + unknown_error_read_from_queue + '\n\n\n'
                            else:
                                print '\n\nFATAL ERROR was received on the concatenation pipe graceful exit in process\n\nError message:\n' + unknown_error_read_from_queue + '\n\n\n'

                            current_write_count = 0
                            flag_fatal_error = True
                            flag_waiting_to_read = False
                            break                                 

    #  Clean up at the end, now check on the subprocesses that are processing the last vos dump files
    #
    if debug_on:
        remaining_to_process = 0
        for thread_index in range(0, subprocess_count):
            remaining_to_process += list_proc_write_count[thread_index]

        print 'Number of slices being processed (' + str(current_write_count) + ') and the number of outstanding slices remaining to be processed (' + str(remaining_to_process) + ')\n'

    while current_write_count  >  0:

        for thread_index in range(0, subprocess_count):
            proc_write_count = list_proc_write_count[thread_index]
            q_proc = list_q_proc[thread_index]
            queue_name = "queue " + str((thread_index + 1))
            proc_write_count, current_write_count = ReadFromTheSubProcessQueue(queue_name, proc_write_count, current_write_count, q_proc)

            if proc_write_count == -666:
                print '\n\nFATAL ERROR was received on ' + queue_name + '   graceful exit in process\n\nError message:\n' + unknown_error_read_from_queue + '\n\n\n'
                current_write_count = 0
                flag_fatal_error = True
                break
            else:
                list_proc_write_count[thread_index] = proc_write_count
                list_q_proc[thread_index] = q_proc

                if flag_process_unique_slices:
                    #  Send the chunk file information onto the piped process that will concatenate files together to create the vos dump file
                    Write_Concatenation_Cmds()

                    #  See if the Concatenation_Process.py has written  back any status for the vos dump files that MAY have been created
                    #  a read on an empty pipe returns SUCCESS but the name of the "created_dump_file" is null
                    success_flag, created_dump_file = Read_Concatenation_Results()
                    if success_flag:
                        if created_dump_file:
                            dict_of_vos_dump_files[created_dump_file] = 'CREATED'

                            dump_file_name = os.path.basename(created_dump_file)
                            if dump_file_name.startswith('DB__'):
                                dump_file_name = dump_file_name[4:]
                            if dump_file_name.endswith('__vosdumpfile'):
                                dump_file_name = dump_file_name[:-13]
                            print 'Created the vos dump file: ' + dump_file_name

                            if debug_on:
                                number_of_files = len(dict_of_vos_dump_files)
                                for file_name, status_string in dict_of_vos_dump_files.iteritems():
                                    if status_string  ==  'CREATED':
                                        number_of_files -= 1

                                print 'Successfully created  ' + created_dump_file + '    ' + str(number_of_files) + ' vos dump files still need to be created\n'
                    else:
                        if created_dump_file:
                            print '\n\nFATAL ERROR for the vos dump file: ' + created_dump_file + '  was received on the concatenation pipe\n\nError message:\n' + unknown_error_read_from_queue + '\n\n\n'
                        else:
                            print '\n\nFATAL ERROR was received on the concatenation pipe graceful exit in process\n\nError message:\n' + unknown_error_read_from_queue + '\n\n\n'

                        current_write_count = 0
                        flag_fatal_error = True
                        flag_waiting_to_read = False
                        break                                 


        if current_write_count  >  0:
            #  Verify that that there are still subprocess that we suspect still are processing the last of the vos dump files
            #  this is like a fail safe so we don't end up looping forever
            remaining_to_be_read = 0
            for thread_index in range(0, subprocess_count):
                remaining_to_be_read += list_proc_write_count[thread_index]

            if remaining_to_be_read  !=  current_write_count:
                print '\n\nFATAL ERROR stuck in endless loop mismatch between  current_write_count (' + str(current_write_count) + ') and the number of outstanding slices to be read (' + str(remaining_to_be_read) + ')\n'
                current_write_count = 0
                flag_fatal_error = True
            else:
                #  Go to sleep for 25 msec and and then check the subprocesses again
                time.sleep(0.025)


    #  Send a STOP message to all the subprocesses

    if debug_on:
        if flag_process_duplicate_slices:
            print 'Send STOP message to the  Decrypt_One_Slice.py  subprocesses...\n'
        if flag_process_unique_slices:
            print 'Send STOP message to the  Decrypt_Multiple_Slices.py  subprocesses...\n'


    stop_message = 'STOP\n'

    for thread_index in range(0, subprocess_count):
        pipe_proc = list_pipe_proc[thread_index]
        pipe_proc.stdin.write(stop_message)
        pipe_proc.stdin.flush()
        list_pipe_proc[thread_index] = pipe_proc


    if flag_fatal_error:
        sys.exit(1)


#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

def  ProcessCommandLine(argv, program_name):

    afs_volume_name = ''
    directory_path = ''
    string_of_file_names = ''

    help01 = '  --help\n\n'
    help02 = '  --volume            <Name of the AFS volume being restored>\n\n'
    help03 = '  --dirpath           <Path to the directory where the dump database files are located>\n\n'
    help04 = '  --list              <A comma separatedlist of the dump file names to are required>\n\n'


    help_msg = help01 + help02 + help03 + help04

    try:
        opts, args = getopt.getopt(argv,"hvdl:",["help","volume=","dirpath=","list="])
    except getopt.GetoptError:
        print ' ' + program_name + '\n\n' + help_msg
        sys.exit(1)


    dryrun = False
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print ' ' + program_name + '\n\n' + help_msg
            sys.exit(0)
        elif opt in ("-v", "--volume"):
            afs_volume_name = arg
        elif opt in ("-d", "--dirpath"):
            directory_path = arg     
        elif opt in ("-l", "--list"):
            string_of_file_names = arg        

    number_of_parms = len(sys.argv)
    if number_of_parms > 7:
        msg = 'Must enclose the \"list\" of file names in quotes or remove the white spaces'
        print msg + '\n ' + program_name + '\n\n' + help_msg
        logger.critical(msg)
        sys.exit(1)

    if not afs_volume_name:
        msg = 'Must specify the AFS volume that is being restored'
        print msg + '\n ' + program_name + '\n\n' + help_msg
        logger.critical(msg)
        sys.exit(1)

    if not directory_path:
        msg = 'Must specify a directory path to where the dump database files are located'
        print msg + '\n ' + program_name + '\n\n' + help_msg
        logger.critical(msg)
        sys.exit(1)

    if not string_of_file_names:
        msg = 'Must specify comma separated list of the required dump file names'
        print msg + '\n ' + program_name + '\n\n' + help_msg
        logger.critical(msg)
        sys.exit(1)


    return(afs_volume_name, directory_path, string_of_file_names)







#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

if __name__ == "__main__":

    #  assign stdout to this class (flushfile)...   Then all "print" statments get flushed aka not buffered
    sys.stdout = flushfile(sys.stdout)



    debug_on = False

    debug_empty_queue = False                 # Log a message every time an empty queue is read (really only for development)

    debug_empty_concatenation_queue = False   # Log a message every time the "concatenation pipe" is read when it's empty  (really only for development)

    debug_CalculateSliceChecksum = False     # Watch what is returned from the decryption of the slice

 
    debug_level_1 = False    # the very lowest level debugging statements (not typical used after development)
    debug_level_2 = False    # available unused in the program
    debug_level_3 = False    # available unused in the program
    debug_level_4 = False    # debug the reading of the slice info file
    debug_level_5 = False    # debug the reading of the vos dump meta data file

    flag_process_duplicate_slices = False
    flag_process_unique_slices = False

    unknown_error_read_from_queue = ''

    #       This dictionary uses the name of the vos dump file that will be created as the KEY and the VALUE is the text string to indicate where in the process
    dict_of_vos_dump_files = {}

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


    afs_volume_name, directory_path, string_of_file_names = ProcessCommandLine(sys.argv[1:], program_name)

    if debug_level_1:
        print '            volume name:  ' + afs_volume_name + '\n'
        print '         directory path:  ' + directory_path + '\n'
        print 'dump database file list:  ' + string_of_file_names + '\n'

    # Validate the input
    dbfile_all_slices = directory_path + '/DB_' + afs_volume_name + '__All_Slices'
    if not os.path.isfile(dbfile_all_slices):
        msg = 'Unable to find the All Slices database file:  ' + dbfile_all_slices
        print 'FATAL ERROR    ' + msg + '\n'
        logger.critical(msg)
        sys.exit(1)

    # split the string of dump database files names on the comma delimter
    file_list = string_of_file_names.split(',')
    number_of_files = len(file_list)
    if number_of_files == 1:
        # okay either the user is using a space instead of a comma as the delimter or the string only has 1 dump database file name
        file_list = string_of_file_names.split()
        number_of_files = len(file_list)

    if debug_level_1:
        print 'Number of dump database files  ' + str(number_of_files) + '\n'

    Restore_stub = '/AFS_backups_in_Cloud/Restore/' + afs_volume_name + '/'

    # build the full path to each of the dump database files and validate that they exist
    database_file_list = []
    for index in range(0, number_of_files):
        db_file_path = directory_path + '/' + file_list[index]
        db_file_path = db_file_path.replace(" ", "")
        database_file_list.append(db_file_path)

        vos_dump_file = Restore_stub + os.path.basename(db_file_path) + '__vosdumpfile'
        dict_of_vos_dump_files[vos_dump_file] = 'Going To Create'

        if debug_level_1:
            print 'dump database file path:   ' + db_file_path + '\n'

        if not os.path.isfile(db_file_path):
            msg = 'Unable to find the dump database file:  ' + db_file_path
            print 'FATAL ERROR    ' + msg + '\n'
            logger.critical(msg)
            sys.exit(1)        


    # For each of the Dump database files, we will need these 3 diectionaries
    #
    #        location_dict
    #        checksum_dict
    #        status_dict
    #

    #   This dictionary uses the checksum_key as the KEY and the VALUE is the location (name) of the slice file within the Object Store
    location_dict = {}

    #   This dictionary uses the slice_index as the KEY and the VALUE is the checksum_key
    checksum_dict = {}

    #   This dictionary uses the slice_index as the KEY and the VALUE is the status of the slice (unique, deduped or DUP)
    status_dict = {}



    list_of_location_dict = []
    list_of_checksum_dict = []
    list_of_status_dict = []


    #   When combining the slices from all of the Dump database files that will be required to perform the restore activity.  This dictionary will contain
    #   information about slices that are only referenced one time.
    # 
    #       This dictionary uses the checksum_key as the KEY and the VALUE is the location (name) of the slice file within the Object Store
    #
    unique_slice_location_dict = {}

    #   When combining the slices from all of the Dump database files that will be required to perform the restore activity.  This dictionary will contain
    #   information about the slices that are referenced multiple times.
    # 
    #       This dictionary uses the checksum_key as the KEY and the VALUE is the location (name) of the slice within the Object Store
    #
    dup_slice_location_dict = {}

    #       This dictionary uses the checksum_key as the KEY and the VALUE is a counter that tracks the number of times that this slices is used
    dup_slice_counter_dict = {}

    #       This dictionary uses the location (name) of the slice file within the Object Store as the KEY and the VALUE is the checksum_key
    all_checksum_dict = {}

    #       This dictionary uses the checksum_key as the KEY and the VALUE is the path to the slice that has been decrypted and uncompressed
    dict_of_duplicate_slices = {}



    number_of_files = len(database_file_list)
    for file_index in range(0, number_of_files):
        db_file_path = database_file_list[file_index]

        if debug_on:
            print 'Reading the dump database   ' + db_file_path + '\n'

        fh_db_file = open(db_file_path, 'r')

        for line in fh_db_file:
            line = line.rstrip('\r|\n')
            # skip over the HEADER
            tokens = line.split()
            if 'HEADER:' != tokens[0]:
                db_record = line.split(' :: ')
                #  The format of this record used within the Dump file database
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
                #     The offset from the begining of the VOS dump file in bytes, where this slice starts
                #     The slice number (slice index) for this slice
                #     The status of the slice in the context of the vos dump file (unique --> 1 occurrence   Deduped --> 1 occurrence  DUP --> multiple occurrences)
                #             Deduped simply indicates that when the VOS dump file was processed this slice had already been seen in a previous VOS dump, therefore
                #             there was no need to write it out to the Object Store
                #              
                #     The location (name) of the slice within the object store
                #
                #
                sha1_value = db_record[0]
                md5_value = db_record[1]
                slice_file_size = db_record[2]
                encrypted_file_size = int(db_record[3])
                slice_file_block_cnt = int(db_record[4])
                encrypted_file_block_cnt = int(db_record[5])
                byte_offset = int(db_record[6])
                slice_index = int(db_record[7])
                slice_status = db_record[8]
                slice_location = db_record[9]
            
                checksum_key = sha1_value + ' :: ' + md5_value + ' :: ' + str(slice_file_size)

                if not location_dict.has_key(checksum_key):
                    location_dict[checksum_key] = slice_location
                else:
                    if not location_dict[checksum_key] == slice_location:
                        msg = 'Attempting to populate  location_dict  using duplicate KEY:  ' + checksum_key
                        print 'FATAL ERROR    ' + msg + '\n'
                        logger.critical(msg)
                        msg = 'assigned VALUE:  ' + location_dict[checksum_key]
                        logger.critical(msg)
                        msg = 'new VALUE:  ' + slice_location
                        logger.critical(msg)
                        sys.exit(1)

                if not checksum_dict.has_key(slice_index):
                    checksum_dict[slice_index] = checksum_key
                else:
                    msg = 'Attempting to populate  checksum_dict  using duplicate KEY:  ' + slice_index
                    print 'FATAL ERROR    ' + msg + '\n'
                    logger.critical(msg)
                    msg = 'assigned VALUE:  ' + checksum_key
                    logger.critical(msg)
                    sys.exit(1)


                if not status_dict.has_key(slice_index):
                    status_dict[slice_index] = slice_status
                else:
                    msg = 'Attempting to populate  status_dict  using duplicate KEY:  ' + slice_index
                    print 'FATAL ERROR    ' + msg + '\n'
                    logger.critical(msg)
                    msg = 'assigned VALUE:  ' + slice_status
                    logger.critical(msg)
                    sys.exit(1)


                if dup_slice_location_dict.has_key(checksum_key):
                    dup_slice_counter_dict[checksum_key] += 1
                    all_checksum_dict[slice_location] = checksum_key
                else:
                    if not unique_slice_location_dict.has_key(checksum_key):
                        unique_slice_location_dict[checksum_key] = slice_location
                        all_checksum_dict[slice_location] = checksum_key
                    else:
                        # This the 2nd occurrence hence this slice is no longer unique
                        if unique_slice_location_dict[checksum_key] == slice_location:
                            del unique_slice_location_dict[checksum_key]
                            dup_slice_location_dict[checksum_key] = slice_location
                            dup_slice_counter_dict[checksum_key] = 2
                        else:
                            msg = 'The  checksum_key  KEY is associated with two different  slice_location  values'
                            print 'FATAL ERROR    ' + msg + '\n'
                            logger.critical(msg)
                            msg = 'checksum_key  KEY:    ' + checksum_key
                            logger.critical(msg)
                            msg = '1st  slice_location  VALUE:    ' + str(unique_slice_location_dict[checksum_key])
                            logger.critical(msg)
                            msg = '2nd  slice_location  VALUE:    ' + slice_location
                            logger.critical(msg)
                            sys.exit(1)

        fh_db_file.close()

        #  Save the dictionaries that are related to this Vos Dump file's database in a LIST
        list_of_location_dict.append(location_dict)
        list_of_checksum_dict.append(checksum_dict)
        list_of_status_dict.append(status_dict)

        location_dict = {}
        checksum_dict = {}
        status_dict = {}


    # Go thur and mark slices as duplicates (if they are)
    number_of_dictionaries = len(list_of_checksum_dict)
    for dictionary_index in range(0, number_of_dictionaries):
        db_file_path = database_file_list[dictionary_index]
        dump_file_name = os.path.basename(db_file_path)

        checksum_dict = list_of_checksum_dict[dictionary_index]
        status_dict = list_of_status_dict[dictionary_index]

        number_of_slices = len(checksum_dict)
        for slice_index in range(1, (number_of_slices + 1)):
            checksum_key = checksum_dict[slice_index]
            if dup_slice_location_dict.has_key(checksum_key):
                status_dict[slice_index] = 'DUP'

        if dump_file_name.startswith('DB__'):
            dump_file_name = dump_file_name[4:]
        print 'The vos dump file  ' + dump_file_name + '  has  ' + str(number_of_slices) + '  slices'

        list_of_checksum_dict[dictionary_index] = checksum_dict
        list_of_status_dict[dictionary_index] = status_dict

        checksum_dict = {}
        status_dict = {}



    # Go and get all the duplicate slices from the Object Store and store them for reuse
    ObjStore_stub = '/AFS_backups_in_Cloud/ObjectStore/'

    total_slices = len(dup_slice_location_dict) + len(unique_slice_location_dict)
    print 'We will need to fetch a total of  ' + str(total_slices) + '  slices'
    print 'Begin by fetching  ' + str(len(dup_slice_location_dict)) + '  duplicate slices'

    try: 
        os.makedirs(Restore_stub)
    except OSError:
        if not os.path.isdir(Restore_stub):
            raise


    #  Define the two global lists and the two global dictionaries needed by the functions
    #
    #         StartMultipleSubProcesses()
    #         WriteDumpInfoToTheSubProcessQueue()
    #         ReadFromTheSubProcessQueue()
    #
    list_of_items_to_write = []
    list_of_item_identifiers = []
    dict_of_written_items = {}             # The checksum_key as the KEY and the VALUE is simply a flag to indicate that the "identified item" was written
    dict_of_items_read_from_queue = {}     # The checksum_key as the KEY and the VALUE is what was read from the queue minus the "status" and the "identifier".  


    for checksum_key, slice_location in dup_slice_location_dict.iteritems():
        temp_list = checksum_key.split(' :: ')
        sha1_checksum = temp_list[0]
        md5_checksum = temp_list[1]
        output_file_name = Restore_stub + 'DUP_' + str(checksum_key.replace(' :: ', '_'))

        temp_record = slice_location + ' :: ' + sha1_checksum + ' :: ' + md5_checksum + ' :: ' + output_file_name + ' :: ' + ObjStore_stub + ' :: '
        list_of_items_to_write.append(temp_record)
        list_of_item_identifiers.append(slice_location)


    # Set up the parameters needed for starting multiple processing queues to process all the duplicate slices

    flag_process_duplicate_slices = True
    number_of_items_to_write = len(list_of_items_to_write)
    if (number_of_items_to_write > 200):
        if (number_of_items_to_write > 1000):
            if (number_of_items_to_write > 10000):
                if (number_of_items_to_write > 20000):
                    number_of_subprocesses = 80
                    queue_depth = 100
                else:
                    number_of_subprocesses = 40
                    queue_depth = 100
            else:
                number_of_subprocesses = 20
                queue_depth = 30
        else:
            number_of_subprocesses = 5
            queue_depth = 20
    else:
        number_of_subprocesses = 1
        queue_depth = 1

    
    StartMultipleSubProcesses(number_of_items_to_write, number_of_subprocesses, queue_depth)

    flag_process_duplicate_slices = False

    for slice_location, file_name in dict_of_items_read_from_queue.iteritems():
        checksum_key = all_checksum_dict[slice_location]
        dict_of_duplicate_slices[checksum_key] = file_name    # The checksum_key as the KEY and the VALUE is the file name within the restore directory


    # Now pickle the dictionary of duplicate slices  (dict_of_duplicate_slices) that have been decrypted and uncompressed
    file_name = Restore_stub + 'Dictionary_duplicate_slices.pickle'
    fh_output = open(file_name, 'wb')
    pickle.dump(dict_of_duplicate_slices, fh_output)
    fh_output.close()
    duplicate_slices_pickle_file = file_name


    # Initialize the piped process that will concatenate the slice files into the vos dump file(s)
    concatenation_node = '/usr/bin/ssh root@afsbk-cloud1.cc.nd.edu -q /AFS_backups_in_Cloud/bin/Concatenation_Process.py'

    ###  TMM  losing data when reading results from Concatenation_Process.py
    ###        (did not help) changing bufsize from 1 to 4096 
    ###        (did not help) changing bufsize to -1  which means to use the system default, which usually means fully buffered
    ###
    ###   concatenation_pipe = Popen([concatenation_node], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
    concatenation_pipe = Popen([concatenation_node], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=-1, close_fds=ON_POSIX, shell = True)
    concatenation_queue = Queue()
    concatenation_thread = Thread(target=enqueue_output, args=(concatenation_pipe.stdout, concatenation_queue))
    concatenation_thread.daemon = True
    concatenation_thread.start()


    total_slices -= len(dup_slice_location_dict)
    print 'Fetch the remaining  ' + str(total_slices) + '  unique slices and creating the  ' + str(len(list_of_checksum_dict)) + '  vos dump files'


    list_of_vos_dump_files = []

    #  Process each VOS dump database
    number_of_dictionaries = len(list_of_checksum_dict)
    for dictionary_index in range(0, number_of_dictionaries):

        db_file_path = database_file_list[dictionary_index]
        location_dict = list_of_location_dict[dictionary_index]
        checksum_dict = list_of_checksum_dict[dictionary_index]

        # Now pickle the two dictionaries that are associated with this VOS dump database
        file_name = Restore_stub + 'Dictionary_location.pickle'
        fh_output = open(file_name, 'wb')
        pickle.dump(location_dict, fh_output)
        fh_output.close()
        location_pickle_file = file_name

        file_name = Restore_stub + 'Dictionary_checksum.pickle'
        fh_output = open(file_name, 'wb')
        pickle.dump(checksum_dict, fh_output)
        fh_output.close()
        checksum_pickle_file = file_name


        #  Define the two global lists and the two global dictionaries needed by the functions
        #
        #         StartMultipleSubProcesses()
        #         WriteDumpInfoToTheSubProcessQueue()
        #         ReadFromTheSubProcessQueue()
        #
        list_of_items_to_write = []
        list_of_item_identifiers = []
        dict_of_written_items = {}             # The checksum_key as the KEY and the VALUE is simply a flag to indicate that the "identified item" was written
        dict_of_items_read_from_queue = {}     # The checksum_key as the KEY and the VALUE is what was read from the queue minus the "status" and the "identifier". 

        slice_processing_increments = 1200
        number_of_slices = len(checksum_dict)

        file_sequence_number = 0
        slice_count = 0
        start_offset = 1

        while (number_of_slices > slice_count):
            end_offset = ((start_offset + slice_processing_increments) - 1)
            if (end_offset > number_of_slices):
                end_offset = number_of_slices
                slice_count = number_of_slices
            else:
                slice_count = end_offset

            output_file_name = Restore_stub + os.path.basename(db_file_path) + '__' + str(start_offset).zfill(9) + '_' + str(end_offset).zfill(9)

            temp_record = str(file_sequence_number) + ' :: ' + output_file_name + ' :: ' + str(start_offset) + ' :: ' + str(end_offset) + ' :: ' + ObjStore_stub + ' :: ' + location_pickle_file + ' :: ' + checksum_pickle_file + ' :: ' + duplicate_slices_pickle_file

            list_of_items_to_write.append(temp_record)
            list_of_item_identifiers.append(str(start_offset))

            start_offset = end_offset + 1
            file_sequence_number += 1

            if debug_on:
                print 'temp record:  -->' + temp_record + '<--\n'



        # Set up the parameters needed for starting multiple processing queues to concatenate the slices files together into chunks

        flag_process_unique_slices = True
        number_of_items_to_write = len(list_of_items_to_write)

        if (number_of_items_to_write > 2):
            if (number_of_items_to_write > 5):
                if (number_of_items_to_write > 10):
                    if (number_of_items_to_write > 100):
                        number_of_subprocesses = 80
                        queue_depth = 1
                    else:
                        number_of_subprocesses = 40
                        queue_depth = 1
                else:
                    number_of_subprocesses = 8
                    queue_depth = 1
            else:
                number_of_subprocesses = 3
                queue_depth = 1
        else:
            number_of_subprocesses = 1
            queue_depth = 1

        #  Write the "START" tag to the piped process that will concatenate the files together for this vos dump file
        vos_dump_file = Restore_stub + os.path.basename(db_file_path) + '__vosdumpfile'
        write_buffer = 'START :: ' + vos_dump_file + ' :: ' + str(number_of_items_to_write) + '\n'
        concatenation_pipe.stdin.write(write_buffer)
        concatenation_pipe.stdin.flush()

        if debug_on:
            print 'Wrote this to concatenation pipe -->START :: ' + vos_dump_file + ' :: ' + str(number_of_items_to_write) + '<--\n'


        dump_file_name = os.path.basename(db_file_path)
        if dump_file_name.startswith('DB__'):
            dump_file_name = dump_file_name[4:]
        print 'Begin to fetch the data to create the vos dump file: ' + dump_file_name

        dict_of_vos_dump_files[vos_dump_file] = 'Start Sending Chunks'
        #  Begin to fetch the individual slices and build them into a chunk (a contiguous sequence of slices) of data that will be concatenated by the pipe process (Concatenation_Process.py)
        StartMultipleSubProcesses(number_of_items_to_write, number_of_subprocesses, queue_depth)

        #  Write the "END" tag to the piped process that will concatenate the files together for this vos dump file
        write_buffer = 'END :: ' + vos_dump_file + '\n'
        concatenation_pipe.stdin.write(write_buffer)
        concatenation_pipe.stdin.flush()

        if debug_on:
            print 'Wrote this to concatenation pipe -->END :: ' + vos_dump_file + '<--\n'

        list_of_vos_dump_files.append(vos_dump_file)
        dict_of_vos_dump_files[vos_dump_file] = 'All Chunks Sent'


        flag_process_unique_slices = False




    #  Write the "FINISH" tag to the piped process that will concatenate the files together for this vos dump file
    write_buffer = 'FINISH :: \n'
    concatenation_pipe.stdin.write(write_buffer)
    concatenation_pipe.stdin.flush()

    #  Determine the number of vos dump files that are still in the process of being concatenated together
    number_of_files = len(dict_of_vos_dump_files)
    for file_name, status_string in dict_of_vos_dump_files.iteritems():
        if status_string  ==  'CREATED':
            number_of_files -= 1

    if number_of_files > 0:
        while True:
            #  See if the Concatenation_Process.py has written  back any status for the vos dump files that MAY have been created
            #  a read on an empty pipe returns SUCCESS but the name of the "created_dump_file" is null
            success_flag, created_dump_file = Read_Concatenation_Results()
            if success_flag:
                if created_dump_file:
                    dict_of_vos_dump_files[created_dump_file] = 'CREATED'

                    dump_file_name = os.path.basename(created_dump_file)
                    if dump_file_name.startswith('DB__'):
                        dump_file_name = dump_file_name[4:]
                    if dump_file_name.endswith('__vosdumpfile'):
                        dump_file_name = dump_file_name[:-13]
                    print 'Created the vos dump file: ' + dump_file_name

                    number_of_files = len(dict_of_vos_dump_files)
                    for file_name, status_string in dict_of_vos_dump_files.iteritems():
                        if status_string  ==  'CREATED':
                            number_of_files -= 1

                    if debug_on:
                        print 'Successfully created  ' + created_dump_file + '    ' + str(number_of_files) + ' vos dump files still need to be created\n'

                    if number_of_files == 0:
                        #  All of the vos dump files have been created
                        break

            else:
                if created_dump_file:
                    print '\n\nFATAL ERROR for the vos dump file: ' + created_dump_file + '  was received on the concatenation pipe\n\nError message:\n' + unknown_error_read_from_queue + '\n\n\n'
                else:
                    print '\n\nFATAL ERROR was received on the concatenation pipe graceful exit in process\n\nError message:\n' + unknown_error_read_from_queue + '\n\n\n'

                break                                 

