#!/usr/bin/env python
#
#    $Header: /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/RCS/Begin_AFSbackup_Slice_and_Dice.py,v 1.1 2015/07/20 21:42:05 root Exp $
#
#    $Revision: 1.1 $
#
#    $Date: 2015/07/20 21:42:05 $
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
# Source code location:     /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/Begin_AFSbackup_Slice_and_Dice.py
#
# Local location:           /AFS_backups_in_Cloud/bin/Begin_AFSbackup_Slice_and_Dice.py
#
#
# Propose:
#
#   This Python program will work it's way through the directory tree where the AFS vos dump files have just been created
#   When it finds a vos dump file it will begin the process of placing that vos dump file into the Object Store.
#
#   Part of that process includes unrolling the vos dump file and processing the individual files within.  Hence we increase
#   the storage efficiency by deduping common data segments at the file level and sharing those common data segments amongest
#   multiple dump levels of the same AFS volume.
#
#
# Logic overview:
#
#
#
#
#
# History:
#
#   Version 0.x     TMM   04/15/2015   code development started
#
#   Version 1.1     TMM   07/20/2015
#
#   Version 1.2     TMM   11/04/2015   Add a lock file to indicate to other programs that the contents of the "Database" and "ObjectStore"
#                                      are in the process of being modified.  Also the presence of this file could be and indicator that
#                                      the program had abended (crashed) the last time it was run.
#
#
#=   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =

import os
import sys
import getopt
import time
import subprocess
import socket

from datetime import datetime
from sys import argv

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

def  ReadFromTheSubProcessQueue(queue_name, queue_enabled_flag, queue_write_count, count_of_jobs_queued, subprocess_q):

    global  debug_on

    global  list_of_records
    global  volumes_being_processed


    if queue_enabled_flag  and  count_of_jobs_queued  !=  0:
        while queue_write_count != 0  and  count_of_jobs_queued  !=  0:
            ###  try:  returned_record = subprocess_q.get_nowait()

            try:  returned_record = subprocess_q.get(timeout=0.1)
            except Empty:
                if debug_empty:
                    print 'Empty no record on ' + queue_name + '\n'

                break
            else:
                if debug_on:
                    print 'Record read from ' + queue_name + ' <--  ' + returned_record

                if 'ERROR '  in returned_record:
                    # On an error condition from trying to create the slice,  an error message is passed back via: "record"
                    print returned_record
                    sys.exit(1)
                elif 'DEBUG '  in returned_record:
                    print returned_record
                else:
                    #  Take the database record that was passed back from the subprocess  VOS_Dump_Slice_Queue.py
                    #  and temporarily place it in a list for further processing
                    queue_write_count -= 1
                    count_of_jobs_queued -= 1
                    list_of_records.append(returned_record.rstrip('\r|\n'))
                    returned_record = returned_record.rstrip('\r|\n')
                    record = returned_record.split(' :: ')
                    afs_volume_name = record[3]
                    if debug_on:
                        print 'Remove afs volume  ' + afs_volume_name + '  from the dictionary'
                    del volumes_being_processed[afs_volume_name]


    return(queue_write_count, count_of_jobs_queued)




#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

def  WriteDumpInfoToTheSubProcessQueue(queue_name, queue_enabled_flag, queue_write_count, files_to_process,  depth_of_queue, count_of_jobs_queued, max_queued, slicer_subprocess):

    global  debug_on

    global  list_of_vos_dump_files_to_process
    global  list_of_db_directories
    global  list_vos_dump_file_sizes
    global  list_afs_volume_names
    global  volumes_being_processed


    if queue_enabled_flag:
        send_vos_dump_info = ''
        while files_to_process  >  0  and  queue_write_count < depth_of_queue  and  count_of_jobs_queued  !=  max_queued:
            vos_dump_file = list_of_vos_dump_files_to_process.pop(0)
            slice_db_directory_path = list_of_db_directories.pop(0)
            vos_dump_file_size = list_vos_dump_file_sizes.pop(0)
            afs_volume_name = list_afs_volume_names.pop(0)

            if volumes_being_processed.has_key(afs_volume_name):
                list_of_vos_dump_files_to_process.append(vos_dump_file)
                list_of_db_directories.append(slice_db_directory_path)
                list_vos_dump_file_sizes.append(vos_dump_file_size)
                list_afs_volume_names.append(afs_volume_name)
                break
            else:
                if debug_on:
                    print 'Add afs volume  ' + afs_volume_name + '  to the dictionary'
                volumes_being_processed[afs_volume_name] = 1
                send_vos_dump_info = send_vos_dump_info + vos_dump_file + ' :: ' +  slice_db_directory_path + ' :: ' +  str(vos_dump_file_size) + ' :: ' +  afs_volume_name + '\n'
                queue_write_count += 1
                count_of_jobs_queued += 1
                files_to_process -= 1

        if send_vos_dump_info:
            slicer_subprocess.stdin.write(send_vos_dump_info)
            slicer_subprocess.stdin.flush()

            if debug_on:
                print 'Wrote this to ' + queue_name + ' -->  ' + send_vos_dump_info
                print queue_name + ' write count: '  + str(queue_write_count) + '     Over All write count: '  + str(count_of_jobs_queued) + '     remaining dump files: ' + str(files_to_process) + '\n'


    return(queue_write_count, files_to_process, count_of_jobs_queued)




#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

def  StartMultipleSubProcesses():

    global  debug_on
    global  debug_empty

    global  list_of_vos_dump_files_to_process
    global  list_of_db_directories
    global  list_vos_dump_file_sizes
    global  list_afs_volume_names
    global  list_of_records
    global  volumes_being_processed

    global  processed_vos_dump_files
    global  processed_db_directory_path
    global  processed_afs_volume_name
    global  failed_vos_dump_files
    global  failed_db_directory_path
    global  failed_afs_volume_name


    number_of_dump_files_to_process = len(list_of_vos_dump_files_to_process)

    # for now it's always 20 sub processes with queue depth of 1
    subprocess_thread_count = 20
    write_queue_depth = 1

    MAX_QUEUED = write_queue_depth * subprocess_thread_count

    flag_proc1_enabled = True
    flag_proc2_enabled = True
    flag_proc3_enabled = True
    flag_proc4_enabled = True
    flag_proc5_enabled = True
    flag_proc6_enabled = True
    flag_proc7_enabled = True
    flag_proc8_enabled = True
    flag_proc9_enabled = True
    flag_proc10_enabled = True
    flag_proc11_enabled = True
    flag_proc12_enabled = True
    flag_proc13_enabled = True
    flag_proc14_enabled = True
    flag_proc15_enabled = True
    flag_proc16_enabled = True
    flag_proc17_enabled = True
    flag_proc18_enabled = True
    flag_proc19_enabled = True
    flag_proc20_enabled = True

    proc1_write_count = 0
    proc2_write_count = 0
    proc3_write_count = 0
    proc4_write_count = 0
    proc5_write_count = 0
    proc6_write_count = 0
    proc7_write_count = 0
    proc8_write_count = 0
    proc9_write_count = 0
    proc10_write_count = 0
    proc11_write_count = 0
    proc12_write_count = 0
    proc13_write_count = 0
    proc14_write_count = 0
    proc15_write_count = 0
    proc16_write_count = 0
    proc17_write_count = 0
    proc18_write_count = 0
    proc19_write_count = 0
    proc20_write_count = 0

    ProcessingNode_1 ='/AFS_backups_in_Cloud/bin/VOS_Dump_Slice_Queue.py'
    ProcessingNode_2 ='/usr/bin/ssh root@afsbk-cloud2.cc.nd.edu -q /AFS_backups_in_Cloud/bin/VOS_Dump_Slice_Queue.py'
    ProcessingNode_3 ='/usr/bin/ssh root@afsbk-cloud3.cc.nd.edu -q /AFS_backups_in_Cloud/bin/VOS_Dump_Slice_Queue.py'
    ProcessingNode_4 ='/usr/bin/ssh root@afsbk-cloud4.cc.nd.edu -q /AFS_backups_in_Cloud/bin/VOS_Dump_Slice_Queue.py'
    ProcessingNode_5 ='/usr/bin/ssh root@afsbk-cloud5.cc.nd.edu -q /AFS_backups_in_Cloud/bin/VOS_Dump_Slice_Queue.py'
    ProcessingNode_6 ='/usr/bin/ssh root@afsbk-cloud6.cc.nd.edu -q /AFS_backups_in_Cloud/bin/VOS_Dump_Slice_Queue.py'
    ProcessingNode_7 ='/usr/bin/ssh root@afsbk-cloud7.cc.nd.edu -q /AFS_backups_in_Cloud/bin/VOS_Dump_Slice_Queue.py'
    ProcessingNode_8 ='/usr/bin/ssh root@afsbk-cloud8.cc.nd.edu -q /AFS_backups_in_Cloud/bin/VOS_Dump_Slice_Queue.py'
    ProcessingNode_9 ='/usr/bin/ssh root@afsbk-cloud9.cc.nd.edu -q /AFS_backups_in_Cloud/bin/VOS_Dump_Slice_Queue.py'
    ProcessingNode_10 ='/usr/bin/ssh root@afsbk-cloud10.cc.nd.edu -q /AFS_backups_in_Cloud/bin/VOS_Dump_Slice_Queue.py'


    #  Configured subprocess that will slice up the vos dump image
    #
    if flag_proc1_enabled:
        slicer_proc1 = Popen([ProcessingNode_1], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc1 = Queue()
        t_proc1 = Thread(target=enqueue_output, args=(slicer_proc1.stdout, q_proc1))
        t_proc1.daemon = True
        t_proc1.start()

    if flag_proc2_enabled:
        slicer_proc2 = Popen([ProcessingNode_2], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc2 = Queue()
        t_proc2 = Thread(target=enqueue_output, args=(slicer_proc2.stdout, q_proc2))
        t_proc2.daemon = True
        t_proc2.start()

    if flag_proc3_enabled:
        slicer_proc3 = Popen([ProcessingNode_3], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc3 = Queue()
        t_proc3 = Thread(target=enqueue_output, args=(slicer_proc3.stdout, q_proc3))
        t_proc3.daemon = True
        t_proc3.start()

    if flag_proc4_enabled:
        slicer_proc4 = Popen([ProcessingNode_4], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc4 = Queue()
        t_proc4 = Thread(target=enqueue_output, args=(slicer_proc4.stdout, q_proc4))
        t_proc4.daemon = True
        t_proc4.start()

    if flag_proc5_enabled:
        slicer_proc5 = Popen([ProcessingNode_5], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc5 = Queue()
        t_proc5 = Thread(target=enqueue_output, args=(slicer_proc5.stdout, q_proc5))
        t_proc5.daemon = True
        t_proc5.start()

    if flag_proc6_enabled:
        slicer_proc6 = Popen([ProcessingNode_6], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc6 = Queue()
        t_proc6 = Thread(target=enqueue_output, args=(slicer_proc6.stdout, q_proc6))
        t_proc6.daemon = True
        t_proc6.start()

    if flag_proc7_enabled:
        slicer_proc7 = Popen([ProcessingNode_7], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc7 = Queue()
        t_proc7 = Thread(target=enqueue_output, args=(slicer_proc7.stdout, q_proc7))
        t_proc7.daemon = True
        t_proc7.start()

    if flag_proc8_enabled:
        slicer_proc8 = Popen([ProcessingNode_8], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc8 = Queue()
        t_proc8 = Thread(target=enqueue_output, args=(slicer_proc8.stdout, q_proc8))
        t_proc8.daemon = True
        t_proc8.start()

    if flag_proc9_enabled:
        slicer_proc9 = Popen([ProcessingNode_9], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc9 = Queue()
        t_proc9 = Thread(target=enqueue_output, args=(slicer_proc9.stdout, q_proc9))
        t_proc9.daemon = True
        t_proc9.start()

    if flag_proc10_enabled:
        slicer_proc10 = Popen([ProcessingNode_10], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc10 = Queue()
        t_proc10 = Thread(target=enqueue_output, args=(slicer_proc10.stdout, q_proc10))
        t_proc10.daemon = True
        t_proc10.start()

    if flag_proc11_enabled:
        slicer_proc11 = Popen([ProcessingNode_1], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc11 = Queue()
        t_proc11 = Thread(target=enqueue_output, args=(slicer_proc11.stdout, q_proc11))
        t_proc11.daemon = True
        t_proc11.start()

    if flag_proc12_enabled:
        slicer_proc12 = Popen([ProcessingNode_2], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc12 = Queue()
        t_proc12 = Thread(target=enqueue_output, args=(slicer_proc12.stdout, q_proc12))
        t_proc12.daemon = True
        t_proc12.start()

    if flag_proc13_enabled:
        slicer_proc13 = Popen([ProcessingNode_3], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc13 = Queue()
        t_proc13 = Thread(target=enqueue_output, args=(slicer_proc13.stdout, q_proc13))
        t_proc13.daemon = True
        t_proc13.start()

    if flag_proc14_enabled:
        slicer_proc14 = Popen([ProcessingNode_4], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc14 = Queue()
        t_proc14 = Thread(target=enqueue_output, args=(slicer_proc14.stdout, q_proc14))
        t_proc14.daemon = True
        t_proc14.start()

    if flag_proc15_enabled:
        slicer_proc15 = Popen([ProcessingNode_5], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc15 = Queue()
        t_proc15 = Thread(target=enqueue_output, args=(slicer_proc15.stdout, q_proc15))
        t_proc15.daemon = True
        t_proc15.start()

    if flag_proc16_enabled:
        slicer_proc16 = Popen([ProcessingNode_6], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc16 = Queue()
        t_proc16 = Thread(target=enqueue_output, args=(slicer_proc16.stdout, q_proc16))
        t_proc16.daemon = True
        t_proc16.start()

    if flag_proc17_enabled:
        slicer_proc17 = Popen([ProcessingNode_7], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc17 = Queue()
        t_proc17 = Thread(target=enqueue_output, args=(slicer_proc17.stdout, q_proc17))
        t_proc17.daemon = True
        t_proc17.start()

    if flag_proc18_enabled:
        slicer_proc18 = Popen([ProcessingNode_8], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc18 = Queue()
        t_proc18 = Thread(target=enqueue_output, args=(slicer_proc18.stdout, q_proc18))
        t_proc18.daemon = True
        t_proc18.start()

    if flag_proc19_enabled:
        slicer_proc19 = Popen([ProcessingNode_9], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc19 = Queue()
        t_proc19 = Thread(target=enqueue_output, args=(slicer_proc19.stdout, q_proc19))
        t_proc19.daemon = True
        t_proc19.start()

    if flag_proc20_enabled:
        slicer_proc20 = Popen([ProcessingNode_10], stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize=1, close_fds=ON_POSIX, shell = True)
        q_proc20 = Queue()
        t_proc20 = Thread(target=enqueue_output, args=(slicer_proc20.stdout, q_proc20))
        t_proc20.daemon = True
        t_proc20.start()

    current_write_count = 0


    #  Now go and do stuff

    while number_of_dump_files_to_process  >  0:
        if current_write_count  !=  MAX_QUEUED:
            proc1_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 1", flag_proc1_enabled, proc1_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc1)
            proc2_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 2", flag_proc2_enabled, proc2_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc2)
            proc3_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 3", flag_proc3_enabled, proc3_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc3)
            proc4_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 4", flag_proc4_enabled, proc4_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc4)
            proc5_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 5", flag_proc5_enabled, proc5_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc5)
            proc6_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 6", flag_proc6_enabled, proc6_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc6)
            proc7_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 7", flag_proc7_enabled, proc7_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc7)
            proc8_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 8", flag_proc8_enabled, proc8_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc8)
            proc9_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 9", flag_proc9_enabled, proc9_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc9)
            proc10_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 10", flag_proc10_enabled, proc10_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc10)
            proc11_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 11", flag_proc11_enabled, proc11_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc11)
            proc12_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 12", flag_proc12_enabled, proc12_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc12)
            proc13_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 13", flag_proc13_enabled, proc13_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc13)
            proc14_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 14", flag_proc14_enabled, proc14_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc14)
            proc15_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 15", flag_proc15_enabled, proc15_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc15)
            proc16_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 16", flag_proc16_enabled, proc16_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc16)
            proc17_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 17", flag_proc17_enabled, proc17_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc17)
            proc18_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 18", flag_proc18_enabled, proc18_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc18)
            proc19_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 19", flag_proc19_enabled, proc19_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc19)
            proc20_write_count, number_of_dump_files_to_process, current_write_count = WriteDumpInfoToTheSubProcessQueue("queue 20", flag_proc20_enabled, proc20_write_count, number_of_dump_files_to_process, write_queue_depth, current_write_count, MAX_QUEUED, slicer_proc20)

        #  See if any database records are in the stack that need to be processed
        while len(list_of_records)  !=  0:
            returned_record = list_of_records.pop(0)

            # TMM debug 
            print 'Returned record:  ==>>' + returned_record + '<<==\n'

            record = returned_record.split(' :: ')
            returned_status_code = int(record[0])
            returned_vos_dump_file = record[1]
            returned_slice_db_directory_path = record[2]
            returned_afs_volume_name = record[3]

            if returned_status_code  ==  0:
                processed_vos_dump_files.append(returned_vos_dump_file)
                processed_db_directory_path.append(returned_slice_db_directory_path)
                processed_afs_volume_name.append(returned_afs_volume_name)
            else:
                failed_vos_dump_files.append(returned_vos_dump_file)
                failed_db_directory_path.append(returned_slice_db_directory_path)
                failed_afs_volume_name.append(returned_afs_volume_name)

        #  Check on the subprocesses wait on them until they have something to read
        #
        flag_waiting_to_read = True
        previous_write_count = current_write_count

        while flag_waiting_to_read:
            if previous_write_count  !=  current_write_count:
                #  One subprocess had something which we read, take one more pass thur to find any others then go write something to the sub processibng queues
                flag_waiting_to_read = False

            proc1_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 1", flag_proc1_enabled, proc1_write_count, current_write_count, q_proc1)
            proc2_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 2", flag_proc2_enabled, proc2_write_count, current_write_count, q_proc2)
            proc3_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 3", flag_proc3_enabled, proc3_write_count, current_write_count, q_proc3)
            proc4_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 4", flag_proc4_enabled, proc4_write_count, current_write_count, q_proc4)
            proc5_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 5", flag_proc5_enabled, proc5_write_count, current_write_count, q_proc5)
            proc6_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 6", flag_proc6_enabled, proc6_write_count, current_write_count, q_proc6)
            proc7_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 7", flag_proc7_enabled, proc7_write_count, current_write_count, q_proc7)
            proc8_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 8", flag_proc8_enabled, proc8_write_count, current_write_count, q_proc8)
            proc9_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 9", flag_proc9_enabled, proc9_write_count, current_write_count, q_proc9)
            proc10_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 10", flag_proc10_enabled, proc10_write_count, current_write_count, q_proc10)
            proc11_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 11", flag_proc11_enabled, proc11_write_count, current_write_count, q_proc11)
            proc12_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 12", flag_proc12_enabled, proc12_write_count, current_write_count, q_proc12)
            proc13_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 13", flag_proc13_enabled, proc13_write_count, current_write_count, q_proc13)
            proc14_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 14", flag_proc14_enabled, proc14_write_count, current_write_count, q_proc14)
            proc15_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 15", flag_proc15_enabled, proc15_write_count, current_write_count, q_proc15)
            proc16_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 16", flag_proc16_enabled, proc16_write_count, current_write_count, q_proc16)
            proc17_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 17", flag_proc17_enabled, proc17_write_count, current_write_count, q_proc17)
            proc18_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 18", flag_proc18_enabled, proc18_write_count, current_write_count, q_proc18)
            proc19_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 19", flag_proc19_enabled, proc19_write_count, current_write_count, q_proc19)
            proc20_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 20", flag_proc20_enabled, proc20_write_count, current_write_count, q_proc20)


    #  Clean up at the end, now check on the subprocesses that are processing the last vos dump files
    #
    if debug_on:
        remaining_to_process = proc1_write_count + proc2_write_count + proc3_write_count + proc4_write_count + proc5_write_count + proc6_write_count + proc7_write_count + proc8_write_count
        print 'Number of vos dump files currently being sliced (' + str(current_write_count) + ') and the number of outstanding vos dump files remaining to be processed (' + str(remaining_to_process) + ')\n'

    while current_write_count  >  0:
        proc1_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 1", flag_proc1_enabled, proc1_write_count, current_write_count, q_proc1)
        proc2_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 2", flag_proc2_enabled, proc2_write_count, current_write_count, q_proc2)
        proc3_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 3", flag_proc3_enabled, proc3_write_count, current_write_count, q_proc3)
        proc4_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 4", flag_proc4_enabled, proc4_write_count, current_write_count, q_proc4)
        proc5_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 5", flag_proc5_enabled, proc5_write_count, current_write_count, q_proc5)
        proc6_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 6", flag_proc6_enabled, proc6_write_count, current_write_count, q_proc6)
        proc7_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 7", flag_proc7_enabled, proc7_write_count, current_write_count, q_proc7)
        proc8_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 8", flag_proc8_enabled, proc8_write_count, current_write_count, q_proc8)
        proc9_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 9", flag_proc9_enabled, proc9_write_count, current_write_count, q_proc9)
        proc10_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 10", flag_proc10_enabled, proc10_write_count, current_write_count, q_proc10)
        proc11_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 11", flag_proc11_enabled, proc11_write_count, current_write_count, q_proc11)
        proc12_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 12", flag_proc12_enabled, proc12_write_count, current_write_count, q_proc12)
        proc13_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 13", flag_proc13_enabled, proc13_write_count, current_write_count, q_proc13)
        proc14_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 14", flag_proc14_enabled, proc14_write_count, current_write_count, q_proc14)
        proc15_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 15", flag_proc15_enabled, proc15_write_count, current_write_count, q_proc15)
        proc16_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 16", flag_proc16_enabled, proc16_write_count, current_write_count, q_proc16)
        proc17_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 17", flag_proc17_enabled, proc17_write_count, current_write_count, q_proc17)
        proc18_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 18", flag_proc18_enabled, proc18_write_count, current_write_count, q_proc18)
        proc19_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 19", flag_proc19_enabled, proc19_write_count, current_write_count, q_proc19)
        proc20_write_count, current_write_count = ReadFromTheSubProcessQueue("queue 20", flag_proc20_enabled, proc20_write_count, current_write_count, q_proc20)


        if current_write_count  >  0:
            #  Verify that that there are still subprocess that we suspect still are processing the last of the vos dump files
            #  this is like a fail safe so we don't end up looping forever
            remaining_to_be_read = proc1_write_count + proc2_write_count + proc3_write_count + proc4_write_count + proc5_write_count + proc6_write_count + proc7_write_count + proc8_write_count + proc9_write_count + proc10_write_count + proc11_write_count + proc12_write_count + proc13_write_count + proc14_write_count + proc15_write_count + proc16_write_count + proc17_write_count + proc18_write_count + proc19_write_count + proc20_write_count
            if remaining_to_be_read  !=  current_write_count:
                print 'ERROR:  stuck in endless loop mismatch between  current_write_count (' + str(current_write_count) + ') and the number of outstanding vos dump files to be read (' + str(remaining_to_be_read) + ')\n'
                sys.exit(1)
            else:
                #  Go sleep and and then check the subprocesses again
                time.sleep(0.2)


    #  Clean up at the end, process the last of the created database records in the stack
    #
    while len(list_of_records)  !=  0:
        returned_record = list_of_records.pop(0)
        record = returned_record.split(' :: ')
        returned_status_code = int(record[0])
        returned_vos_dump_file = record[1]
        returned_slice_db_directory_path = record[2]
        returned_afs_volume_name = record[3]
        if returned_status_code  ==  0:
            processed_vos_dump_files.append(returned_vos_dump_file)
            processed_db_directory_path.append(returned_slice_db_directory_path)
            processed_afs_volume_name.append(returned_afs_volume_name)
        else:
            failed_vos_dump_files.append(returned_vos_dump_file)
            failed_db_directory_path.append(returned_slice_db_directory_path)
            failed_afs_volume_name.append(returned_afs_volume_name)


    #  Send a STOP message to all the subprocesses

    if debug_on:
        print 'Send STOP message to the  VOS_Dump_Slice_Queue  subprocesses...\n'

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
    if flag_proc9_enabled:
        slicer_proc9.stdin.write(stop_message)
        slicer_proc9.stdin.flush()
    if flag_proc10_enabled:
        slicer_proc10.stdin.write(stop_message)
        slicer_proc10.stdin.flush()
    if flag_proc11_enabled:
        slicer_proc11.stdin.write(stop_message)
        slicer_proc11.stdin.flush()
    if flag_proc12_enabled:
        slicer_proc12.stdin.write(stop_message)
        slicer_proc12.stdin.flush()
    if flag_proc13_enabled:
        slicer_proc13.stdin.write(stop_message)
        slicer_proc13.stdin.flush()
    if flag_proc14_enabled:
        slicer_proc14.stdin.write(stop_message)
        slicer_proc14.stdin.flush()
    if flag_proc15_enabled:
        slicer_proc15.stdin.write(stop_message)
        slicer_proc15.stdin.flush()
    if flag_proc16_enabled:
        slicer_proc16.stdin.write(stop_message)
        slicer_proc16.stdin.flush()
    if flag_proc17_enabled:
        slicer_proc17.stdin.write(stop_message)
        slicer_proc17.stdin.flush()
    if flag_proc18_enabled:
        slicer_proc18.stdin.write(stop_message)
        slicer_proc18.stdin.flush()
    if flag_proc19_enabled:
        slicer_proc19.stdin.write(stop_message)
        slicer_proc19.stdin.flush()
    if flag_proc20_enabled:
        slicer_proc20.stdin.write(stop_message)
        slicer_proc20.stdin.flush()



#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

def  ProcessCommandLine(argv, program_name):

 
    parameter_type = ''
    input_file_path = ''
    directory_path = ''

    help01 = '  --help\n\n'
    help02 = '  --file          <file containing the names of the vos dump files to process>\n\n'
    help03 = '  --dir           <full path to the directory where to begin looking for vos dump files to process>\n\n\n'
    help04 = '  --defaultDIR    begin searching for vos dump files in the default directory  /AFS_backups_in_Cloud/DailyDump/IMAGES\n\n\n'
    help05 = '  The --file option and --dir are mutually exclusive\n\n'


    help_msg = help01 + help02 + help03 + help04

    try:
        opts, args = getopt.getopt(argv,"hf:d:D",["help","file=","dir=","defaultDIR"])
    except getopt.GetoptError:
        print ' ' + program_name + '\n\n' + help_msg
        sys.exit(1)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print ' ' + program_name + '\n\n' + help_msg
            sys.exit(0)
        elif opt in ("-f", "--file"):
            input_file_path = arg
        elif opt in ("-d", "--dir"):
            directory_path = arg
        elif opt in ("-D", "--defaultDIR"):
            directory_path = '/AFS_backups_in_Cloud/DailyDump/IMAGES'
            directory_path = '/AFS_backups_in_Cloud/DailyDump/DEV/IMAGES'

    if not input_file_path:
        if not directory_path:
            msg = 'Must specify either a input file  OR  a directory path'
            print msg + '\n\n ' + program_name + '\n\n' + help_msg
            sys.exit(1)
        else:
            parameter_type = 'DIRECTORY'
            return_string = directory_path
    else:
        if not directory_path:
            parameter_type = 'FILE'
            return_string = input_file_path
        else:
            msg = 'Can not specify both a input file  AND  a directory path'
            print msg + '\n\n ' + program_name + '\n\n' + help_msg
            sys.exit(1)


    return(parameter_type, return_string)




#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

if __name__ == "__main__":


    #  assign stdout to this class (flushfile)...   Then all "print" statments get flushed aka not buffered
    sys.stdout = flushfile(sys.stdout)


    # TMM  to enable debugging  (set to True)
    debug_on = False
    debug_empty = False

    #  Check to see if there is a lock in place to indicate that another program (or perhaps yesterdays run of this program that crashed)
    #  is actively in the middle of making modifications to the vos dump database files and to the contents of the "ObjectStore"
    lock_file_name = '/AFS_backups_in_Cloud/Log/LOCK_FILE'
    if os.path.isfile(lock_file_name):
        sys.stderr.write('ERROR   Unable to start the lock file:  ' + lock_file_name + '  exists\n')
        sys.exit(1)


    list_of_vos_dump_files_to_process = []
    list_of_db_directories = []
    list_vos_dump_file_sizes = []
    list_afs_volume_names = []
    list_of_records = []
    volumes_being_processed = {}

    processed_vos_dump_files = []
    processed_db_directory_path = []
    processed_afs_volume_name = []
    failed_vos_dump_files = []
    failed_db_directory_path = []
    failed_afs_volume_name = []


    path_list = os.path.split(str(sys.argv[0:1])[2:-2])
    program_name = str(path_list[1:2])[2:-3]

    parameter_type, path = ProcessCommandLine(sys.argv[1:], program_name)


    #  Create the LOCK file so that other programs that I have created don't clobber the databases's and the ObjectStore
    #
    msg = '\n\nThe lock was applied on the ObjectStore and it\'s Database(s)\n\n'
    msg = msg + 'The lock was created by  ' + program_name + '\n\n'
    msg = msg + 'On the host  ' + socket.getfqdn() + '\n\n'
    msg = msg + 'With process id  ' + str(os.getpid()) + '    at ' + datetime.now().strftime("%I:%M%p on %B %d, %Y") + '\n'
    lock_file_fh = open(lock_file_name, 'w')
    lock_file_fh.write(msg)
    lock_file_fh.close()


    if parameter_type == 'DIRECTORY':
        # Set the directory you want to start from
        vos_dump_directory = path

        # The file extension to be used as an argument for walk to match against.  The meta file would indicate that the dump has been completed
        file_extension = '.meta'

        for dirName, subdirList, fileList in os.walk(vos_dump_directory):
            for fname in fileList:
                if fname.endswith(file_extension):
                    #  store where the slice database should be located, try both the development location and the production location
                    if 'DailyDump/IMAGES'  in dirName:
                        slice_db_path = dirName.replace("DailyDump/IMAGES", "Database")
                    elif 'DailyDump/DEV/IMAGES'  in dirName:
                        slice_db_path = dirName.replace("DailyDump/DEV/IMAGES", "Database")
                    else:
                        print 'ERROR  unexpected directory name:  ' + dirName
                        sys.exit(1)

                    #  store the full path to the vos dump file to process
                    vos_dump_name, fileExtension = os.path.splitext(fname)
                    vos_dump_file = '%s/%s' % (dirName, vos_dump_name)
                    list_of_vos_dump_files_to_process.append(vos_dump_file)
                    list_of_db_directories.append(slice_db_path)

                    #  save the size of the vos dump file
                    vos_dump_file_size = os.path.getsize(vos_dump_file)
                    list_vos_dump_file_sizes.append(vos_dump_file_size)

                    #  save the name of the AFS volume
                    volume_name = vos_dump_name[:-22]
                    list_afs_volume_names.append(volume_name)
                    print 'vos dump file:  ' + vos_dump_file + '     volume: ' + volume_name

    elif parameter_type == 'FILE':
        fh_input_file = open(path, 'r')

        for line in fh_input_file:
            vos_dump_file = line.rstrip('\r|\n')
            if not os.path.isfile(vos_dump_file):
                print 'WARNING  unable to find the vos dump file:  ' + vos_dump_file
                continue

            dirName, fileName = os.path.split(vos_dump_file)

            #  store where the slice database should be located, try both the development location and the production location
            if 'DailyDump/IMAGES'  in dirName:
                slice_db_path = dirName.replace("DailyDump/IMAGES", "Database")
            elif 'DailyDump/DEV/IMAGES'  in dirName:
                slice_db_path = dirName.replace("DailyDump/DEV/IMAGES", "Database")
            else:
                print 'WARNING  unexpected directory name:  ' + dirName
                continue    


            list_of_vos_dump_files_to_process.append(vos_dump_file)
            list_of_db_directories.append(slice_db_path)    

            #  save the size of the vos dump file
            vos_dump_file_size = os.path.getsize(vos_dump_file)
            list_vos_dump_file_sizes.append(vos_dump_file_size)

            #  save the name of the AFS volume
            volume_name = fileName[:-22]
            list_afs_volume_names.append(volume_name)
            print 'vos dump file:  ' + vos_dump_file + '     volume: ' + volume_name

        fh_input_file.close
    else:
        print 'Unexpected parameter type:  ' + parameter_type + '   expecting either DIRECTORY  or  FILE\n\n'
        sys.exit(1)

    #  Begin to take the vos dump files and slice them up and stuff them into the object store
    StartMultipleSubProcesses()

    # After the vos dump files have been sliced and stuffed into the object store, it's time to delete them
    number_of_failures = len(failed_afs_volume_name)
    if number_of_failures > 0:
        print 'Had  ' + str(number_of_failures) + '  vos dump files that encountered processing errors'

        while number_of_failures > 0:
            number_of_failures -= 1
            vos_dump_file = failed_vos_dump_files.pop(0)
            volume_name = failed_afs_volume_name.pop(0)

            print 'FAILED:  ' + vos_dump_file + '     volume: ' + volume_name

    success_count = len(processed_afs_volume_name)

    print 'Successfully processed  ' + str(success_count) + '  vos dump files.  Now begin to remove them'

    while success_count > 0:
        success_count -= 1
        vos_dump_file = processed_vos_dump_files.pop(0)
        volume_name =  processed_afs_volume_name.pop(0)

        meta_file = vos_dump_file + '.meta'

        print 'Removing vos dump file:  ' + vos_dump_file + '     volume: ' + volume_name

        if os.path.exists(vos_dump_file):
            try:
                #   os.remove(vos_dump_file)
                print 'TMM  -  disabled the removal of the file:  ' + vos_dump_file
            except OSError, e:
                print ("ERROR: %s - %s." % (e.filename, e.strerror))
        else:
            print("WARNING,  unable to find %s file." % vos_dump_file)    


        print 'Removing meta file:  ' + meta_file + '     volume: ' + volume_name

        if os.path.exists(meta_file):
            try:
                #   os.remove(meta_file)
                print 'TMM  -  disabled the removal of the file:  ' + meta_file
            except OSError, e:
                print ("ERROR: %s - %s." % (e.filename,e.strerror))
        else:
            print("WARNING,  unable to find %s file." % meta_file)    

    #  Remove the LOCK file upon successful completion
    os.remove(lock_file_name)

    sys.exit(0)
