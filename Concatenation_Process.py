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
# Source code location:     /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/Concatenation_Process.py
#
# Local location:           /AFS_backups_in_Cloud/bin/Concatenation_Process.py
#
#
# Propose:
#
#   This Python program will concatenate a series of chunk files into a vos dump file
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
#   This program takes it's input from standard input and writes the result to standard output
#
#   Here is the format of the 3 unqiue messages that this program is expecting to encounter
#
#
#

#
#
#
# History:
#
#   Version 0.x     TMM   10/20/2015   code development started
#
#

# 
#=   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =

import os
import sys
import select
import threading
import shutil

import time



from threading  import Thread

try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x


ON_POSIX = 'posix' in sys.builtin_module_names


#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#  Define this class so we can assign stdout to it...   Then all "print" statments get flushed aka not buffered
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

class flushfile(object):
    def __init__(self, f):
        self.f = f
    def write(self, x):
        self.f.write(x)
        self.f.flush()




#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#  Define this class so we can concatenate chunk files as thread
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

class ConcatenateChunks(threading.Thread): 
    def __init__(self, vos_dump_file): 
        threading.Thread.__init__(self) 
        self.vos_dump_file = vos_dump_file
 

    def run(self): 

        global  dict_flag_active_thread
        global  dict_of_files_to_concatenated
        global  dict_of_file_handles

        vos_dump_fh = dict_of_file_handles[self.vos_dump_file]
        concatenate_file_list = dict_of_files_to_concatenated[self.vos_dump_file]

        for chunk_file_name in concatenate_file_list:
            with open(chunk_file_name,'rb') as chunk_fh:
                shutil.copyfileobj(chunk_fh, vos_dump_fh, 1024*1024*10)
                #10MB per writing chunk to avoid reading big file into memory.


        dict_of_file_handles[self.vos_dump_file] = vos_dump_fh

        for chunk_file_name in concatenate_file_list:
            # TMM  at this time do not delete the chunk file just rename it
            temp_file = chunk_file_name + '__tempChunk'
            os.rename(chunk_file_name, temp_file)

            # os.remove(chunk_file_name)

        del dict_of_files_to_concatenated[self.vos_dump_file]
        dict_flag_active_thread[self.vos_dump_file] = False




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

def Chunks_Need_ToBe_Concatenated():

    global  debug_thread
    global  debug_thread_level_1
    global  debug_thread_level_2
    global  debug_thread_level_3

    global  vos_dump_file
    global  dict_of_vos_dump_chunk_lists
    global  dict_of_concatenation_threads
    global  dict_of_next_chunk_sequence_number
    global  dict_flag_active_thread
    global  dict_flag_chunks_waiting


    if debug_thread:
        debug_log_fh.write('CHUNKS waiting   vos_dump_file: ' + vos_dump_file + '\n')
        debug_log_fh.flush()


    if dict_flag_active_thread[vos_dump_file]:
        #  The concatenation thread for this vos dump file is still active, we can not wait check back again later
        if debug_thread:
            debug_log_fh.write('Concatenation thread for ' + vos_dump_file + 'is still active check back again later\n')
            debug_log_fh.flush()
        return


    if vos_dump_file in dict_of_concatenation_threads:
        if debug_thread:
            debug_log_fh.write('The vos_dump_file: ' + vos_dump_file + '   has an thread construct\n')
            debug_log_fh.flush()

        #  Get the thread for this vos dump file
        concatenation_thread = dict_of_concatenation_threads[vos_dump_file]
        
        if debug_thread and debug_thread_level_1:
            debug_log_fh.write('WAIT   still working on concatenating files from a previous thread\n')
            debug_log_fh.flush()

        #  WAIT on concatenation thread...
        #  This call will block if we are still working on concatenating files from a previous call to concatenate a list of chunk files
        #  There should be NO blocking because of the status of the flag in dict_flag_active_thread[vos_dump_file]
        concatenation_thread.join()

        if debug_thread and debug_thread_level_1:
            debug_log_fh.write('The WAIT is over from a previous thread to concatenate files\n')
            debug_log_fh.flush()


    #  Get the list for chunk file manifest for this vos dump file
    list_of_chunk_files = dict_of_vos_dump_chunk_lists[vos_dump_file]
    next_sequence_number = dict_of_next_chunk_sequence_number[vos_dump_file]
    number_of_chunks = len(list_of_chunk_files)
    most_recent_sequence_number = next_sequence_number

    if debug_thread:
        debug_log_fh.write('next_sequence_number: ' + str(next_sequence_number) + '     number_of_chunks: ' + str(number_of_chunks) + '\n')
        debug_log_fh.flush()


    list_of_files_to_concatenate = []
    for sequence_number in range(next_sequence_number, number_of_chunks):
        if debug_thread and debug_thread_level_3:
            debug_log_fh.write('  sequence_number: ' + str(sequence_number) + '     value: ' + list_of_chunk_files[sequence_number] + '\n')
            debug_log_fh.flush()
    
        if not list_of_chunk_files[sequence_number] == 'empty':
            if not list_of_chunk_files[sequence_number] == 'concatenated':
                if debug_thread and debug_thread_level_2:
                    debug_log_fh.write('Add to the list_of_files_to_concatenate    from sequence_number: ' + str(sequence_number) + '     value: ' + list_of_chunk_files[sequence_number] + '\n')
                    debug_log_fh.flush()

                list_of_files_to_concatenate.append(list_of_chunk_files[sequence_number])
                list_of_chunk_files[sequence_number] = 'concatenated'
                most_recent_sequence_number = sequence_number
        else:
            break


    if list_of_files_to_concatenate:
        if debug_thread:
            debug_log_fh.write('Number of files to concatenate: ' + str(len(list_of_files_to_concatenate)) + '     most_recent_sequence_number : ' + str(most_recent_sequence_number) + '\n')
            debug_log_fh.flush()

        dict_of_vos_dump_chunk_lists[vos_dump_file] = list_of_chunk_files
        dict_of_files_to_concatenated[vos_dump_file] = list_of_files_to_concatenate
        dict_of_next_chunk_sequence_number[vos_dump_file] = most_recent_sequence_number

        if not vos_dump_file in dict_of_concatenation_threads:
            if debug_thread and debug_thread_level_1:
                debug_log_fh.write('CREATE a concatenation thread for the vos_dump_file: ' + vos_dump_file + '\n')
                debug_log_fh.flush()

            #  CREATE a concatenation thread for this vos dump file
            concatenation_thread = ConcatenateChunks(vos_dump_file)
            dict_of_concatenation_threads[vos_dump_file] = concatenation_thread
            dict_flag_active_thread[vos_dump_file] = True

            if debug_thread:
                debug_log_fh.write('START the concatenation thread for the vos_dump_file: ' + vos_dump_file + '\n')
                debug_log_fh.flush()

            #  START the concatenation thread for this vos dump file
            concatenation_thread.start()
        else:
            if debug_thread:
                debug_log_fh.write('RUN the concatenation thread for the vos_dump_file: ' + vos_dump_file + '\n')
                debug_log_fh.flush()

            #  RUN the concatenation thread (again) for this vos dump file
            concatenation_thread.run()

    else:
        if debug_thread:
            debug_log_fh.write('No files in sequence to concatenate\n')
            debug_log_fh.flush()

    ###   flag_chunks_waiting = False
    dict_flag_chunks_waiting[vos_dump_file] = False








if __name__ == "__main__":

    debug_on = False
    debug_empty_stdin = False

    debug_thread = False
    debug_thread_level_1 = False
    debug_thread_level_2 = False
    debug_thread_level_3 = False

    #  KEY is the name of the vos dump file   the VALUE is the file handle to the vos dump file that is being created from the chunks
    dict_of_file_handles = {}

    #  KEY is the name of the vos dump file   the VALUE is a list that contains the chunk files (list_of_chunk_files) that are being process to create the vos dump file     
    dict_of_vos_dump_chunk_lists = {}

    #  KEY is the name of the vos dump file   the VALUE is the next chunk sequence number to process
    dict_of_next_chunk_sequence_number = {}

    #  KEY is the name of the vos dump file   the VALUE is a list that contains the names of the chunk files that will be concatenated to the end of the vos dump file that is being created 
    dict_of_files_to_concatenated = {}

    #  KEY is the name of the vos dump file   the VALUE is an active THREAD construct that is concatenating files
    # 
    #        dict_of_concatenation_threads   --   Active threads for each vos dump file that is being created
    #        dict_of_threads_finishing       --   Concatenation thread that is adding the last chunks to the vos dump file
    #        dict_of_threads_completed       --   All of the chunk files have been concatenated to the vos dump file

    dict_of_concatenation_threads = {}
    dict_of_threads_finishing = {}
    dict_of_threads_completed = {}

    #  KEY is the name of the vos dump file   the VALUE is boolean True/False used to indicate that the thread is actively concatenating chunk files together
    dict_flag_active_thread = {}

    #  KEY is the name of the vos dump file   the VALUE is boolean True/False used to indicate that there are chunk files that need to be concatenated
    dict_flag_chunks_waiting = {}



    #  Index is the sequence number that will assemble the chunks into the vos dump file
    list_of_chunk_files = []


    flag_all_done = False

    path_list = os.path.split(str(sys.argv[0:1])[2:-2])
    program_name = str(path_list[1:2])[2:-3]


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



    #  Set up queue and a thread to read from stdin
    stdin_queue = Queue()
    stdin_thread = Thread(target=enqueue_output, args=(sys.stdin, stdin_queue))
    stdin_thread.daemon = True # thread dies with the program
    stdin_thread.start()

    #
    #  If there's input ready, then read in the input record (slice offset, slice number, slice length)
    #
    #  If there is nothing to read see if any chunk files need to be concatenated
    #  
    #  To begin this while loop set up a place holder entry for the dictionaries
    #

    vos_dump_file = 'place_holder'
    dict_flag_chunks_waiting[vos_dump_file] = False

    flag_keep_waiting = True


    while flag_keep_waiting:
        if debug_on:
            debug_msg = 'reading from stdin\n'
            debug_log_fh.write(debug_msg)
            debug_log_fh.flush()



        #  Now read from stdin without blocking    or for 25 msec    stdin_queue.get(timeout=0.25)
        try:  line = stdin_queue.get_nowait()
        except Empty:
            #
            #  There wasn't anything to read from the stdin pipe
            #
            #  So then if there are chunk files that can be concatenated spawn a thread to do that
            #  
            if debug_empty_stdin:
                debug_log_fh.write('empty read buffer...\n')
                debug_log_fh.flush()


            if dict_flag_chunks_waiting[vos_dump_file]:
                Chunks_Need_ToBe_Concatenated()
            else:
                if not dict_of_concatenation_threads:
                    if dict_of_threads_finishing:
                        list_of_vos_dump_files = []
                        dict_of_threads = {}
                        for vos_dump_file, concatenation_thread in dict_of_threads_finishing.iteritems():
                            list_of_vos_dump_files.append(vos_dump_file)
                            dict_of_threads[vos_dump_file] = concatenation_thread

                        for vos_dump_file in list_of_vos_dump_files:
                            if dict_flag_active_thread[vos_dump_file]:
                                #  The concatenation thread for this vos dump file is still finishing up, we can not wait check back again later
                                if debug_thread:
                                    debug_log_fh.write('Concatenation thread is still finishing up for ' + vos_dump_file + '\n')
                                    debug_log_fh.flush()
                                continue
                            else:                                
                                #  WAIT on concatenation thread...
                                #  This call will block if we are still finishing up on the concatenation of the last chunk files
                                #  There should be NO blocking because of the status of the flag in dict_flag_active_thread[vos_dump_file]
                                concatenation_thread = dict_of_threads[vos_dump_file]
                                concatenation_thread.join()

                                if debug_thread:
                                    debug_log_fh.write('COMPLETED  concatenating files for  ' + vos_dump_file + '  \n')
                                    debug_log_fh.flush()

                                dict_of_threads_completed[vos_dump_file] = dict_of_threads_finishing[vos_dump_file]
                                del dict_of_threads_finishing[vos_dump_file]

                                #  All the chunks have been concatenated so close the temp file and rename it, then send a SUCCESS message for this vos dump file
                                vos_dump_fh = dict_of_file_handles[vos_dump_file]
                                vos_dump_fh.close()
                                temp_file = vos_dump_file + '__temp'
                                os.rename(temp_file, vos_dump_file)        

                                msg = 'SUCCESS :: ' + vos_dump_file + '\n'

                                if debug_on:
                                    debug_msg = 'Wrote to stdout\n-------\n' + msg + '-------\n'
                                    debug_log_fh.write(debug_msg)
                                    debug_log_fh.flush()

                                sys.stdout.write(msg)
                                sys.stdout.flush()

                    else:
                        if not flag_all_done:
                            #  Looks like all the vos dump files have been concatenated... Verify if this is correct
                            number_of_vos_dump_files = len(dict_of_file_handles)
                            number_of_completed_concatenations = len(dict_of_threads_completed)

                            if debug_thread:
                                debug_log_fh.write('Number of vos dump files: ' + str(number_of_vos_dump_files) + '     completed: ' + str(number_of_completed_concatenations) + '\n')
                                debug_log_fh.flush()

                            if number_of_completed_concatenations == number_of_vos_dump_files:
                                flag_all_done = True                          
                            else:
                                # Fatal error
                                if debug_on:
                                    debug_msg = 'Fatal Error  number of vos dump files: ' + str(number_of_vos_dump_files) + '   NOT equal to number completed: ' + str(number_of_completed_concatenations) + '\n'
                                    debug_log_fh.write(debug_msg)
                                    debug_log_fh.flush()

                                msg = 'ERROR :: ' + vos_dump_file + ' :: number of vos dump files: ' + str(number_of_vos_dump_files) + '   NOT equal to number completed: ' + str(number_of_completed_concatenations) + '\n'
                                sys.stdout.write(msg)
                                sys.stdout.flush()

                                #  Sleep for 5 seconds then self terminate
                                if debug_on:
                                    debug_log_fh.close()
                                time.sleep(5.25)
                                sys.exit(1)

                        if debug_empty_stdin:
                            debug_log_fh.write('All the threads have completed...\n')
                            debug_log_fh.flush()                        


                else:
                    if debug_empty_stdin:
                        debug_log_fh.write('Threads are still accepting new chunks...\n')
                        debug_log_fh.flush()

        else: 
            # ... do something with line
            if debug_on:
                debug_msg = 'stdin contents:   ==>' + line + '<==\n'
                debug_log_fh.write(debug_msg)
                debug_log_fh.flush()

            if line:
                line = line.rstrip('\r|\n')
                record = line.split(' :: ')
                if 'START :: ' in line:

                    vos_dump_file = record[1]
                    number_of_chunk_files = int(record[2])

                    list_of_chunk_files = []
                    for index in range(0, number_of_chunk_files):
                        list_of_chunk_files.append('empty')

                    dict_of_vos_dump_chunk_lists[vos_dump_file] = list_of_chunk_files

                    temp_file = vos_dump_file + '__temp'
                    if os.path.isfile(temp_file):
                        os.remove(temp_file)

                    vos_dump_fh = open(temp_file,'ab')
                    dict_of_file_handles[vos_dump_file] = vos_dump_fh
                    dict_of_next_chunk_sequence_number[vos_dump_file] = 0
                    dict_flag_active_thread[vos_dump_file] = False
                    dict_flag_chunks_waiting[vos_dump_file] = False

                    #  Get rid of this palce holder if it exists
                    if 'place_holder' in  dict_flag_chunks_waiting:
                        del dict_flag_chunks_waiting['place_holder']


                elif 'CHUNK :: ' in line:
                    if debug_on:
                        debug_msg = 'stdin contents:   ==>' + line + '<==\n'
                        debug_log_fh.write(debug_msg)
                        debug_log_fh.flush()

                    file_name = record[1]
                    if vos_dump_file == file_name:
                        chunk_sequence_number = int(record[2])
                        chunk_file_name = record[3]

                        list_of_chunk_files = dict_of_vos_dump_chunk_lists[vos_dump_file]
                        if list_of_chunk_files[chunk_sequence_number] == 'empty':
                            list_of_chunk_files[chunk_sequence_number] = chunk_file_name
                            dict_of_vos_dump_chunk_lists[vos_dump_file] = list_of_chunk_files
                            ###   flag_chunks_waiting = True
                            dict_flag_chunks_waiting[vos_dump_file] = True
                        else:
                            if debug_on:
                                debug_msg = 'ERROR  this chunk file information was already sent:  -->' + line + '<--\n'
                                debug_log_fh.write(debug_msg)
                                debug_log_fh.flush()

                            msg = 'ERROR :: ' + vos_dump_file + ' :: this chunk file information was already sent:  ==>' + line + '<==\n'
                            sys.stdout.write(msg)
                            sys.stdout.flush()

                            #  Sleep for 5 seconds then self terminate
                            if debug_on:
                                debug_log_fh.close()
                            time.sleep(5.25)
                            sys.exit(1)
                    else:
                        if debug_on:
                            debug_msg = 'ERROR  The name of vos dump file that was sent in the CHUNK command (' + file_name + ') did not match the expected file name (' + vos_dump_file + ')\n\nHere is what was sent  -->' + line + '<--\n'
                            debug_log_fh.write(debug_msg)
                            debug_log_fh.flush()

                        msg = 'ERROR :: ' + vos_dump_file + ' :: The name of vos dump file that was sent in the CHUNK command (' + file_name + ') did not match the expected file name (' + vos_dump_file + ')\n'
                        sys.stdout.write(msg)
                        sys.stdout.flush()

                        #  Sleep for 5 seconds then self terminate
                        if debug_on:
                            debug_log_fh.close()
                        time.sleep(5.25)
                        sys.exit(1)



                elif 'END :: ' in line:
                    if debug_on:
                        debug_msg = 'stdin contents:   ==>' + line + '<==\n'
                        debug_log_fh.write(debug_msg)
                        debug_log_fh.flush()

                    file_name = record[1]
                    if vos_dump_file == file_name:
                        list_of_chunk_files = dict_of_vos_dump_chunk_lists[vos_dump_file]
                        next_sequence_number = dict_of_next_chunk_sequence_number[vos_dump_file]
                        number_of_chunks = len(list_of_chunk_files)
                        number_of_chunks -= (next_sequence_number + 1)

                        if debug_on:
                            debug_msg = 'The vos dump file:  ' + vos_dump_file + '  has concatenated  ' + str(next_sequence_number + 1) + '  chunk files and has  ' + str(number_of_chunks) + '  remaining to be concatenated\n'
                            debug_log_fh.write(debug_msg)
                            debug_log_fh.flush()

                        if not number_of_chunks  ==  (next_sequence_number + 1):
                            #  Make a pass through to see if there are chunk files that still need to be concatenated
                            ###   flag_chunks_waiting = True
                            dict_flag_chunks_waiting[vos_dump_file] = True
                            Chunks_Need_ToBe_Concatenated()
                            dict_of_threads_finishing[vos_dump_file] = dict_of_concatenation_threads[vos_dump_file]
                            del dict_of_concatenation_threads[vos_dump_file]
                        else:
                            #  Send a SUCCESS message that this vos dump file has been created from its slices
                            dict_of_threads_completed[vos_dump_file] = dict_of_concatenation_threads[vos_dump_file]
                            del dict_of_concatenation_threads[vos_dump_file]

                            #  All the chunks have been concatenated so close the temp file and rename it
                            vos_dump_fh = dict_of_file_handles[vos_dump_file]
                            vos_dump_fh.close()
                            temp_file = vos_dump_file + '__temp'
                            os.rename(temp_file, vos_dump_file)        

                            msg = 'SUCCESS :: ' + vos_dump_file + '\n'

                            if debug_on:
                                debug_msg = 'Wrote to stdout\n-------\n' + msg + '-------\n'
                                debug_log_fh.write(debug_msg)
                                debug_log_fh.flush()

                            sys.stdout.write(msg)
                            sys.stdout.flush()


                    else:
                        if debug_on:
                            debug_msg = 'ERROR  The name of vos dump file that was sent in the END command (' + file_name + ') did not match the expected file name (' + vos_dump_file + ')\n\nHere is what was sent  -->' + line + '<--\n'
                            debug_log_fh.write(debug_msg)
                            debug_log_fh.flush()

                        msg = 'ERROR :: ' + vos_dump_file + ' :: The name of vos dump file that was sent in the END command (' + file_name + ') did not match the expected file name (' + vos_dump_file + ')\n'
                        sys.stdout.write(msg)
                        sys.stdout.flush()

                        #  Sleep for 5 seconds then self terminate
                        if debug_on:
                            debug_log_fh.close()
                        time.sleep(5.25)
                        sys.exit(1)




                elif 'FINISH :: ' in line:
                    if debug_on:
                        debug_msg = 'Received FINISH command\n'
                        debug_log_fh.write(debug_msg)
                        debug_log_fh.flush()

                    flag_keep_waiting = False
                    list_of_all_vos_dump_files = []
                    for vos_dump_file, flag_value in dict_flag_chunks_waiting.iteritems():
                        list_of_all_vos_dump_files.append(vos_dump_file)





                elif 'IGNORE :: ' in line:
                    if debug_on:
                        debug_msg = 'Received IGNORE command\n'
                        debug_log_fh.write(debug_msg)
                        debug_log_fh.flush()




                else:
                    if debug_on:
                        debug_msg = 'Some unexpected input was received on the pipe:   ==>' + line + '<==\n'
                        debug_log_fh.write(debug_msg)
                        debug_log_fh.flush()

                    msg = 'ERROR :: ' + vos_dump_file + ' :: some unexpected input was received on the pipe:  ==>' + line + '<==\n'
                    sys.stdout.write(msg)
                    sys.stdout.flush()

                    #  Sleep for 5 seconds then self terminate
                    if debug_on:
                        debug_log_fh.close()
                    time.sleep(5.25)
                    sys.exit(1)

            else:
                # an empty line means that the stdin pipe has been closed
                if debug_on:
                    debug_log_fh.write('An empty line was received on the stdin pipe, this indicates that the parent process has closed the pipe\n')
                    debug_log_fh.flush()

                sys.exit(1)




    flag_keep_waiting = True

    while flag_keep_waiting:
        for vos_dump_file in list_of_all_vos_dump_files:
            if dict_flag_chunks_waiting[vos_dump_file]:
                Chunks_Need_ToBe_Concatenated()
            else:
                if not dict_of_concatenation_threads:
                    if dict_of_threads_finishing:
                        list_of_vos_dump_files = []
                        dict_of_threads = {}
                        for vos_dump_file, concatenation_thread in dict_of_threads_finishing.iteritems():
                            list_of_vos_dump_files.append(vos_dump_file)
                            dict_of_threads[vos_dump_file] = concatenation_thread

                        for vos_dump_file in list_of_vos_dump_files:
                            if dict_flag_active_thread[vos_dump_file]:
                                #  The concatenation thread for this vos dump file is still finishing up, we can not wait check back again later
                                if debug_thread:
                                    debug_log_fh.write('Concatenation thread is still finishing up for ' + vos_dump_file + '\n')
                                    debug_log_fh.flush()
                                continue
                            else:                                
                                #  WAIT on concatenation thread...
                                #  This call will block if we are still finishing up on the concatenation of the last chunk files
                                #  There should be NO blocking because of the status of the flag in dict_flag_active_thread[vos_dump_file]
                                concatenation_thread = dict_of_threads[vos_dump_file]
                                concatenation_thread.join()

                                if debug_thread:
                                    debug_log_fh.write('COMPLETED  concatenating files for  ' + vos_dump_file + '  \n')
                                    debug_log_fh.flush()

                                dict_of_threads_completed[vos_dump_file] = dict_of_threads_finishing[vos_dump_file]
                                del dict_of_threads_finishing[vos_dump_file]

                                #  All the chunks have been concatenated so close the temp file and rename it, then send a SUCCESS message for this vos dump file
                                vos_dump_fh = dict_of_file_handles[vos_dump_file]
                                vos_dump_fh.close()
                                temp_file = vos_dump_file + '__temp'
                                os.rename(temp_file, vos_dump_file)        

                                msg = 'SUCCESS :: ' + vos_dump_file + '\n'

                                if debug_on:
                                    debug_msg = 'Wrote to stdout\n-------\n' + msg + '-------\n'
                                    debug_log_fh.write(debug_msg)
                                    debug_log_fh.flush()

                                sys.stdout.write(msg)
                                sys.stdout.flush()

                    else:
                        if not flag_all_done:
                            #  Looks like all the vos dump files have been concatenated... Verify if this is correct
                            number_of_vos_dump_files = len(dict_of_file_handles)
                            number_of_completed_concatenations = len(dict_of_threads_completed)

                            if debug_thread:
                                debug_log_fh.write('Number of vos dump files: ' + str(number_of_vos_dump_files) + '     completed: ' + str(number_of_completed_concatenations) + '\n')
                                debug_log_fh.flush()

                            if number_of_completed_concatenations == number_of_vos_dump_files:
                                flag_all_done = True                          
                            else:
                                # Fatal error
                                if debug_on:
                                    debug_msg = 'Fatal Error  number of vos dump files: ' + str(number_of_vos_dump_files) + '   NOT equal to number completed: ' + str(number_of_completed_concatenations) + '\n'
                                    debug_log_fh.write(debug_msg)
                                    debug_log_fh.flush()

                                msg = 'ERROR :: ' + vos_dump_file + ' :: number of vos dump files: ' + str(number_of_vos_dump_files) + '   NOT equal to number completed: ' + str(number_of_completed_concatenations) + '\n'
                                sys.stdout.write(msg)
                                sys.stdout.flush()

                                #  Sleep for 5 seconds then self terminate
                                if debug_on:
                                    debug_log_fh.close()
                                time.sleep(5.25)
                                sys.exit(1)

                        if debug_empty_stdin:
                            debug_log_fh.write('All the threads have completed...\n')
                            debug_log_fh.flush()

                        #  All the threads have completed
                        flag_keep_waiting = False           #  This will force the "for loop" to terminate
                        break                               #  This will force the "for loop" to break out

                else:
                    if debug_empty_stdin:
                        debug_log_fh.write('Threads are still accepting new chunks...\n')
                        debug_log_fh.flush()





    if debug_on:
        debug_log_fh.close()




