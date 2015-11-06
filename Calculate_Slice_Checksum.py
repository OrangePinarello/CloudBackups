#!/usr/bin/env python
#
#    $Header: /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/RCS/Calculate_Slice_Checksum.py,v 1.1 2015/07/20 21:35:54 root Exp $
#
#    $Revision: 1.1 $
#
#    $Date: 2015/07/20 21:35:54 $
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
# Source code location:     /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/Calculate_Slice_Checksum.py
#
# Local location:           /AFS_backups_in_Cloud/bin/Calculate_Slice_Checksum.py
#
#
# Propose:
#
#   This Python program will be directed by a calling program to calculate the checksum value for the
#   slice using 2 methods (SHA-1 and MD5), these checksum values will be used for lookup within the
#   backup database and the ObjectStore (AWS S3).
#
#
#
#
# Logic overview:
#
#   After this program has been startred the calling program communicates within via a PIPEd stdin.
#
#   Passing via stdin the following 5 data elements:
#
#        The slices SHA-1 checksum
#        The slices MD5 checksum
#        The length of the slice
#        The offset into the vos dump image file where the slice is located
#        The slice number
#
#
#   After this program has compressed and encrypted the slice, it will communicate back to the calling
#   program via a PIPEd stdout the results
#
#   Passing via stdout the following 10 data elements:
#
#          SHA1 checksum of the uncompressed and unencrypted slice file
#          MD5 checksum of the uncompressed and unencrypted slice file
#          Slice file size in terms of bytes
#
#          Compressed and encrypted file size in terms of bytes
#          Slice file in terms of 1KB blocks
#          Compressed and encrypted file size in terms of 1KB blocks
#
#          Offset of this slice within the vos dump file
#          Slice number
#          Status  Duplicate or Unique  slice file within the context of this vos dump file
#
#          The name of the  compressed and encrypted slice file within the object store
#
#
# Command Line Parameters:
#
#   This program takes these additional optional parameters
#
#     1)  The name of the name of the AFS volume dump file
#     2)  The path to the ObjectStore
#     3)  The encryption salt
#
#
#
#
# History:
#
#   Version 0.x     TMM   02/26/2015   code development started
#
#   Version 1.1     TMM   mm/dd/2015
#
#        Completed
#
#
#
#=   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =   =

import os
import sys
import getopt
import hashlib
import base64
import zlib
import binascii
import time
import random
import select

from sys import argv
from Crypto.Cipher import AES




# ========
# ========
def  ProcessCommandLine(argv, program_name):

    input_file_path = ''


    try:
        opts, args = getopt.getopt(argv,"hf:",["help","file="])
    except getopt.GetoptError:
        print ' ' + program_name + '  --help   --file <vos dump file>    --obj <path to object store>    --salt <Do Not Forget this string>'
        sys.exit(1)

    dryrun = False
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print ' ' + program_name + '  --help   --file <input file>    --obj <path to object store>    --salt <Do Not Forget this string>'
            sys.exit(0)
        elif opt in ("-f", "--file"):
            input_file_path = arg


    if not input_file_path:
        msg = 'ERROR   Must specify the name of the vos dump file'
        print msg + '\n ' + program_name + '  --help   --file <input file>    --obj <path to object store>    --salt <Do Not Forget this string>'
        sys.exit(1)


    return(input_file_path)
    return(input_file_path, meta_file_path, salt)




# ========
# ========
def  CalculateSliceChecksum(slice_offset, slice_size, debug_on):

    global fh_vos_dump
    global file_size_in_bytes

    file_offset = fh_vos_dump.tell()

    if file_offset  !=  slice_offset:
        fh_vos_dump.seek(slice_offset, os.SEEK_SET)
        file_offset = fh_vos_dump.tell()
        if file_offset  !=  slice_offset:
            return_msg_buffer = 'ERROR   Unable to seek to starting offset'
            if debug_on:
                debug_log_fh.write(return_msg_buffer + '\n')
                debug_log_fh.flush()

            return (return_msg_buffer)


    db_slice_record = ''
    ending_offset = file_offset + slice_size

    if file_size_in_bytes  >=  ending_offset:
        slice_buffer = fh_vos_dump.read(slice_size)
        calculated_offset = file_offset + slice_size
        current_offset = fh_vos_dump.tell()

        #  Make sure that the "real" file offset is what we have calculated it to be.  If not then serious error condition
        if current_offset  !=  calculated_offset:
            if debug_on:
                debug_msg = 'File offset before read: ' + str(file_offset) + '\n'
                debug_msg = debug_msg + 'File offset after read:  ' + str(current_offset) + '\n'
                debug_msg = debug_msg + 'Calculated file offset:  ' + str(calculated_offset) + '\n\n'
                debug_msg = debug_msg + 'asked for buffer size:  ' + str(slice_size) + '\n'
                debug_msg = debug_msg + 'returned buffer size:   ' + str(len(slice_buffer)) + '\n'
                debug_log_fh.write(debug_msg)
                debug_log_fh.flush()

            return_msg_buffer = 'ERROR   File offset: ' + str(current_offset) + '     does not match calculated file offset: ' + str(calculated_offset)
            if debug_on:
                debug_log_fh.write(return_msg_buffer + '\n')
                debug_log_fh.flush()

            return (return_msg_buffer)

        #  Calculate the MD5 and SHA-1 checksum values for the contents of the slice buffer
        hash_handle = hashlib.md5()
        hash_handle.update(slice_buffer)
        value_md5 = hash_handle.hexdigest()

        hash_handle = hashlib.sha1()
        hash_handle.update(slice_buffer)
        value_sha1 = hash_handle.hexdigest()

        record = str(value_sha1) + ' :: ' + str(value_md5) + ' :: ' + str(slice_size)

    else:
        return_msg_buffer = 'ERROR   Ending offset: ' + str(ending_offset) + '     is past the end of the file: ' + str(file_size_in_bytes)
        if debug_on:
            debug_log_fh.write(return_msg_buffer + '\n')
            debug_log_fh.flush()

        return (return_msg_buffer)

    return (record)




if __name__ == "__main__":


    # TMM  to enable debugging  (set to True)
    debug_on = False


    path_list = os.path.split(str(sys.argv[0:1])[2:-2])
    program_name = str(path_list[1:2])[2:-3]
    if debug_on:
        #  Since this program's  stdin and stdout is setup as a PIPE the only way
        #  to debug it is to log everything a debug log file in /tmp
        short_program_name = program_name.replace('.py', '')
        my_pid = os.getpid()
        debug_log = '/tmp/' + short_program_name + '__' + str(my_pid) + '.debug_log'
        debug_log_fh = open(debug_log, "w")
        debug_log_fh.write('Starting...\n')
        debug_log_fh.flush()

    input_file_path = ProcessCommandLine(sys.argv[1:], program_name)

    if not os.path.isfile(input_file_path):
        error_msg = 'ERROR   Unable to find the vos dump file:  ' + input_file_path + '\n'
        if debug_on:
            debug_log_fh.write(error_msg)
            debug_log_fh.flush()

        sys.stdout.write(error_msg)
        sys.stdout.flush()
        sys.exit(1)

    if debug_on:
        debug_log_fh.write('Found vos dump file: ' + str(input_file_path) + '\n')
        debug_log_fh.flush()
 
    path_list = os.path.split(input_file_path)
    vosdump_file_name = str(path_list[1:2])[2:-3]

    st = os.stat(input_file_path)
    file_size_in_bytes = st.st_size

    if debug_on:
        debug_msg = 'Opening dump file name:  ' + input_file_path + '\n'
        debug_log_fh.write(debug_msg)
        debug_log_fh.flush()

    fh_vos_dump = open(input_file_path, "rb")

    if debug_on:
        debug_msg = 'dump file opened:  ' + input_file_path + '\n'
        debug_log_fh.write(debug_msg)
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
            debug_msg = 'stdin contents:   --->' + line + '<---\n'
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
            input_record = line.split(' :: ')
            slice_number = int(input_record[0])
            slice_offset = int(input_record[1])
            slice_length = int(input_record[2])

            if debug_on:
                debug_msg = 'Call  CalculateSliceChecksum    slice number: ' + str(slice_number) + '\n'
                debug_log_fh.write(debug_msg)
                debug_log_fh.flush()

            checksum_key = CalculateSliceChecksum(slice_offset, slice_length, debug_on)

            #  The format of the slice information that is returned is comprised of these 5 elements
            #
            #  The first 3 fields are used as a lookup key  (checksum_key)
            #
            #     SHA1 checksum of the uncompressed and unencrypted slice file
            #     MD5 checksum of the uncompressed and unencrypted slice file
            #     Slice file size in terms of bytes
            #
            #     Offset of this slice within the vos dump file
            #     Slice number
            #
            slice_info = str(checksum_key) + ' :: ' + str(slice_offset) + ' :: ' + str(slice_number) + '\n'

            if debug_on:
                debug_msg = 'Write to stdout: ' + slice_info
                debug_log_fh.write(debug_msg)
                debug_log_fh.flush()

            sys.stdout.write(slice_info)
            sys.stdout.flush()

    if debug_on:
        debug_log_fh.close()

    fh_vos_dump.close()
    sys.exit(0)
