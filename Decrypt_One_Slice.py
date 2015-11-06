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
# Source code location:     /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/Decrypt_One_Slice.py
#
# Local location:           /AFS_backups_in_Cloud/bin/Decrypt_One_Slice.py
#
#
# Propose:
#
#   This Python program will decrypt and uncompress a file, which is a slice of a file that was the output
#   from the AFS "vos dump" process.
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
#   Here is the format of the 6 input parameters using this 4 character field sperator  " :: "
#
#
#     1)  Object identifier (slice_tag)     khmW/user.thoyt__201505291040__0-0__521
#
#     2)  SHA-1 checksum after the slice is decrypted and uncompressed
#
#     3)  md5 checksum after the slice is decrypted and uncompressed
#
#     4)  Full path file name where the decrypted and uncompressed slice file will be written to (created by this program)
#
#     5)  Stub location of the object store (/AFS_backups_in_Cloud/ObjectStore) used together
#         with the slice tag will provide the location within the object store for the slice
#
#     6)  Salt to used to decrypt the slice
#
#
#
#
# History:
#
#   Version 0.x     TMM   12/18/2014   code development started
#
#
#   Version 1.1     TMM   12/22/2014
#
#        Working code uses 3 checksum values (md5, SHA-1 and SHA384) to validate that the slice file was decrypted
#
#
#   Version 1.2     TMM   06/16/2015
#
#        Now only using md5 and SHA-1 to validate
#
#
#   Version 1.3     TMM   10/08/2015
#
#        Change the interface the program now reads it's input parameters from standard input (stdin) and writes the
#        status result back out to standard output (stdout)
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

from sys import argv
from Crypto.Cipher import AES





# ========
# ========
def decryptionAES256(secret_key, input_string):

    global debug_decrypt

    size_of_extra = int(input_string[0:2])
    offset = size_of_extra + 2
    extra_buffer = input_string[2:offset]
    encrypted_string = input_string[offset:]

    if debug_decrypt:
        debug_msg = '  ---  DecryptionAES256  ---\n'
        debug_msg = debug_msg + 'Input buffer size: ' + str(len(input_string)) + '\n'
        debug_msg = debug_msg + 'Size of extra: ' + str(size_of_extra) + '\n'
        debug_msg = debug_msg + 'Offset: ' + str(offset) + '\n'

        printable_buffer = binascii.b2a_hex(extra_buffer)

        debug_msg = debug_msg + 'Extra buffer size: ' + str(len(extra_buffer)) + '    buffer contents: ' + printable_buffer + '\n'
        debug_msg = debug_msg + 'Encrypted string size: ' + str(len(encrypted_string)) + '\n'
        debug_msg = debug_msg + 'Secret Key: ' + secret_key + '\n'
        debug_log_fh.write(debug_msg)
        debug_log_fh.flush()

    padding_character = ''
    DecodeAES = lambda c, e: c.decrypt(e)
    cipher = AES.new(secret_key)
    decoded = DecodeAES(cipher, encrypted_string)

    if debug_decrypt:
        debug_msg = 'decrypted buffer size: ' + str(len(decoded)) + '\n'
        debug_log_fh.write(debug_msg)
        debug_log_fh.flush()

    output_buffer = decoded + extra_buffer

    if debug_decrypt:
        debug_msg = 'Output buffer size: ' + str(len(output_buffer)) + '\n  - + - + - + - + - + - + - + - + - + - + - +\n\n'
        debug_log_fh.write(debug_msg)
        debug_log_fh.flush()

    return (output_buffer)




if __name__ == "__main__":

    debug_on = False
    debug_decrypt = False
    debug_empty_stdin = False

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

        if not line:
            if debug_empty_stdin:
                debug_log_fh.write('empty read buffer...\n')
                debug_log_fh.flush()

            time.sleep(1.5)
        elif 'STOP' in line:
            if debug_on:
                debug_log_fh.write('Received STOP command...\n')
                debug_log_fh.flush()

            flag_keep_waiting = False
        else:
            if debug_on:
                debug_msg = 'stdin contents:   ==>' + line + '<==\n'
                debug_log_fh.write(debug_msg)
                debug_log_fh.flush()

            line = line.rstrip('\r|\n')
            input_record = line.split(' :: ')

            item_identifier = input_record[0]
            slice_tag = input_record[1]
            expected_sha1_value = input_record[2]
            expected_md5_value = input_record[3]
            output_file = input_record[4]
            object_store = input_record[5]
            sodium = input_record[6]

            #           slice_tag:     khmW/user.thoyt__201505291040__0-0__521
            #     
            #        object_store:     /AFS_backups_in_Cloud/ObjectStore

    
            #    object_store_key:     khmW
            #    object_directory:     /AFS_backups_in_Cloud/ObjectStore/k/h/m
            #     slice_file_name:     user.thoyt__201505291040__0-0__521


            object_store_key = str(slice_tag.split('/')[0:1])[2:-2]
            slice_file_name = str(slice_tag.split('/')[1:2])[2:-2]
            object_directory = object_store + object_store_key[0] + '/' + object_store_key[1] + '/' + object_store_key[2]

            #   /AFS_backups_in_Cloud/ObjectStore/k/h/m/user.thoyt__201505291040__0-0__521
            slice_file_path = object_directory + '/' + slice_file_name

            if debug_on:
                temp_string = 'file name:  ' + slice_file_name + '\n' + 'object store key:  ' + object_store_key + '\n' + 'object directory:  ' + object_directory + '\n'
                debug_log_fh.write(temp_string)
                debug_log_fh.flush()


            if not os.path.isfile(slice_file_path):
                msg = 'ERROR :: ' + item_identifier + ' :: unable to find the object file:  ' + slice_file_path + '\n'
            else:
                hash_handle = hashlib.md5()
                hash_handle.update(slice_file_name)
                file_name_hash = base64.urlsafe_b64encode(hash_handle.hexdigest())

                slice_short_name = object_store_key + '/' + slice_file_name

                hash_handle = hashlib.md5()
                hash_handle.update(file_name_hash + slice_short_name + sodium)
                encryption_key = (base64.urlsafe_b64encode(hash_handle.hexdigest()))[9:41]


                #  Open the encrypted file and read it back and decrypted it
                #  Then uncompress the decrypted buffer and write it out to a file
                st = os.stat(slice_file_path)
                slice_fh = open(slice_file_path, 'rb')
                encrypted_buffer = slice_fh.read(st.st_size)
                slice_fh.close()

                decrypted_slice_buffer = decryptionAES256(encryption_key, encrypted_buffer)
                slice_buffer = zlib.decompress(decrypted_slice_buffer)

                #  Calculate 2 checksum values for the (decrypted and uncompressed) slice_buffer  md5, sha1
                hash_handle = hashlib.md5()
                hash_handle.update(slice_buffer)
                value_md5 = hash_handle.hexdigest()

                hash_handle = hashlib.sha1()
                hash_handle.update(slice_buffer)
                value_sha1 = hash_handle.hexdigest()

                #  Compare the expected values with the calculated values
                if expected_md5_value == value_md5  and  expected_sha1_value == value_sha1:
                    slice_fh = open(output_file, 'wb')
                    slice_fh.write(slice_buffer)
                    slice_fh.close()

                    msg = 'SUCCESS :: ' + item_identifier + ' :: ' + output_file + '\n'
                else:
                    if expected_md5_value  !=  value_md5:
                        if expected_sha1_value  !=  value_sha1:
                            msg = 'ERROR :: ' + item_identifier + ' :: Mismatch with both the SHA-1 and md5 checksums for  ' + slice_tag + '\n'
                        else:
                            msg = 'ERROR :: ' + item_identifier + ' :: Mismatch with md5 checksum for  ' + slice_tag + '\n'
                    else:
                        msg = 'ERROR :: ' + item_identifier + ' :: Mismatch with SHA-1 checksum for  ' + slice_tag + '\n'


            sys.stdout.write(msg)
            sys.stdout.flush()

            if debug_on:
                debug_msg = 'Wrote to stdout\n-------\n' + msg + '-------\n'
                debug_log_fh.write(debug_msg)
                debug_log_fh.flush()