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
# Source code location:     /afs/nd.edu/user7/sysadmin/Private/NetApp/Scripts/Decrypt_Multiple_Slices.py
#
# Local location:           /usr/local/bin/Decrypt_Multiple_Slices.py
#
#
# Propose:
#
#   This Python program will decrypt and uncompress multiple "slice" files.
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
#   Here is the format of the 9 input parameters using this 4 character field sperator  " :: "
#
#     1)  Object identifier (the starting slice number)
#
#     2)  The file sequence number for the output file that will be created
#     3)  The file name for the output file that will be created
#
#     4)  The slice number to begin processing (uncompress and decrypt)
#     5)  The slice number of the last slice to process
#
#     6)  Stub location of the object store (/AFS_backups_in_Cloud/ObjectStore) used together with
#         the slice tag will provide the location within the object store where to find the slice
#
#     7)  file name for the pickle file for the dictionary location_dict
#     8)  file name for the pickle file for the dictionary checksum_dict
#     9)  file name for the pickle file for the dictionary dict_of_duplicate_slices
#
#
#
#
# History:
#
#   Version 0.x     TMM   10/14/2015   code development started
#
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

import pickle

from sys import argv
from Crypto.Cipher import AES





# ========
#
#  For performance improvement to reduce the number of times that memory buffers are copyied, this function
#  utilizes global variables.  Therefore we are no longer passing the encrypted data (encrypted_buffer) as
#  an input parameter and returning (passing back) the decrypted data (decrypted_buffer)
#
# ========
def decryptionAES256(secret_key):

    global  debug_decrypt
    global  encrypted_buffer
    global  decrypted_buffer

    size_of_extra = int(encrypted_buffer[0:2])
    offset = size_of_extra + 2
    extra_buffer = encrypted_buffer[2:offset]
    encrypted_string = encrypted_buffer[offset:]

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

    decrypted_buffer = decoded + extra_buffer

    if debug_decrypt:
        debug_msg = 'Output buffer size: ' + str(len(output_buffer)) + '\n  - + - + - + - + - + - + - + - + - + - + - +\n\n'
        debug_log_fh.write(debug_msg)
        debug_log_fh.flush()






if __name__ == "__main__":

    debug_on = True
    debug_decrypt = False
    debug_empty_stdin = False

    dict_of_decrypted_slices = {}
    encrypted_buffer = ''
    decrypted_buffer = ''

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
    first_time = True

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
            file_sequence_number = input_record[1]
            output_file_name = input_record[2]

            start_slice_number = int(input_record[3])
            end_slice_number = int(input_record[4])

            object_store = input_record[5]

            if first_time:
                #  Load the dictionaries
                location_pickle_file = input_record[6]
                checksum_pickle_file = input_record[7]
                duplicate_slices_pickle_file = input_record[8]
                sodium = ""

                fh_input = open(location_pickle_file, 'rb')
                location_dict = pickle.load(fh_input)
                fh_input.close()

                fh_input = open(checksum_pickle_file, 'rb')
                checksum_dict = pickle.load(fh_input)
                fh_input.close()

                fh_input = open(duplicate_slices_pickle_file, 'rb')
                dict_of_duplicate_slices = pickle.load(fh_input)
                fh_input.close()

                first_time = False

            if debug_on:
                temp_string = 'item_identifier:  ' + item_identifier + '\n' + 'file_sequence_number:  ' + file_sequence_number + '\n' + 'output_file_name:  ' + output_file_name + '\n'
                temp_string += 'start_slice_number:  ' + str(start_slice_number) + '\n' + 'end_slice_number:  ' + str(end_slice_number) + '\n' + 'object_store:  ' + object_store + '\n'
                temp_string += 'location_pickle_file:  ' + location_pickle_file + '\n' + 'checksum_pickle_file:  ' + checksum_pickle_file + '\n' + 'duplicate_slices_pickle_file:  ' + duplicate_slices_pickle_file + '\n'
                debug_log_fh.write(temp_string)
                debug_log_fh.flush()


            for slice_number in range(start_slice_number, (end_slice_number + 1)):
                checksum_key = checksum_dict[slice_number]
                temp_list = checksum_key.split(' :: ')
                expected_sha1_value = temp_list[0]
                expected_md5_value = temp_list[1]

                if not dict_of_duplicate_slices.has_key(checksum_key):
                    if not location_dict.has_key(checksum_key):
                        msg = 'ERROR :: ' + item_identifier + ' :: unable to find the object file within the dictionary  location_dict   with this checksum key:  ' + checksum_key + '\n'

                        sys.stdout.write(msg)
                        sys.stdout.flush()

                        if debug_on:
                            debug_msg = 'Wrote to stdout:   ==>' + msg
                            debug_log_fh.write(debug_msg)
                            debug_log_fh.flush()

                        #  Sleep for 5 seconds then self terminate
                        debug_log_fh.close()
                        time.sleep(5.25)
                        sys.exit(1)

                    #
                    #   Build out the path to the object file
                    #
                    #           slice_tag:     khmW/user.thoyt__201505291040__0-0__521
                    #     
                    #        object_store:     /AFS_backups_in_Cloud/ObjectStore
                    #    object_store_key:     khmW
                    #     slice_file_name:     user.thoyt__201505291040__0-0__521
                    #    object_directory:     /AFS_backups_in_Cloud/ObjectStore/k/h/m
                    #    slice_file_path:      /AFS_backups_in_Cloud/ObjectStore/k/h/m/user.thoyt__201505291040__0-0__521

                    slice_tag = location_dict[checksum_key]
                    object_store_key = str(slice_tag.split('/')[0:1])[2:-2]
                    slice_file_name = str(slice_tag.split('/')[1:2])[2:-2]
                    object_directory = object_store + object_store_key[0] + '/' + object_store_key[1] + '/' + object_store_key[2]
                    slice_file_path = object_directory + '/' + slice_file_name

                    if debug_on:
                        temp_string = 'file name:  ' + slice_file_name + '\n' + 'object store key:  ' + object_store_key + '\n' + 'object directory:  ' + object_directory + '\n'
                        debug_log_fh.write(temp_string)
                        debug_log_fh.flush()

                    if not os.path.isfile(slice_file_path):
                        msg = 'ERROR :: ' + item_identifier + ' :: unable to find the object file:  ' + slice_file_path + '\n'

                        sys.stdout.write(msg)
                        sys.stdout.flush()

                        if debug_on:
                            debug_msg = 'Wrote to stdout:   ==>' + msg
                            debug_log_fh.write(debug_msg)
                            debug_log_fh.flush()

                        #  Sleep for 5 seconds then self terminate
                        debug_log_fh.close()
                        time.sleep(5.25)
                        sys.exit(1)

                    #
                    #  Derive the encryption key for this slice, then open the encrypted and compressed file and read it back into a buffer
                    #  Then uncompress the decrypted buffer and then concatenate it to the other slices that have been decrypted and uncompressed
                    #
                    hash_handle = hashlib.md5()
                    hash_handle.update(slice_file_name)
                    file_name_hash = base64.urlsafe_b64encode(hash_handle.hexdigest())

                    slice_short_name = object_store_key + '/' + slice_file_name

                    hash_handle = hashlib.md5()
                    hash_handle.update(file_name_hash + slice_short_name + sodium)
                    encryption_key = (base64.urlsafe_b64encode(hash_handle.hexdigest()))[9:41]

                    st = os.stat(slice_file_path)
                    slice_fh = open(slice_file_path, 'rb')
                    encrypted_buffer = slice_fh.read(st.st_size)
                    slice_fh.close()

                    decryptionAES256(encryption_key)
                    slice_buffer = zlib.decompress(decrypted_buffer)

                    #  Calculate 2 checksum values for the (decrypted and uncompressed) slice_buffer  md5, sha1
                    hash_handle = hashlib.md5()
                    hash_handle.update(slice_buffer)
                    value_md5 = hash_handle.hexdigest()

                    hash_handle = hashlib.sha1()
                    hash_handle.update(slice_buffer)
                    value_sha1 = hash_handle.hexdigest()

                    #  Compare the expected values with the calculated values
                    if expected_md5_value == value_md5  and  expected_sha1_value == value_sha1:
                        if slice_number  ==  start_slice_number:
                            buffer_for_all_slices = slice_buffer
                        else:
                            buffer_for_all_slices += slice_buffer
                    else:
                        # an error condition
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
                            debug_msg = 'Wrote to stdout:   ==>' + msg
                            debug_log_fh.write(debug_msg)
                            debug_log_fh.flush()

                        #  Sleep for 5 seconds then self terminate
                        debug_log_fh.close()
                        time.sleep(5.25)
                        sys.exit(1)

                else:
                    #  This slice has already be decrypted and uncompressed
                    if not dict_of_decrypted_slices.has_key(checksum_key):
                        decrypted_file_name = dict_of_duplicate_slices[checksum_key]
                        if not os.path.isfile(decrypted_file_name):
                            msg = 'ERROR :: ' + item_identifier + ' :: unable to find the decrypted file:  ' + decrypted_file_name + '\n'

                            sys.stdout.write(msg)
                            sys.stdout.flush()

                            if debug_on:
                                debug_msg = 'Wrote to stdout:   ==>' + msg
                                debug_log_fh.write(debug_msg)
                                debug_log_fh.flush()

                            #  Sleep for 5 seconds then self terminate
                            debug_log_fh.close()
                            time.sleep(5.25)
                            sys.exit(1)
                        
                        #  Open the decrypted file
                        st = os.stat(decrypted_file_name)
                        slice_fh = open(decrypted_file_name, 'rb')
                        dict_of_decrypted_slices[checksum_key] = slice_fh.read(st.st_size)
                        slice_fh.close()

                    if slice_number  ==  start_slice_number:
                        buffer_for_all_slices = dict_of_decrypted_slices[checksum_key]
                    else:
                        buffer_for_all_slices += dict_of_decrypted_slices[checksum_key]



            #
            #  All the slices have be processed now write out all the decrypted and uncompressed slices into one file
            #
            slice_fh = open(output_file_name, 'wb')
            slice_fh.write(buffer_for_all_slices)
            slice_fh.close()

            msg = 'SUCCESS :: ' + item_identifier + ' :: ' + file_sequence_number + ' :: ' + output_file_name + '\n'

            sys.stdout.write(msg)
            sys.stdout.flush()

            if debug_on:
                debug_msg = 'Wrote to stdout:   ==>' + msg
                debug_log_fh.write(debug_msg)
                debug_log_fh.flush()


