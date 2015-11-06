#!/usr/bin/env python
#
#    $Header: /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/RCS/Create_Slice.py,v 1.2 2015/07/20 21:31:45 root Exp $
#
#    $Revision: 1.2 $
#
#    $Date: 2015/07/20 21:31:45 $
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
# Source code location:     /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/Create_Slice.py
#
# Local location:           /AFS_backups_in_Cloud/bin/Create_Slice.py
#
#
# Propose:
#
#   This Python program will be directed by a calling program to slice the AFS "vos dump" image file.
#   These slices will be compressed and then encrypted.  The calling program has already calculated
#   the checksum value for the slice using 2 methods (SHA-1 and MD5), these checksum values will be
#   used for lookup within the backup database and the ObjectStore (AWS S3).
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
#   Version 0.x     TMM   02/16/2015   code development started
#
#   Version 1.1     TMM   02/26/2015
#
#        Completed
#
#
#   Version 1.2     TMM   07/20/2015
#
#        Bug fixes
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

from sys import argv
from Crypto.Cipher import AES




# ========
# ========
def  ProcessCommandLine(argv, program_name):

    input_file_path = ''
    object_store = ''
    salt = ''

    try:
        opts, args = getopt.getopt(argv,"hf:o:s:",["help","file=","obj=","salt="])
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
        elif opt in ("-o", "--obj"):
            object_store = arg
        elif opt in ("-s", "--salt"):
            salt = arg


    if not input_file_path:
        msg = 'ERROR   Must specify the name of the vos dump file'
        print msg + '\n ' + program_name + '  --help   --file <input file>    --obj <path to object store>    --salt <Do Not Forget this string>'
        sys.exit(1)

    if not object_store:
        msg = 'ERROR   Must specify the path to the start of the object store'
        print msg + '\n ' + program_name + '  --help   --file <input file>    --obj <path to object store>    --salt <Do Not Forget this string>'
        sys.exit(1)

    return(input_file_path, object_store, salt)




# ========
# ========
def  encryptionAES256(secret_key, input_string, debug_encryption):

    global debug_log_fh

    # 32 bytes = 256 bits     24 bytes = 192 bits     16 bytes = 128 bits

    encryption_block_size = 32

    number_of_blocks, remainder = divmod(len(input_string), encryption_block_size)

    aligned_buffer_size = (number_of_blocks * encryption_block_size)
    aligned_buffer = input_string[:aligned_buffer_size]
    extra_buffer = input_string[aligned_buffer_size:]

    if debug_encryption:
        debug_msg = 'DEBUG     ---  encryptionAES256  ---\n'
        debug_msg = debug_msg + 'DEBUG   Input buffer size: ' + str(len(input_string)) + '\n'
        debug_msg = debug_msg + 'DEBUG   encryption block size: ' + str(encryption_block_size) + '    number of blocks: ' + str(number_of_blocks) + '    buffer size: ' + str(aligned_buffer_size) + '\n'
        debug_msg = debug_msg + 'DEBUG   Aligned buffer size: ' + str(len(aligned_buffer)) + '\n'
        printable_buffer = binascii.b2a_hex(extra_buffer)
        debug_msg = debug_msg + 'DEBUG   Extra buffer size: ' + str(len(extra_buffer)) + '    buffer contents: ' + printable_buffer + '\n'
        debug_msg = debug_msg + 'DEBUG   Secret Key: ' + secret_key + '\n'
        debug_log_fh.write(debug_msg)
        debug_log_fh.flush()


    # Since the input buffer is going to be 32 byte aligned just pad with null's
    padding_character = ''
    pad = lambda s: s + (encryption_block_size - len(s) % encryption_block_size) * padding_character

    # encrypt with AES
    EncodeAES = lambda c, s: str(c.encrypt(pad(s)))

    # creates the cipher using the "secret_key"
    cipher = AES.new(secret_key)

    # encodes you private info!
    encoded = EncodeAES(cipher, aligned_buffer)

    if debug_encryption:
        debug_msg = 'DEBUG   Encrypted buffer size: ' + str(len(encoded)) + '\n'
        debug_log_fh.write(debug_msg)
        debug_log_fh.flush()

    output_buffer = str(len(extra_buffer)).zfill(2) + extra_buffer + encoded

    if debug_encryption:
        debug_msg = 'DEBUG   Output buffer size: ' + str(len(output_buffer)) + '\n'
        debug_msg = debug_msg + 'DEBUG     - + - + - + - + - + - + - + - + - + - + - +\n\n'
        debug_log_fh.write(debug_msg)
        debug_log_fh.flush()

    return (output_buffer)




# ========
# ========
def  decryptionAES256(secret_key, input_string, debug_encryption):

    global debug_log_fh

    size_of_extra = int(input_string[0:2])
    offset = size_of_extra + 2
    extra_buffer = input_string[2:offset]
    encrypted_string = input_string[offset:]

    if debug_encryption:
        debug_msg = 'DEBUG     ---  DecryptionAES256  ---\n'
        debug_msg = debug_msg + 'DEBUG   Input buffer size: ' + str(len(input_string)) + '\n'
        debug_msg = debug_msg + 'DEBUG   Size of extra: ' + str(size_of_extra) + '\n'
        debug_msg = debug_msg + 'DEBUG   Offset: ' + str(offset) + '\n'
        printable_buffer = binascii.b2a_hex(extra_buffer)
        debug_msg = debug_msg + 'DEBUG   Extra buffer size: ' + str(len(extra_buffer)) + '    buffer contents: ' + printable_buffer + '\n'
        debug_msg = debug_msg + 'DEBUG   Encrypted string size: ' + str(len(encrypted_string)) + '\n'
        debug_msg = debug_msg + 'DEBUG   Secret Key: ' + secret_key + '\n'

    padding_character = ''
    DecodeAES = lambda c, e: c.decrypt(e)
    cipher = AES.new(secret_key)
    decoded = DecodeAES(cipher, encrypted_string)

    if debug_encryption:
        debug_msg = 'DEBUG   decrypted buffer size: ' + str(len(decoded)) + '\n'
        debug_log_fh.write(debug_msg)
        debug_log_fh.flush()


    output_buffer = decoded + extra_buffer

    if debug_encryption:
        debug_msg = 'DEBUG   Output buffer size: ' + str(len(output_buffer)) + '\n'
        debug_msg = debug_msg + 'DEBUG     - + - + - + - + - + - + - + - + - + - + - +\n\n'
        debug_log_fh.write(debug_msg)
        debug_log_fh.flush()

    return (output_buffer)




# ========
# ========
def  CreateOneSlice(slice_offset, slice_number, slice_size, stub_name, salt, flag_validate_encryption, debug_on):
    global fh_vos_dump
    global object_store
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


        slice_file_name = stub_name + str(slice_number)
        hash_handle = hashlib.md5()
        hash_handle.update(slice_file_name)
        file_name_hash = base64.urlsafe_b64encode(hash_handle.hexdigest())

        # Use the random number generator to come up with the 4 character random directory name (object_store_key) within the object store
        index1 = random.randrange(1, 53, 1)
        if index1  <  27:
            index1 += 64
        else:
            index1 += 70

        index2 = random.randrange(1, 63, 1)
        if index2  <  11:
            index2 += 47
        elif index2  <  37:
            index2 += 54
        else:
            index2 += 60

        index3 = random.randrange(1, 63, 1)
        if index3  <  27:
            index3 += 96
        elif index3  <  53:
            index3 += 38
        else:
            index3 -= 5

        index4 = random.randrange(1, 63, 1)
        if index4  <  27:
            index4 += 64
        elif index4  <  37:
            index4 += 21
        else:
            index4 += 60

        object_store_key = str(chr(index1) + chr(index2) + chr(index3) + chr(index4))
        slice_tag = object_store_key + '/' + slice_file_name

        object_directory = object_store  +  '/'  +  str(chr(index1))  +  '/'  +  str(chr(index2))  +  '/'  +  str(chr(index3))
        slice_file_path = object_directory + '/' + slice_file_name

        hash_handle = hashlib.md5()
        hash_handle.update(file_name_hash + slice_tag + sodium)
        encryption_key = (base64.urlsafe_b64encode(hash_handle.hexdigest()))[9:41]

        if not os.path.exists(object_directory):
            try:
                os.makedirs(object_directory)
            except OSError as exception:
                if exception.errno != errno.EEXIST:
                    raise

        if debug_on:
            debug_msg = 'encryption key:  ' + encryption_key + '\n'
            debug_msg = debug_msg + 'key length:      ' + str(len(encryption_key)) + '\n'
            debug_msg = debug_msg + 'slice file name: ' + slice_file_path + '\n'
            debug_log_fh.write(debug_msg)
            debug_log_fh.flush()

        compressed_slice_buffer = zlib.compress(slice_buffer)
        encrypted_slice_buffer = encryptionAES256(encryption_key, compressed_slice_buffer, flag_validate_encryption)

        if flag_validate_encryption:
            slice_file_path = object_directory + '/' + slice_file_name + '_encrypted_AES'

        slice_fh = open(slice_file_path, 'wb')
        slice_fh.write(encrypted_slice_buffer)
        slice_fh.close()


        if flag_validate_encryption:
            #  Write the compressed buffer (it's not encrypted) out to a file
            slice_file_path = object_directory + '/' + slice_file_name + '_compressed'
            slice_fh = open(slice_file_path, 'wb')
            slice_fh.write(compressed_slice_buffer)
            slice_fh.close()

            #  Write the uncompressed and un-encrypted buffer as file
            slice_file_path = object_directory + '/' + slice_file_name + '_orignal'
            slice_fh = open(slice_file_path, 'wb')
            slice_fh.write(slice_buffer)
            slice_fh.close()

            #  Reopen the encrypted file and read it back and decrypted it and write it out
            #  Then uncompress the decrypted buffer and write it out to a file
            slice_file_path = object_directory + '/' + slice_file_name + '_encrypted_AES'
            st = os.stat(slice_file_path)
            slice_fh = open(slice_file_path, 'rb')
            encrypted_buffer = slice_fh.read(st.st_size)
            slice_fh.close()

            decrypted_slice_buffer = decryptionAES256(encryption_key, encrypted_buffer, flag_validate_encryption)

            slice_file_path = object_directory + '/' + slice_file_name + '_decrypted_AES'
            slice_fh = open(slice_file_path, 'wb')
            slice_fh.write(decrypted_slice_buffer)
            slice_fh.close()

            uncompressed_slice_buffer = zlib.decompress(decrypted_slice_buffer)

            slice_file_path = object_directory + '/' + slice_file_name + '_uncompressed_AES'
            slice_fh = open(slice_file_path, 'wb')
            slice_fh.write(uncompressed_slice_buffer)
            slice_fh.close()


        #  Get the number of disk blocks the compress and encrypted file is using (output_block_cnt)
        #
        disk_block_size = 1024
        if flag_validate_encryption:
            slice_file_path = object_directory + '/' + slice_file_name + '_encrypted_AES'

        st = os.stat(slice_file_path)
        encrypted_file_size_in_bytes = st.st_size
        quotient, remainder = divmod(encrypted_file_size_in_bytes, disk_block_size)
        if int(remainder)  ==  0:
            output_block_cnt = int(quotient)
        else:
            output_block_cnt = int(quotient) + 1

        #  Get the number of disk blocks the input slice file was consuming (input_block_cnt)
        unencrypted_file_size_in_bytes = len(slice_buffer)
        quotient, remainder = divmod(unencrypted_file_size_in_bytes, disk_block_size)
        if int(remainder)  ==  0:
            input_block_cnt = int(quotient)
        else:
            input_block_cnt = int(quotient) + 1

        record = str(encrypted_file_size_in_bytes) + ' :: ' + str(input_block_cnt) + ' :: ' + str(output_block_cnt) + ' :: ' + str(slice_offset) + ' :: ' + str(slice_number) + ' :: unique :: ' + slice_tag

    else:
        return_msg_buffer = 'ERROR   Ending offset: ' + str(ending_offset) + '     is past the end of the file: ' + str(file_size_in_bytes)
        if debug_on:
            debug_log_fh.write(return_msg_buffer + '\n')
            debug_log_fh.flush()

        return (return_msg_buffer)

    return (record)




if __name__ == "__main__":


    # TMM  to enable debugging  (set to True)

    flag_validate_encryption = False
    debug_encryption = False
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

    input_file_path, object_store, sodium = ProcessCommandLine(sys.argv[1:], program_name)

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
 
    if not os.path.isdir(object_store):
        error_msg = 'ERROR   Unable to find the object store directory:  ' + object_store + '\n'
        if debug_on:
            debug_log_fh.write(error_msg)
            debug_log_fh.flush()

        sys.stdout.write(error_msg)
        sys.stdout.flush()
        sys.exit(1)

    if debug_on:
        debug_log_fh.write('Found the object store: ' + str(object_store) + '\n')
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

    afs_volume_name = str(vosdump_file_name.split('__')[0:1])[2:-2]
    time_stamp = str(vosdump_file_name.split('__')[1:2])[2:-2]
    dump_level = str(vosdump_file_name.split('__')[2:3])[2:-2]
    # concationate the timpe string into a 12 character string (yyyymmddHHMM)
    year = str(time_stamp.split('_')[0:1])[2:-2]
    month = str(time_stamp.split('_')[1:2])[2:-2]
    date = str(time_stamp.split('_')[2:3])[2:-2]
    hh_mm = str(time_stamp.split('_')[3:4])[2:-2]
    HH = str(hh_mm.split(':')[0:1])[2:-2]
    MM = str(hh_mm.split(':')[1:2])[2:-2]

    slice_file_stub_name = afs_volume_name + '__' + year + month + date + HH + MM + '__' + dump_level + '__'

    if debug_on:
        debug_msg = 'AFS volume name:  ' + afs_volume_name + '\n'
        debug_log_fh.write(debug_msg)
        debug_msg = 'Year: ' + year + '  Month: ' + month + '  Date: ' + date + '\n'
        debug_log_fh.write(debug_msg)
        debug_msg = 'Stub file name:  ' + slice_file_stub_name + '\n'
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
            debug_msg = 'stdin contents:   ---->>' + line + '<<----\n'
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

            checksum_key = input_record[0] + ' :: ' + input_record[1] + ' :: ' + input_record[2]

            slice_length = int(input_record[2])
            slice_offset = int(input_record[3])
            slice_number = int(input_record[4])

            if debug_on:
                debug_msg = 'Call  CreateOneSlice    slice number: ' + str(slice_number) + '\n'
                debug_log_fh.write(debug_msg)
                debug_log_fh.flush()

            partial_db_record = CreateOneSlice(slice_offset, slice_number, slice_length, slice_file_stub_name, sodium, flag_validate_encryption, debug_on)

            #  The format of the record used within database for this vos dump image
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
            #     Offset of this slice within the vos dump file
            #     Slice number
            #     Status  Duplicate or Unique  slice file within the context of this vos dump file
            #
            #     The name of the slice file within the object store
            #
            db_record = str(checksum_key) + ' :: ' + str(partial_db_record) + '\n'

            if debug_on:
                debug_msg = 'Write to stdout: ' + db_record
                debug_log_fh.write(debug_msg)
                debug_log_fh.flush()

            sys.stdout.write(db_record)
            sys.stdout.flush()

    if debug_on:
        debug_log_fh.close()

    fh_vos_dump.close()
    sys.exit(0)
