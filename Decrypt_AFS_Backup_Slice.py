#!/usr/bin/env python
#
#    $Header: /afs/nd.edu/user7/sysadmin/Private/NetApp/Scripts/RCS/Decrypt_AFS_Backup_Slice.py,v 1.1 2014/12/22 21:06:10 root Exp root $
#
#  Copyright (C) 2014 Terry McCoy     (terry@nd.edu)
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
# Source code location:     /afs/nd.edu/user7/sysadmin/Private/NetApp/Scripts/Decrypt_AFS_Backup_Slice.py
#
# Local location:           /usr/local/bin/Decrypt_AFS_Backup_Slice.py
#
#
# Propose:
#
#   This Python program will decrypt and uncompress a file, that is a slice of a file that was the output
#    from the AFS "vos dump" process.
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
#     1)  The path to the file to be decrypted
#
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
def  ProcessCommandLine(argv, program_name):

    slice_tag = ''
    salt = ''
    value_md5 = ''
    value_sha1 = ''
    output_file_path = ''
    object_store = '/AFS_backups_in_Cloud/ObjectStore'

    try:
        opts, args = getopt.getopt(argv,"ht:x:y:s:o:O:",["help","tag=","md5=","sha1=","salt=","output=","ObjStore="])
    except getopt.GetoptError:
        print ' ' + program_name + '  --help   --tag <slice tag name>   --md5 <key>   --sha1 <key>   --output <output file>   --ObjStore <path to object store>   --salt <Do Not Forget this string>'
        sys.exit(1)

    dryrun = False
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print ' ' + program_name + '  --help   --tag <slice tag name>   --md5 <key>   --sha1 <key>   --output <output file>   --ObjStore <path to object store>   --salt <Do Not Forget this string>'
            sys.exit(0)
        elif opt in ("-t", "--tag"):
            slice_tag = arg
        elif opt in ("-x", "--md5"):
            value_md5 = arg
        elif opt in ("-y", "--sha1"):
            value_sha1 = arg
        elif opt in ("-s", "--salt"):
            salt = arg
        elif opt in ("-o", "--output"):
            output_file_path = arg
        elif opt in ("-O", "--ObjStore"):
            object_store = arg

    return(object_store, slice_tag, output_file_path, value_md5, value_sha1, salt)



# ========
# ========
def decryptionAES256(secret_key, input_string):

    global debug_on

    size_of_extra = int(input_string[0:2])
    offset = size_of_extra + 2
    extra_buffer = input_string[2:offset]
    encrypted_string = input_string[offset:]

    # TMM   debugging
    if debug_on:
        print '  ---  DecryptionAES256  ---\n'
        print 'Input buffer size: ' + str(len(input_string)) + '\n'
        print 'Size of extra: ' + str(size_of_extra) + '\n'
        print 'Offset: ' + str(offset) + '\n'
        printable_buffer = binascii.b2a_hex(extra_buffer)
        print 'Extra buffer size: ' + str(len(extra_buffer)) + '    buffer contents: ' + printable_buffer + '\n'
        print 'Encrypted string size: ' + str(len(encrypted_string)) + '\n'
        print 'Secret Key: ' + secret_key + '\n'

    padding_character = ''
    DecodeAES = lambda c, e: c.decrypt(e)
    cipher = AES.new(secret_key)
    decoded = DecodeAES(cipher, encrypted_string)

    # TMM   debugging
    if debug_on:
        print 'decrypted buffer size: ' + str(len(decoded)) + '\n'

    output_buffer = decoded + extra_buffer

    # TMM   debugging
    if debug_on:
        print 'Output buffer size: ' + str(len(output_buffer)) + '\n'
        print '  - + - + - + - + - + - + - + - + - + - + - +\n\n'

    return (output_buffer)




if __name__ == "__main__":

    debug_on = False


    path_list = os.path.split(str(sys.argv[0:1])[2:-2])
    program_name = str(path_list[1:2])[2:-3]

    # OLD
    #       slice_file_path, output_file, expected_md5_value, expected_sha1_value, sodium = ProcessCommandLine(sys.argv[1:], program_name)

    #    slice_file_path:     /AFS_backups_in_Cloud/ObjectStore/khmW/user.thoyt__201505291040__0-0__521
    #


    object_store, slice_tag, output_file, expected_md5_value, expected_sha1_value, sodium = ProcessCommandLine(sys.argv[1:], program_name)

    #        object_store:     AFS_backups_in_Cloud/ObjectStore
    #
    #           slice_tag:     khmW/user.thoyt__201505291040__0-0__521     

    
    #    object_store_key:     khmW
    #    object_directory:     AFS_backups_in_Cloud/ObjectStore/k/h/m
    #     slice_file_name:     user.thoyt__201505291040__0-0__521



    if not os.path.isfile(slice_file_path):
        msg = 'Unable to open the file:  ' + slice_file_path + '\n'
        print msg
        sys.exit(1)


    #   /AFS_backups_in_Cloud/ObjectStore/khmW/user.thoyt__201505291040__0-0__521

    path_list = os.path.split(slice_file_path)
    slice_file_name = str(path_list[1:2])[2:-3]
    directory_path = str(path_list[0:1])[2:-3]

    path_list = os.path.split(directory_path)
    object_store_key = str(path_list[1:2])[2:-3]

    # TMM  debug

    print 'file name:  ' + slice_file_name + '\n'
    print 'object store key:  ' + object_store_key + '\n'

            
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
    else:
         msg1 = ''
         msg2 = ''
         if expected_md5_value  !=  value_md5:
             msg1 = 'Mismatch with md5 checksum'
         if expected_sha1_value  !=  value_sha1:
             msg2 = 'Mismatch with SHA-1 checksum'

         msg = 'ERROR:  ' + msg1 + '       ' + msg2 + '\n'
         print msg
         sys.exit(1)


    print 'Success\n'
    sys.exit(0)
