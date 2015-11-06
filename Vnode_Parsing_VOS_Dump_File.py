#!/usr/bin/env python
#
#
#    $Header: /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/RCS/Vnode_Parsing_VOS_Dump_File.py,v 1.3 2015/07/20 21:14:43 root Exp $
#
#    $Revision: 1.3 $
#
#    $Date: 2015/07/20 21:14:43 $
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
# Source code location:     /afs/nd.edu/user7/sysadmin/Private/NetApp/Scripts/Vnode_Parsing_VOS_Dump_File.py
#
# Local location:           /usr/local/bin/Vnode_Parsing_VOS_Dump_File.py
#
#
# Propose:
#
#   This Python program will take a file, that is the output from the AFS "vos dump" comamnd and unroll it for
#   the purposes of locating where the actual data for the individual Vnodes is contained within the AFS volume.
#   It will then attempt to calculate where the boundaries (offsets into the dump file) are for slicing up the
#   vos dump file along boundaries that encompass entire Vnodes.
#
#   Hence each Vnode will be represented by one or more slices.  Note the target optimal slice size is an input
#   value that is supplied when this program is envoked.  I have experiemented with various optimal slice sizes
#   ranging from 256KB to 8KB and have determined that using 32KB as the optimal slice size works will for smaller
#   size vnodes.
#
#   An improvment that was designed into this program is to increase the "optimal" slice size when the size of the
#   vnode exceeds a certain threshold as defined by the variable SMALL_FILE_SIZE.  When the vnode is larger than
#   this threshold the slice size will be increased by 4 times the optimal slice size that was provided as input
#   to this program.
#
#   Also if the size of a vnode is smaller than a minimal size, defined by the variable DEFAULT_SMALLEST_SLICE,
#   then it will be combined with the next vnode(s); until the combined size of the vnodes is larger than the
#   size as defined by DEFAULT_SMALLEST_SLICE
#
 
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
#     1)  The name of the name of the AFS volume dump file
#     2)  The output file (contains information about each slice)
#     3)  The size of the optimal slice
#
#
#
# History:
#
#   Version 0.x     TMM   01/08/2015   code development started
#
#   Version 1.1     TMM   01/27/2015
#
#        Working code.
#
#        The expected output from this program is a data file.  Under normal circumstances the format will be one
#        line per slice.  Each line will have these 3 fields delimited by a colon ":"
#
#              Slice number
#              Offset into vos dump file where the slice will start
#              Size of the slice (length of the slice file that will be created before data compression)
#
#
#        Under certain conditions (the file is not a vos dump file or its a very small vos dump file) the output file
#        will contain just one line.  That line will be composed of these 5 fields that are delimited by a colon ":"
#
#              Starting offset into the file, this should always be zero
#              Starting slice number, this should always be one
#              Number of slices that will be created
#              The size of the file that will be sliced up.
#              Size of the slice (length of the resulting slice file that will be created before data compression)
#
#
#        Alternatively under certain conditions the result in an error the line will have the a string that starts
#        with  "ERROR:"  Also this program will return an status code of 1 to indicate an error condition
#
#
#
#   Version 1.2     TMM   02/16/2015
#
#        Reworked the subroutine  ProcessTheVnodeOffsetInfo  for vnodes that are files larger than the "opitmal slice"
#        we will create a small slice as the first slice with a fixed size of 4KB.
#
#        Also reworked the subroutine  ProcessTheVnodeOffsetInfo  to support the annotation of the start of a file from
#        within the vos dump file.  We will provide the full path to the identified file (with respect from within the
#        AFS volume) along with the file's size in bytes.
#
#
#
#   Version 1.3     TMM   02/16/2015
#
#        Added some bug fixes 
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
import mmap
import struct

from sys import argv





# ========
# ========
def  ProcessCommandLine(argv, program_name):

    input_file_path = ''
    output_file_path = ''
    optimal_slice_size = ''

    try:
        opts, args = getopt.getopt(argv,"hi:o:s:",["help","input=","output=","size="])
    except getopt.GetoptError:
        print ' ' + program_name + '  --help   --input <input file>   --output <output file>  --size <size of optimal slice file>'
        sys.exit(1)

    dryrun = False
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print ' ' + program_name + '  --help   --input <input file>   --output <output file>  --size <size of optimal slice file>'
            sys.exit(0)
        elif opt in ("-i", "--input"):
            input_file_path = arg
        elif opt in ("-o", "--output"):
            output_file_path = arg
        elif opt in ("-s", "--size"):
            optimal_slice_size = arg


    if not input_file_path:
        msg = 'Must specify the name of the input file\n\n'
        print msg + program_name + '  --help   --input <input file>   --output <output file>  --size <size of optimal slice file>'
        sys.exit(1)

    if not output_file_path:
        msg = 'Must specify the name of the output file\n\n'
        print msg + program_name + '  --help   --input <input file>   --output <output file>  --size <size of optimal slice file>'
        sys.exit(1)

    if not optimal_slice_size:
        msg = 'Must specify the optimal slice file size\n\n'
        print msg + program_name + '  --help   --input <input file>   --output <output file>  --size <size of optimal slice file>'
        sys.exit(1)

    return(input_file_path, output_file_path, int(optimal_slice_size))




# ========
# ========
def  ProcessTheVnodeOffsetInfo(scratch_file, output_file_path, size):

    #  Open up the output file for writing
    output_file_fh = open(output_file_path, "w")

    #  Open the scratch file for reading
    temp_file_fh = open(scratch_file, "r")

    optimal_slice_size = int(size)

    # If a vnode is smaller than this minimal size, then we will need to combine it with the next vnode(s)
    # until they are at least this size 
    DEFAULT_SMALLEST_SLICE = 1024

    # If the vnode is larger than this, increase the slice size so that its 4 times the optimal slice size
    # For larger vnodes (files) we will use larger slices
    SMALL_FILE_SIZE = 1048576

    # If the vnode is a file and its larger than the optimal slice then the file's first slice will be a very small
    # slice that will encapsulate the file's meta data (file ownership, permissions, modification time, size)
    FILE_META_DATA_SLICE_SIZE = 4096

    slice_number = 1
    flag_first_vnode = True

    multi_file_counter = 0
    error_msg = ''

    for line in temp_file_fh:
        line = line.rstrip('\r|\n')
        record = line.split(":")
        vnode_offset = int(record[0])
        vnode_length = int(record[1])
        vnode_type = record[2]

        if 'File' in vnode_type  or  'Directory' in vnode_type:
            if flag_first_vnode:
                start_of_slice = vnode_offset
                if vnode_length  >=  optimal_slice_size:
                    # Okay the vnode (file) is larger than the optimal slice, so lets slice it up
                    if vnode_length  >  SMALL_FILE_SIZE:
                        # Increasing the size of the slice because this is a large file
                        variable_slice_size = optimal_slice_size * 4
                    else:
                        variable_slice_size = optimal_slice_size

                    if 'File' in vnode_type:
                        # Now the file's first slice will be 4KB.
                        #
                        # We are doing this because the meta data (file ownership, permissions, modification time, size)
                        # is contained within the first bit of the file's first slice.  So it could be possible that the
                        # only "change" that is made to this file will be the meta data and not the actual data.  In that
                        # case then only this slice would need to be updated
                        
                        size_of_first_slice = FILE_META_DATA_SLICE_SIZE
                        quotient, remainder = divmod((vnode_length - size_of_first_slice), variable_slice_size)
                        if int(remainder)  ==  0:
                            size_of_last_slice = variable_slice_size
                            number_of_slices = int(quotient) + 1
                        else:
                            size_of_last_slice = remainder
                            number_of_slices = int(quotient) + 2
                    else:
                        size_of_first_slice = variable_slice_size
                        quotient, remainder = divmod(vnode_length, variable_slice_size)
                        if int(remainder)  ==  0:
                            size_of_last_slice = variable_slice_size
                            number_of_slices = int(quotient)
                        else:
                            size_of_last_slice = remainder
                            number_of_slices = int(quotient) + 1

                    index_to_last_slice = number_of_slices - 1

                    for index in range(number_of_slices):
                        if index  ==  0:
                            if 'File' in vnode_type:
                                output = str(slice_number) + ':' + str(start_of_slice) + ':' + str(size_of_first_slice) + ':SOF\n'
                            else:
                                output = str(slice_number) + ':' + str(start_of_slice) + ':' + str(size_of_first_slice) + '\n'
                            next_slice_offset = start_of_slice + size_of_first_slice
                        elif index  ==  index_to_last_slice:
                            if 'File' in vnode_type:
                                output = str(slice_number) + ':' + str(next_slice_offset) + ':' + str(size_of_last_slice) + ':EOF\n'
                            else:
                                output = str(slice_number) + ':' + str(next_slice_offset) + ':' + str(size_of_last_slice) + '\n'
                            next_slice_offset += size_of_last_slice
                        else:
                            output = str(slice_number) + ':' + str(next_slice_offset) + ':' + str(variable_slice_size) + '\n'
                            next_slice_offset += variable_slice_size

                        output_file_fh.write(output)
                        slice_number += 1

                    # Now that we have written out that vnode (file).
                    # Reset to get ready to process the next vnode and the corresponding start of the next slice
                    flag_first_vnode = True
                else:
                    # The vnode is smaller than the optimal slice
                    if vnode_length  >=  DEFAULT_SMALLEST_SLICE:
                        # But the vnode is larger than the default smallest size, so lets write it out
                        if 'File' in vnode_type:
                            output = str(slice_number) + ':' + str(start_of_slice) + ':' + str(vnode_length) + ':SingleFile\n'
                        else:
                            output = str(slice_number) + ':' + str(start_of_slice) + ':' + str(vnode_length) + '\n'

                        output_file_fh.write(output)
                        slice_number += 1                        

                        # Now that we have written out that vnode (file).
                        # Reset to get ready to process the next vnode and the corresponding start of the next slice
                        flag_first_vnode = True                        
                    else:
                        # The vnode is to small so we will need to combine it with the next vnode(s)
                        flag_first_vnode = False
                        size_of_slice = vnode_length
                        multi_file_counter = 1
            else:
                if 'File' in vnode_type:
                    multi_file_counter += 1

                # This is not the first vnode, so this slice will be comprised of more than one vnode
                size_of_slice = size_of_slice + vnode_length
                if size_of_slice  >=  DEFAULT_SMALLEST_SLICE:
                    # We have combined two or vnodes and achieved a slice larger than the default smallest slice
                    if size_of_slice  >=  optimal_slice_size:
                        # Okay the combined vnodes are larger than the optimal slice, so lets slice it up
                        # If the last vnode was a large vnode, then we are increasing the slice size
                        if vnode_length  >  SMALL_FILE_SIZE:
                            variable_slice_size = optimal_slice_size * 4
                        else:
                            variable_slice_size = optimal_slice_size

                        size_of_first_slice = variable_slice_size
                        quotient, remainder = divmod(size_of_slice, variable_slice_size)
                        if int(remainder)  ==  0:
                            size_of_last_slice = variable_slice_size
                            number_of_slices = int(quotient)
                        else:
                            size_of_last_slice = remainder
                            number_of_slices = int(quotient) + 1

                        index_to_last_slice = number_of_slices - 1

                        for index in range(number_of_slices):
                            if index  ==  0:
                                if 'File' in vnode_type:
                                    output = str(slice_number) + ':' + str(start_of_slice) + ':' + str(size_of_first_slice) + ':StartMultipleFiles ' + str(multi_file_counter) + '\n'
                                else:
                                     output = str(slice_number) + ':' + str(start_of_slice) + ':' + str(size_of_first_slice) + '\n'

                                next_slice_offset = start_of_slice + size_of_first_slice
                            elif index  ==  index_to_last_slice:
                                if 'File' in vnode_type:
                                    output = str(slice_number) + ':' + str(next_slice_offset) + ':' + str(size_of_last_slice) + ':EOF\n'
                                else:
                                    output = str(slice_number) + ':' + str(next_slice_offset) + ':' + str(size_of_last_slice) + '\n'

                                next_slice_offset += size_of_last_slice
                            else:
                                output = str(slice_number) + ':' + str(next_slice_offset) + ':' + str(variable_slice_size) + '\n'
                                next_slice_offset += variable_slice_size

                            output_file_fh.write(output)
                            slice_number += 1
                            multi_file_counter = 0
                    else:
                        # But the combined vnodes are larger than the default smallest size, so lets write it out
                        if 'File' in vnode_type:
                            output = str(slice_number) + ':' + str(start_of_slice) + ':' + str(size_of_slice) + ':MultipleFiles ' + str(multi_file_counter) + '\n'
                        else:
                            output = str(slice_number) + ':' + str(start_of_slice) + ':' + str(size_of_slice) + '\n'
                        output_file_fh.write(output)
                        slice_number += 1 
                        multi_file_counter = 0                      

                    # Now that we have written out the slice that has the combined vnodes
                    # Reset to get ready to process the next vnode and the corresponding start of the next slice
                    flag_first_vnode = True                        
                else:
                    # The combined vnodes are still to small, keep gathering vnodes until we
                    # reach at least the default minimal slice size
                    flag_first_vnode = False
        elif 'Dump file header' in vnode_type:
            if slice_number  !=  1:
                error_msg = 'ERROR:  Something is wrong the \"Dump file header\" should only be the first slice\n'
                break

            output = str(slice_number) + ':' + str(vnode_offset) + ':' + str(vnode_length) + '\n'
            output_file_fh.write(output)
            slice_number += 1
        else:
            error_msg = 'ERROR:  Something is wrong unknown vnode type: ' + str(vnode_type) + '\n'
            break


        if error_msg:
            if debug_on:
                print error_msg

            output_file_fh.write(error_msg)
            output_file_fh.close()
            sys.exit(1)







if __name__ == "__main__":


    # MAGIC NUMBERS
    DUMPVERSION     = 1              # it is the 4 byte hex value of '\x00000001'
    DUMPBEGINMAGIC  = 3013677858     # it is the 4 byte hex value of '\xb3a11322'
    DUMPENDMAGIC    =  975260526     # it is the 4 byte hex value of '\x3a214b6e'


    # Top level tags
    TAG_DUMPHEADER = '\x01'
    TAG_VOLHEADER  = '\x02'
    TAG_VNODE      = '\x03'
    TAG_DUMPEND    = '\x04'


    # Dump Header Tags
    DHTAG_VOLNAME    = 'n'
    DHTAG_VOLID      = 'v'
    DHTAG_DUMPTIMES  = 't'


    # Volume Header Tags
    VHTAG_VOLID      = 'i'
    VHTAG_VERS       = 'v'
    VHTAG_VOLNAME    = 'n'
    VHTAG_INSERV     = 's'
    VHTAG_BLESSED    = 'b'
    VHTAG_VUNIQ      = 'u'
    VHTAG_TYPE       = 't'
    VHTAG_PARENT     = 'p'
    VHTAG_CLONE      = 'c'
    VHTAG_MAXQUOTA   = 'q'
    VHTAG_MINQUOTA   = 'm'
    VHTAG_DISKUSED   = 'd'
    VHTAG_FILECNT    = 'f'
    VHTAG_ACCOUNT    = 'a'
    VHTAG_OWNER      = 'o'
    VHTAG_CREAT      = 'C'
    VHTAG_ACCESS     = 'A'
    VHTAG_UPDATE     = 'U'
    VHTAG_EXPIRE     = 'E'
    VHTAG_BACKUP     = 'B'
    VHTAG_OFFLINE    = 'O'
    VHTAG_MOTD       = 'M'
    VHTAG_WEEKUSE    = 'W'
    VHTAG_DUDATE     = 'D'
    VHTAG_DAYUSE     = 'Z'



    #  VNODE tags
    VTAG_TYPE          = 't'
    VTAG_NLINKS        = 'l'
    VTAG_DVERS         = 'v'
    VTAG_CLIENT_DATE   = 'm'
    VTAG_AUTHOR        = 'a'
    VTAG_OWNER         = 'o'
    VTAG_GROUP         = 'g'
    VTAG_MODE          = 'b'
    VTAG_PARENT        = 'p'
    VTAG_SERVER_DATE   = 's'
    VTAG_ACL           = 'A'
    VTAG_DATA          = 'f'
    VTAG_LARGE_DATA    = 'h'



    debug_on = False
    debug_on_level_10 = False


    path_list = os.path.split(str(sys.argv[0:1])[2:-2])
    program_name = str(path_list[1:2])[2:-3]
    short_program_name = program_name.replace('.py', '')

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

    my_pid = os.getpid()
    log_file_name = temp_directory + '/' + short_program_name + '__' + str(my_pid) + '.log'
    scratch_file = temp_directory + '/' + short_program_name + '__' + str(my_pid) + '.temp'

    input_file_path, output_file_path, optimal_slice_size = ProcessCommandLine(sys.argv[1:], program_name)

 
    if not os.path.isfile(input_file_path):
        error_msg = 'Unable to open the input file:  ' + input_file_path
        print error_msg
        sys.exit(1)

    # Possible expected input file formats:
    #
    #      /dumpinfo/AFS_backups_to_AWS/user.pharvey__2014_08_21_06:27__1-5
    #
    #      /dumpinfo/AFS_backups_to_AWS/user.pharvey__2014_08_21_06:27__1-5.meta
    #
    path_list = os.path.split(input_file_path)
    vosdump_file_name = str(path_list[1:2])[2:-3]

    if debug_on:
        print 'file name:  ' + vosdump_file_name + '\n'

    #  Get the size if the vos dump file in bytes
    st = os.stat(input_file_path)
    file_size_in_bytes = st.st_size

    #  Open up the vos dump file for reading
    input_file_fh = open(input_file_path, "rb")
    input_file_offset = input_file_fh.tell()

    #  Open up the output file for writing
    output_file_fh = open(output_file_path, "w")

    #  Open up a scratch file for writing
    temp_file_fh = open(scratch_file, "w")

    # define the optimal size of the slice files to create (this is supplied as an input parameter to this program)
    slice_buffer_size = optimal_slice_size


    if debug_on:
        print 'slice buffer size: ' + str(slice_buffer_size) + '    File size: ' + str(file_size_in_bytes) + '\n'

    # TMM    I am sure that we can remove the check of the size of the VOS dump file
    #
    #         if file_size_in_bytes > slice_buffer_size:
    #   For now check that the file size is greater than zero
    
    if file_size_in_bytes > 0:
        # Going to walk the data structures within the vos dump file, get the file descriptor and setup memory mapped access
        input_fd = input_file_fh.fileno()

        # Create memory mapped file
        input_md = mmap.mmap(input_fd, 0, access=mmap.ACCESS_READ)

        input_md.seek(0,0)

        tag = input_md.read_byte()         # read the DUMP HEADER tag it should be single byte with a hex value of 0x01

        tuple_32bit_int = struct.unpack('>I', input_md.read(4))     # read the MAGIC number if should be 4 bytes with a hex value of 0xb3a11322
        magic_number = tuple_32bit_int[0]

        tuple_32bit_int = struct.unpack('>I', input_md.read(4))     # read the DUMP VERSION number it should be 4 bytes with a hex value of 0x00000001
        dump_version = tuple_32bit_int[0]

        if debug_on:
            print 'tag byte: ' + binascii.hexlify(tag) + '    Required tag: ' + binascii.hexlify(TAG_DUMPHEADER) + '\n'
            print 'magic number: ' + str(magic_number) + '    Required number: ' + str(DUMPBEGINMAGIC) + '\n'
            print 'dump version: ' + str(dump_version) + '    Required number: ' + str(DUMPVERSION) + '\n'


        if tag != TAG_DUMPHEADER  or  magic_number != DUMPBEGINMAGIC  or  dump_version != DUMPVERSION:
            if debug_on:
                print 'The input file is NOT a vos dump file\n'

            quotient, remainder = divmod(file_size_in_bytes, optimal_slice_size)
            if int(remainder)  ==  0:
                number_of_slices = int(quotient)
            else:
                number_of_slices = int(quotient) + 1

            #      file offset   :   starting slice   :   number of slices   :   size of the file(chunk size)   :   slice size
            record = '0:1:' + str(number_of_slices) + ':' + str(file_size_in_bytes) + ':' + str(optimal_slice_size) + '\n'
            output_file_fh.write(record)
            output_file_fh.close()
            sys.exit(0)
    else:
        if debug_on:
            print 'The input file is to small'

        #      file offset   :   starting slice   :   number of slices   :   size of the file(chunk size)   :   slice size
        record = '0:1:1:' + str(file_size_in_bytes) + ':' + str(file_size_in_bytes) + '\n'
        output_file_fh.write(record)
        output_file_fh.close()
        sys.exit(0)


    #  Process the rest of the Dump Header data structure
    error_msg = ''
    flag_dumpheader_VOLNAME = False
    flag_dumpheader_VOLID = False
    flag_dumpheader_DUMPTIMES = False

    loop_flag = True
    while loop_flag:
        tag = input_md.read_byte()

        if tag == DHTAG_VOLNAME:
            if flag_dumpheader_VOLNAME:
                error_msg = 'ERROR:  Second occurrence of DHTAG_VOLNAME   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break


            # read until null character is found to terminate the string that has the volume name
            flag_dumpheader_VOLNAME = True
            read_flag = True
            volume_name = ''

            while read_flag:
                tag = input_md.read_byte()
                if tag == '\x00':
                    read_flag = False
                else:
                    volume_name = volume_name + str(tag)

            if debug_on:
                print 'Volume name: ' + str(volume_name) + '\n'

        elif tag == DHTAG_VOLID:
            if flag_dumpheader_VOLID:
                error_msg = 'ERROR:  Second occurrence of DHTAG_VOLID   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            junk = input_md.read(4)   #  skip over the volume id 
        elif tag == DHTAG_DUMPTIMES:
            if flag_dumpheader_DUMPTIMES:
                error_msg = 'ERROR:  Second occurrence of DHTAG_DUMPTIMES   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            tuple_16bit_int = struct.unpack('>H', input_md.read(2))
            how_many_32bit_intergers = tuple_16bit_int[0]
            bytes_to_skip = how_many_32bit_intergers * 4
            junk = input_md.read(bytes_to_skip)             #  skip over all of the time stamps
        else:
            loop_flag = False
            if tag != TAG_VOLHEADER:
                error_msg = 'ERROR:  Did not find the start of the Volume header section in the VOS dump file\n'


    if error_msg: 
        if debug_on:
            print error_msg

        output_file_fh.write(error_msg)
        output_file_fh.close()
        sys.exit(1)




    #  Process the rest of the Volume Header data structure

    error_msg = ''
    flag_volumeheader_VOLID      = False
    flag_volumeheader_VERS       = False
    flag_volumeheader_VOLNAME    = False
    flag_volumeheader_INSERV     = False
    flag_volumeheader_BLESSED    = False
    flag_volumeheader_VUNIQ      = False
    flag_volumeheader_TYPE       = False
    flag_volumeheader_PARENT     = False
    flag_volumeheader_CLONE      = False
    flag_volumeheader_MAXQUOTA   = False
    flag_volumeheader_MINQUOTA   = False
    flag_volumeheader_DISKUSED   = False
    flag_volumeheader_FILECNT    = False
    flag_volumeheader_ACCOUNT    = False
    flag_volumeheader_OWNER      = False
    flag_volumeheader_CREAT      = False
    flag_volumeheader_ACCESS     = False
    flag_volumeheader_UPDATE     = False
    flag_volumeheader_EXPIRE     = False
    flag_volumeheader_BACKUP     = False
    flag_volumeheader_OFFLINE    = False
    flag_volumeheader_MOTD       = False
    flag_volumeheader_WEEKUSE    = False
    flag_volumeheader_DUDATE     = False
    flag_volumeheader_DAYUSE     = False

    loop_flag = True

    while loop_flag:
        tag = input_md.read_byte()

        if tag == VHTAG_VOLID:
            if flag_volumeheader_VOLID:
                error_msg = 'ERROR:  Second occurrence of VHTAG_VOLID   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_VOLID = True
            junk = input_md.read(4)   #  skip over the volume id 
        elif tag == VHTAG_VERS:
            if flag_volumeheader_VERS:
                error_msg = 'ERROR:  Second occurrence of VHTAG_VERS   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_VERS = True
            junk = input_md.read(4)   #  skip over the volume version
        elif tag == VHTAG_VOLNAME:
            if flag_volumeheader_VOLNAME:
                error_msg = 'ERROR:  Second occurrence of VHTAG_VOLNAME   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_VOLNAME = True
           # read until null character is found to terminate the string that has the volume name
            read_flag = True
            volume_name = ''

            while read_flag:
                tag = input_md.read_byte()
                if tag == '\x00':
                    read_flag = False
                else:
                    volume_name = volume_name + str(tag)

        elif tag == VHTAG_INSERV:
            if flag_volumeheader_INSERV:
                error_msg = 'ERROR:  Second occurrence of VHTAG_INSERV   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_INSERV = True
            junk = input_md.read_byte()
        elif tag == VHTAG_BLESSED:
            if flag_volumeheader_BLESSED:
                error_msg = 'ERROR:  Second occurrence of VHTAG_BLESSED   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_BLESSED = True
            junk = input_md.read_byte()
        elif tag == VHTAG_VUNIQ:
            if flag_volumeheader_VUNIQ:
                error_msg = 'ERROR:  Second occurrence of VHTAG_VUNIQ   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_VUNIQ = True
            junk = input_md.read(4)       # skip over the volume uniquifier
        elif tag == VHTAG_TYPE:
            if flag_volumeheader_TYPE:
                error_msg = 'ERROR:  Second occurrence of VHTAG_TYPE   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_TYPE = True
            junk = input_md.read_byte()
        elif tag == VHTAG_PARENT:
            if flag_volumeheader_PARENT:
                error_msg = 'ERROR:  Second occurrence of VHTAG_PARENT   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_PARENT = True
            junk = input_md.read(4)       # skip over the parent volume id
        elif tag == VHTAG_CLONE:
            if flag_volumeheader_CLONE:
                error_msg = 'ERROR:  Second occurrence of VHTAG_CLONE   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_CLONE = True
            junk = input_md.read(4)
        elif tag == VHTAG_MAXQUOTA:
            if flag_volumeheader_MAXQUOTA:
                error_msg = 'ERROR:  Second occurrence of VHTAG_MAXQUOTA   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_MAXQUOTA = True
            junk = input_md.read(4)       # skip over the volume's (max) quota
        elif tag == VHTAG_MINQUOTA:
            if flag_volumeheader_MINQUOTA:
                error_msg = 'ERROR:  Second occurrence of VHTAG_MINQUOTA   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_MINQUOTA = True
            junk = input_md.read(4) 
        elif tag == VHTAG_DISKUSED:
            if flag_volumeheader_DISKUSED:
                error_msg = 'ERROR:  Second occurrence of VHTAG_DISKUSED   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_DISKUSED = True
            junk = input_md.read(4)       # skip over the amount of space being used (in terms of 1KB blocks)
        elif tag == VHTAG_FILECNT:
            if flag_volumeheader_FILECNT:
                error_msg = 'ERROR:  Second occurrence of VHTAG_FILECNT   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_FILECNT = True
            junk = input_md.read(4)       # skip over the number of files and directories that are contained in this volume
        elif tag == VHTAG_ACCOUNT:
            if flag_volumeheader_ACCOUNT:
                error_msg = 'ERROR:  Second occurrence of VHTAG_ACCOUNT   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_ACCOUNT = True
            junk = input_md.read(4) 
        elif tag == VHTAG_OWNER:
            if flag_volumeheader_OWNER:
                error_msg = 'ERROR:  Second occurrence of VHTAG_OWNER   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_OWNER = True
            junk = input_md.read(4) 
        elif tag == VHTAG_CREAT:
            if flag_volumeheader_CREAT:
                error_msg = 'ERROR:  Second occurrence of VHTAG_CREAT   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_CREAT = True
            junk = input_md.read(4)       # skip over the creation date for the volume
        elif tag == VHTAG_ACCESS:
            if flag_volumeheader_ACCESS:
                error_msg = 'ERROR:  Second occurrence of VHTAG_ACCESS   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_ACCESS = True
            junk = input_md.read(4)       # skip over the date the volume was last accessed
        elif tag == VHTAG_UPDATE:
            if flag_volumeheader_UPDATE:
                error_msg = 'ERROR:  Second occurrence of VHTAG_UPDATE   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_UPDATE = True
            junk = input_md.read(4)       # skip over the date the volume was last updated
        elif tag == VHTAG_EXPIRE:
            if flag_volumeheader_EXPIRE:
                error_msg = 'ERROR:  Second occurrence of VHTAG_EXPIRE   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_EXPIRE = True
            junk = input_md.read(4)
        elif tag == VHTAG_BACKUP:
            if flag_volumeheader_BACKUP:
                error_msg = 'ERROR:  Second occurrence of VHTAG_BACKUP   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_BACKUP = True
            junk = input_md.read(4)       # skip over the date the volume was last backed up
        elif tag == VHTAG_OFFLINE:
            if flag_volumeheader_OFFLINE:
                error_msg = 'ERROR:  Second occurrence of VHTAG_OFFLINE   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_OFFLINE = True
           # read until null character is found to terminate the string that has the volume off line message
            read_flag = True
            volume_offline_msg = ''

            while read_flag:
                tag = input_md.read_byte()
                if tag == '\x00':
                    read_flag = False
                else:
                    volume_offline_msg = volume_offline_msg + str(tag)
        elif tag == VHTAG_MOTD:
            if flag_volumeheader_MOTD:
                error_msg = 'ERROR:  Second occurrence of VHTAG_MOTD   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_MOTD = True
           # read until null character is found to terminate the string that has the volume message of the day
            read_flag = True
            volume_motd = ''

            while read_flag:
                tag = input_md.read_byte()
                if tag == '\x00':
                    read_flag = False
                else:
                    volume_motd = volume_motd + str(tag)

        elif tag == VHTAG_WEEKUSE:
            if flag_volumeheader_WEEKUSE:
                error_msg = 'ERROR:  Second occurrence of VHTAG_WEEKUSE   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_WEEKUSE = True
            tuple_16bit_int = struct.unpack('>H', input_md.read(2))
            how_many_32bit_intergers = tuple_16bit_int[0]
            bytes_to_read = how_many_32bit_intergers * 4
            junk = input_md.read(bytes_to_read)             #  skip over all of the time stamps
        elif tag == VHTAG_DUDATE:
            if flag_volumeheader_DUDATE:
                error_msg = 'ERROR:  Second occurrence of VHTAG_DUDATE   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_DUDATE = True
            junk = input_md.read(4)
        elif tag == VHTAG_DAYUSE:
            if flag_volumeheader_DAYUSE:
                error_msg = 'ERROR:  Second occurrence of VHTAG_DAYUSE   prehaps this is not a VOS dump file\n'
                loop_flag = False
                break

            flag_volumeheader_DAYUSE = True
            junk = input_md.read(4)
        else:
            loop_flag = False
            if tag != TAG_VNODE:
                error_msg = 'ERROR:  Did not find the start of the VNODE header section in the VOS dump file\n'


    if error_msg: 
        if debug_on:
            print error_msg

        output_file_fh.write(error_msg)
        output_file_fh.close()
        sys.exit(1)


    input_md.seek(-1, os.SEEK_CUR)
    record = '0:' + str(input_md.tell()) + ':Dump file header\n'
    temp_file_fh.write(record)

    previous_vnode_offset = 0
    loop_flag = True


    #  Process the rest of the VNODE Header data structure
    while loop_flag:
        tag = input_md.read_byte()

        if tag == TAG_VNODE:
            vnode_offset_start = input_md.tell() - 1
            if debug_on:
                print 'Begin parsing the Vnode at offset: ' + hex(vnode_offset_start)

            tuple_32bit_int = struct.unpack('>I', input_md.read(4))
            vnode_number = tuple_32bit_int[0]

            tuple_32bit_int = struct.unpack('>I', input_md.read(4))
            vnode_uniquifier = tuple_32bit_int[0]

            error_msg = ''
            flag_vnode_TYPE          = False
            flag_vnode_NLINKS        = False
            flag_vnode_DVERS         = False
            flag_vnode_CLIENT_DATE   = False
            flag_vnode_AUTHOR        = False
            flag_vnode_OWNER         = False
            flag_vnode_GROUP         = False
            flag_vnode_MODE          = False
            flag_vnode_PARENT        = False
            flag_vnode_SERVER_DATE   = False
            flag_vnode_ACL           = False
            flag_vnode_DATA          = False
            flag_vnode_LARGE_DATA    = False
            

            while loop_flag:
                tag_offset = input_md.tell()
                tag = input_md.read_byte()

                if debug_on_level_10:
                    debug_msg = 'tag value: ' + binascii.hexlify(tag) + '   Vnode start: ' + hex(vnode_offset_start) + '   Current Offset: ' + hex(tag_offset) + '\n'
                    print debug_msg

                if tag == VTAG_TYPE: 
                    if flag_vnode_TYPE:
                        error_msg = 'ERROR:  Second occurrence of VTAG_TYPE   Vnode start: ' + hex(vnode_offset_start) + '   VTAG_TYPE offset: ' + hex(tag_offset) + '\n'
                        loop_flag = False
                        break

                    flag_vnode_TYPE = True
                    tuple_8bit_int = struct.unpack('>B', input_md.read_byte())     # read the MAGIC number if should be 4 bytes with a hex value of 0xb3a11322
                    vnode_type = tuple_8bit_int[0]

                elif tag == VTAG_NLINKS:
                    if flag_vnode_NLINKS:
                        error_msg = 'ERROR:  Second occurrence of VTAG_NLINKS   Vnode start: ' + hex(vnode_offset_start) + '   VTAG_NLINKS offset: ' + hex(tag_offset) + '\n'
                        loop_flag = False
                        break

                    flag_vnode_NLINKS = True
                    tuple_16bit_int = struct.unpack('>H', input_md.read(2))
                    number_of_links = tuple_32bit_int[0]

                elif tag == VTAG_DVERS:
                    if flag_vnode_DVERS:
                        error_msg = 'ERROR:  Second occurrence of VTAG_DVERS   Vnode start: ' + hex(vnode_offset_start) + '   VTAG_DVERS offset: ' + hex(tag_offset) + '\n'
                        loop_flag = False
                        break

                    flag_vnode_DVERS = True
                    junk = input_md.read(4)

                elif tag == VTAG_CLIENT_DATE:
                    if flag_vnode_CLIENT_DATE:
                        error_msg = 'ERROR:  Second occurrence of VTAG_CLIENT_DATE   Vnode start: ' + hex(vnode_offset_start) + '   VTAG_CLIENT_DATE offset: ' + hex(tag_offset) + '\n'
                        loop_flag = False
                        break

                    flag_vnode_CLIENT_DATE = True
                    junk = input_md.read(4)

                elif tag == VTAG_AUTHOR:
                    if flag_vnode_AUTHOR:
                        error_msg = 'ERROR:  Second occurrence of VTAG_AUTHOR   Vnode start: ' + hex(vnode_offset_start) + '   VTAG_AUTHOR offset: ' + hex(tag_offset) + '\n'
                        loop_flag = False
                        break

                    flag_vnode_AUTHOR = True
                    junk = input_md.read(4)

                elif tag == VTAG_OWNER:
                    if flag_vnode_OWNER:
                        error_msg = 'ERROR:  Second occurrence of VTAG_OWNER   Vnode start: ' + hex(vnode_offset_start) + '   VTAG_OWNER offset: ' + hex(tag_offset) + '\n'
                        loop_flag = False
                        break

                    flag_vnode_OWNER = True
                    junk = input_md.read(4)

                elif tag == VTAG_GROUP:
                    if flag_vnode_GROUP:
                        error_msg = 'ERROR:  Second occurrence of VTAG_GROUP   Vnode start: ' + hex(vnode_offset_start) + '   VTAG_GROUP offset: ' + hex(tag_offset) + '\n'
                        loop_flag = False
                        break

                    flag_vnode_GROUP = True
                    junk = input_md.read(4)

                elif tag == VTAG_MODE:
                    if flag_vnode_MODE:
                        error_msg = 'ERROR:  Second occurrence of VTAG_MODE   Vnode start: ' + hex(vnode_offset_start) + '   VTAG_MODE offset: ' + hex(tag_offset) + '\n'
                        loop_flag = False
                        break

                    flag_vnode_MODE = True
                    junk = input_md.read(2)

                elif tag == VTAG_PARENT:
                    if flag_vnode_PARENT:
                        error_msg = 'ERROR:  Second occurrence of VTAG_PARENT   Vnode start: ' + hex(vnode_offset_start) + '   VTAG_PARENT offset: ' + hex(tag_offset) + '\n'
                        loop_flag = False
                        break

                    flag_vnode_PARENT = True
                    junk = input_md.read(4)

                elif tag == VTAG_SERVER_DATE:
                    if flag_vnode_SERVER_DATE:
                        error_msg = 'ERROR:  Second occurrence of VTAG_SERVER_DATE   Vnode start: ' + hex(vnode_offset_start) + '   VTAG_SERVER_DATE offset: ' + hex(tag_offset) + '\n'
                        loop_flag = False
                        break

                    flag_vnode_SERVER_DATE = True
                    junk = input_md.read(4)

                elif tag == VTAG_ACL:
                    if flag_vnode_ACL:
                        error_msg = 'ERROR:  Second occurrence of VTAG_ACL   Vnode start: ' + hex(vnode_offset_start) + '   VTAG_ACL offset: ' + hex(tag_offset) + '\n'
                        loop_flag = False
                        break

                    flag_vnode_ACL = True
                    vnode_offset_acl_segment = input_md.tell() - 1
                    input_md.seek(192, os.SEEK_CUR)
                    tag = input_md.read_byte()
                    vnode_offset_file_segment = input_md.tell() - 1
                    if tag != VTAG_DATA:
                        error_msg = 'ERROR:  did NOT find the file segment   Vnode start: ' + hex(vnode_offset_start) + '   ACL offset: ' + hex(vnode_offset_acl_segment) + '    Segment file offset: ' + hex(vnode_offset_file_segment) + '\n'
                        loop_flag = False
                        break

                    input_md.seek(-1, os.SEEK_CUR)

                elif tag == VTAG_DATA:
                    if flag_vnode_DATA:
                        error_msg = 'ERROR:  Second occurrence of VTAG_DATA   Vnode start: ' + hex(vnode_offset_start) + '   VTAG_DATA offset: ' + hex(tag_offset) + '\n'
                        loop_flag = False
                        break

                    flag_vnode_DATA = True
                    tuple_32bit_int = struct.unpack('>I', input_md.read(4))
                    offset_to_next_vnode = tuple_32bit_int[0]
                    vnode_offset_file_data = input_md.tell()
                    input_md.seek(offset_to_next_vnode, os.SEEK_CUR)
                    tag = input_md.read_byte()

                    input_md.seek(-1, os.SEEK_CUR)
                    next_vnodes_offset = input_md.tell()
                    if tag != TAG_VNODE   and   tag != TAG_DUMPEND:
                        error_msg = 'ERROR:  Jumped over the file data   did NOT find the start of the next Vnode   Current Vnode: ' + hex(vnode_offset_start) + '   Next Vnode: ' + hex(next_vnodes_offset) + '   Segment file offset: ' + hex(vnode_offset_file_segment) + '   Offset: ' + hex(offset_to_next_vnode) + '\n'
                        loop_flag = False
                        break

                    if previous_vnode_offset != 0:
                        vnode_length = vnode_offset_start - previous_vnode_offset
                        if previous_vnode_type  ==  1:
                            record = str(previous_vnode_offset) + ':' + str(vnode_length) + ':File\n'
                        elif previous_vnode_type  ==  2:
                            record = str(previous_vnode_offset) + ':' + str(vnode_length) + ':Directory\n'
                        elif previous_vnode_type  ==  3:
                            record = str(previous_vnode_offset) + ':' + str(vnode_length) + ':File\n'
                        else:
                            record = str(previous_vnode_offset) + ':' + str(vnode_length) + ':ERROR  unknown vnode type: ' + hex(previous_vnode_type) + '\n'

                        temp_file_fh.write(record)
        
                    previous_vnode_offset = vnode_offset_start
                    previous_vnode_type = vnode_type

                elif tag == VTAG_LARGE_DATA:
                    if flag_vnode_LARGE_DATA:
                        error_msg = 'ERROR:  Second occurrence of VTAG_LARGE_DATA   Vnode start: ' + hex(vnode_offset_start) + '   VTAG_LARGE_DATA offset: ' + hex(tag_offset) + '\n'
                        loop_flag = False
                        break

                    flag_vnode_LARGE_DATA = True
                    tuple_64bit_int = struct.unpack('>Q', input_md.read(8))
                    offset_to_next_vnode = tuple_64bit_int[0]
                    vnode_offset_file_data = input_md.tell()
                    input_md.seek(offset_to_next_vnode, os.SEEK_CUR)
                    tag = input_md.read_byte()

                    input_md.seek(-1, os.SEEK_CUR)
                    next_vnodes_offset = input_md.tell()
                    if tag != TAG_VNODE   and   tag != TAG_DUMPEND:
                        error_msg = 'ERROR:  Jumped over the file data (VTAG_LARGE_DATA)   did NOT find the start of the next Vnode   Current Vnode: ' + hex(vnode_offset_start) + '   Next Vnode: ' + hex(next_vnodes_offset) + '   Segment file offset: ' + hex(vnode_offset_file_segment) + '   Offset: ' + hex(offset_to_next_vnode) + '\n'
                        loop_flag = False
                        break

                    if previous_vnode_offset != 0:
                        vnode_length = vnode_offset_start - previous_vnode_offset
                        if previous_vnode_type  ==  1:
                            record = str(previous_vnode_offset) + ':' + str(vnode_length) + ':File\n'
                        elif previous_vnode_type  ==  2:
                            record = str(previous_vnode_offset) + ':' + str(vnode_length) + ':Directory\n'
                        elif previous_vnode_type  ==  3:
                            record = str(previous_vnode_offset) + ':' + str(vnode_length) + ':File\n'
                        else:
                            record = str(previous_vnode_offset) + ':' + str(vnode_length) + ':ERROR  unknown vnode type: ' + hex(previous_vnode_type) + '\n'

                        temp_file_fh.write(record)
        
                    previous_vnode_offset = vnode_offset_start
                    previous_vnode_type = vnode_type

                else:
                    if tag == TAG_VNODE:
                        input_md.seek(-1, os.SEEK_CUR)
                        current_offset = input_md.tell()
                        if debug_on_level_10:
                            print 'Found the start of the next Vnode      Current Offset: ' + hex(current_offset) + '\n'
                    elif tag == TAG_DUMPEND:
                        input_md.seek(-1, os.SEEK_CUR)
                        current_offset = input_md.tell()
                        if debug_on:
                            print 'Found the tag for the end of the dump file\n'
                    else:
                        error_msg = 'ERROR:  Unknown vnode option: ' + binascii.hexlify(tag) + '   Vnode start: ' + hex(vnode_offset_start) + '   Current Offset: ' + hex(current_offset) + '\n'

                    loop_flag = False
                    


            if error_msg:
                if debug_on:
                    print error_msg

                output_file_fh.write(error_msg)
                output_file_fh.close()
                sys.exit(1)

            loop_flag = True
        elif tag == TAG_DUMPEND:
            tuple_32bit_int = struct.unpack('>I', input_md.read(4))     # read the MAGIC number if should be 4 bytes with a hex value of 0xb3a11322
            ending_magic_number = tuple_32bit_int[0]
            if ending_magic_number != DUMPENDMAGIC:
                current_offset = input_md.tell() - 4
                error_msg = 'ERROR:  Unknown dump ending magic number: ' + hex(ending_magic_number) + '   tag dump end: ' + hex(current_offset - 1) + '   Current Offset: ' + hex(current_offset) + '\n'
            else:
                current_offset = input_md.tell()
                if debug_on:
                    print 'Found the ending dump magic value   Current Offset: ' + hex(current_offset) + '\n'

                if previous_vnode_offset != 0:
                    vnode_length = current_offset - previous_vnode_offset
                    if previous_vnode_type  ==  1:
                        record = str(previous_vnode_offset) + ':' + str(vnode_length) + ':File\n'
                    elif previous_vnode_type  ==  2:
                        record = str(previous_vnode_offset) + ':' + str(vnode_length) + ':Directory\n'
                    elif previous_vnode_type  ==  3:
                        record = str(previous_vnode_offset) + ':' + str(vnode_length) + ':File\n'
                    else:
                        record = str(previous_vnode_offset) + ':' + str(vnode_length) + ':ERROR  unknown vnode type: ' + hex(previous_vnode_type) + '\n'

                    temp_file_fh.write(record)
                    previous_vnode_offset = 0

            loop_flag = False                       
        else:
            current_offset = input_md.tell()
            error_msg = 'ERROR:  Unknown dump header option: ' + binascii.hexlify(tag) + '   Current Offset: ' + hex(current_offset) + '\n'
            loop_flag = False


        if error_msg:
            if debug_on:
                print error_msg

            output_file_fh.write(error_msg)
            output_file_fh.close()
            sys.exit(1)



    input_file_fh.close()
    temp_file_fh.close()
    output_file_fh.close()
    
    ProcessTheVnodeOffsetInfo(scratch_file, output_file_path, optimal_slice_size)

    if not debug_on:
        if os.path.isfile(scratch_file):
            os.remove(scratch_file)


    sys.exit(0)


