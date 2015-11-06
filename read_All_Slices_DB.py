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
# Source code location:     /afs/nd.edu/user7/sysadmin/Private/AFS/AFS_tools/CloudBackups/read_All_Slices_DB.py
#
# Local location:           /AFS_backups_in_Cloud/bin/read_All_Slices_DB.py
#
#
# Propose:
#
#   This Python program will read the specified All Slices database file and validate it's contents with each
#   of the vos dump database files corresponding to that volume
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
#
#         --help
#
#         --file                    <full path to the All Slices Database file>
#
#
#
#
#
# History:
#
#   Version 0.x     TMM   08/19/2015   code development started
#
#   Version 1.1     TMM   08/25/2015
#
#        Initail code drop, 
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





ON_POSIX = 'posix' in sys.builtin_module_names






#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

def  ProcessCommandLine(argv, program_name):

    input_file_path = ''

    help01 = '  --help\n'
    help02 = '  --file                    <full path to the All Slices database filee>\n\n'


    help_msg = help01 + help02

    try:
        opts, args = getopt.getopt(argv,"hf:",["help","file="])
    except getopt.GetoptError:
        print ' ' + program_name + '\n\n' + help_msg
        sys.exit(1)



    dryrun = False
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print ' ' + program_name + '\n\n' + help_msg
            sys.exit(0)
        elif opt in ("-f", "--file"):
            input_file_path = arg        

    if not input_file_path:
        msg = 'Must specify the full path to the All Slices database file'
        print msg + '\n ' + program_name + '\n\n' + help_msg
        logger.critical(msg)
        sys.exit(1)


    return(input_file_path)







#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==
#
#
#
#  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==  ==

if __name__ == "__main__":



    debug_on = True
 

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



    #   This dictionary is populate from the contents of the All Slice database  (checksum_key is the combination of the SHA1, MD5 and file size)
    database_of_all_slices = {}

    #   This dictionary is populate with the link count for each record within the All Slice database  (checksum_key is the combination of the SHA1, MD5 and file size)
    link_count_all_slices = {}

    #   This dictionary is populate with the slice name for each record within the All Slice database  (checksum_key is the combination of the SHA1, MD5 and file size)
    slice_name_all_slices = {}

    #   This dictionary is populate with boolean   True = record exist within daily dump(s)   False = no matches within daily dumps   (checksum_key is the combination of the SHA1, MD5 and file size)
    needs_to_be_removed_all_slices = {}

    #   This dictionary uses the slice name as the key and stores the checksum_key for each record within the All Slice database
    keys_of_all_slices = {}

    #   This dictionary keeps track of the number of times a slice is used within all the daily dumps  (checksum_key is the combination of the SHA1, MD5 and file size)
    daily_dump_link_count = {}

    #   This dictionary will saves the record from the daily dump that does NOT have an entry within the All Slices database  (checksum_key is the combination of the SHA1, MD5 and file size)
    needs_to_be_added_record = {}

    #   This dictionary will saves the checksum_key of the record that needs to be added to the All Slices database   (slice_name is used as the hash key for the dictionary)
    needs_to_be_added_checksum_key = {}

    #   This dictionary will saves the link count (aka the number of times this record has been encountered within the the daily dump)
    needs_to_be_added_link_count = {}





    dbfile_all_slices = ProcessCommandLine(sys.argv[1:], program_name)

    dbfile_all_slices = dbfile_all_slices.rstrip()
    dbfile_all_slices = dbfile_all_slices.lstrip()


    #  The path provided to the All Slices database should be in this format:  /AFS_backups_in_Cloud/Database/user/b/bkeve/DB_user.bkeve__All_Slices
    #  Now lets make sure the "file name" really is in the right format, the end result will be to derive the AFS volume name from the name of the
    #  All Slices database file name

    file_name = os.path.basename(dbfile_all_slices)

    parsed_list = file_name.split('__')
    match_string = 'All_Slices'
    if not match_string  in  parsed_list[-1]:
        msg = 'Did not find the expected file name format     ' + match_string + ' was not found in file name ' + file_name
        logger.critical(msg)
        sys.exit(1)       

    parsed_list = file_name.split('_')
    match_string = 'DB'
    if not match_string  in  parsed_list[0]:
        msg = 'Did not find the expected file name format     ' + match_string + ' was not found in file name ' + file_name
        logger.critical(msg)
        sys.exit(1)       

    parsed_list.pop(0)
    parsed_list.pop(-1)
    parsed_list.pop(-1)
    parsed_list.pop(-1)

    afs_volume_name = '_'.join(str(e) for e in parsed_list)

    if not os.path.isfile(dbfile_all_slices):
        msg = 'Unable to find the file:  ' + dbfile_all_slices
        logger.critical(msg)
        sys.exit(1)
    else:
        if debug_on:
            print 'Reading the All Slice Database...\n'

        db_directory = os.path.dirname(dbfile_all_slices)

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
                #     Compressed and encrypted file size in terms of 1KB blocksThis dictionary is populate from the contents of the All Slice database
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
                if not database_of_all_slices.has_key(checksum_key):
                    database_of_all_slices[checksum_key] = str(encrypted_file_size) + ' :: ' + str(slice_file_block_cnt) + ' :: ' + str(encrypted_file_block_cnt) + ' :: ' + slice_name + ' :: ' + str(link_count)
                else:
                    msg = 'For the database:   ' + dbfile_all_slices
                    logger.critical(msg)
                    msg = 'Records within the All Slices database should have unique keys'
                    logger.critical(msg)
                    msg = 'Duplicate key value:    ' + checksum_key
                    logger.critical(msg)
                    sys.exit(1)

                # load the dictionary with the checksum key for each slice and uses the slice name as the key into the dictionary
                if not keys_of_all_slices.has_key(slice_name):
                    keys_of_all_slices[slice_name] = checksum_key
                else:
                    msg = 'For the database:   ' + dbfile_all_slices
                    logger.critical(msg)
                    msg = 'Records within the All Slices database should have unique slice names'
                    logger.critical(msg)
                    msg = 'Duplicate key value:    ' + slice_name
                    logger.critical(msg)
                    sys.exit(1)

                # load the dictionary with the link count from the All Slices database
                link_count_all_slices[checksum_key] = link_count

                #  load the dictionary with the slice name
                slice_name_all_slices[checksum_key] = slice_name

                needs_to_be_removed_all_slices[checksum_key] = True

        fh_all_slices.close()


    if debug_on:
        print 'List the directory contents:  ' + db_directory + '\n\n'


    files = [f for f in os.listdir(db_directory) if os.path.isfile(os.path.join(db_directory, f))]

    if debug_on:
        print 'List of all files:  ' + str(files)

    number_of_files = len(files)
    match_string = 'DB__' + afs_volume_name + '__'
    db_file_list = []

    for index in range(0, number_of_files):
        if match_string  in  files[index]:
            full_path = db_directory + '/' + files[index]
            db_file_list.append(full_path)


    if debug_on:
        print 'Number of database files:  ' + str(len(db_file_list))
        print 'List of all database files:  ' + str(db_file_list)

    number_of_files = len(db_file_list)

    for index in range(0, number_of_files):
        daily_dump_db_file = db_file_list[index]

        if not os.path.isfile(daily_dump_db_file):
            msg = 'For the database:   ' + dbfile_all_slices
            logger.critical(msg)
            msg = 'Unable to find the file:  ' + daily_dump_db_file
            logger.critical(msg)
            sys.exit(1)
        else:
            if debug_on:
                print 'Reading the Daily Dump database...   ' + daily_dump_db_file

            fh_daily_dump = open(daily_dump_db_file, 'r')

            for line in fh_daily_dump:
                line = line.rstrip('\r|\n')
                # skip over the HEADER
                tokens = line.split()
                if 'HEADER:' != tokens[0]:
                    daily_dump_record = line.split(' :: ')
                    #  The format of this record used within the daily dump database
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
                    #     The offset into the vos dump wher this slice is located
                    #     The slice number
                    #     The status of the slice is either "unique" or "DUP"
                    #
                    #     The name of the slice file within the object store
                    #
                    #

                    sha1_value = daily_dump_record[0]
                    md5_value = daily_dump_record[1]
                    slice_file_size = daily_dump_record[2]
                    encrypted_file_size = int(daily_dump_record[3])
                    slice_file_block_cnt = int(daily_dump_record[4])
                    encrypted_file_block_cnt = int(daily_dump_record[5])
                    slice_offset = int(daily_dump_record[6])
                    slice_number = int(daily_dump_record[7])
                    status = daily_dump_record[8]
                    slice_name = daily_dump_record[9]

                    checksum_key = sha1_value + ' :: ' + md5_value + ' :: ' + str(slice_file_size)

                    #  Does this slice exist in the All Slices database                
                    if database_of_all_slices.has_key(checksum_key):
                        #  YES  this slice does exist within the All Slices database
                        if keys_of_all_slices.has_key(slice_name):
                            if keys_of_all_slices[slice_name] == checksum_key:
                                #  YES  the slice_name and checksum_key match the values within the All Slices database
                                needs_to_be_removed_all_slices[checksum_key] = False
                                if not daily_dump_link_count.has_key(checksum_key):
                                    daily_dump_link_count[checksum_key] = 1
                                else:
                                    daily_dump_link_count[checksum_key] = daily_dump_link_count[checksum_key] + 1
                            else:
                                # Fatal Error:  slice_name and checksum_key do not match the values within the All Slices database
                                msg = 'For the database:   ' + dbfile_all_slices
                                logger.critical(msg)
                                msg = 'slice_name and checksum_key do not match the values within the All Slices database'
                                logger.critical(msg)
                                msg = 'slice_name value:    ' + slice_name
                                logger.critical(msg)
                                msg = 'daily dump database:    ' + daily_dump_db_file
                                logger.critical(msg)
                                sys.exit(1)
                        else:
                            # Fatal Error:  slice_name from the daily dump is not within the All Slices database
                            msg = 'For the database:   ' + dbfile_all_slices
                            logger.critical(msg)
                            msg = 'the slice_name is not within the All Slices database'
                            logger.critical(msg)
                            msg = 'slice_name value:    ' + slice_name
                            logger.critical(msg)
                            msg = 'daily dump database:    ' + daily_dump_db_file
                            logger.critical(msg)
                            sys.exit(1)
                    else:
                        #  This record in the daily dump has an checksum_key that is not within the All Slices database.  So lets
                        #  save the record so that we can possible update the All Slices database with the correct information
                        if not needs_to_be_added_all_slices.has_key(checksum_key):
                            needs_to_be_added_record[checksum_key] = line
                            if not needs_to_be_added_checksum_key.has_key(slice_name):
                                needs_to_be_added_checksum_key[slice_name] = checksum_key
                                needs_to_be_added_link_count[checksum_key] = 1
                            else:
                                # Fatal Error:  duplicate slice_name value with different checksum_key values
                                msg = 'For the database:   ' + dbfile_all_slices
                                logger.critical(msg)
                                msg = 'duplicate slice_name value with different checksum_key values'
                                logger.critical(msg)
                                msg = 'slice_name value:    ' + slice_name
                                logger.critical(msg)
                                msg = 'daily dump database:    ' + daily_dump_db_file
                                logger.critical(msg)
                                sys.exit(1)
                        else:
                            needs_to_be_added_link_count[checksum_key] = needs_to_be_added_link_count[checksum_key] + 1


            fh_daily_dump.close()


    temp_dictionary = {}

    for checksum_key, boolean_value in needs_to_be_removed_all_slices.iteritems():
        if boolean_value:
            temp_dictionary[checksum_key] = True

    needs_to_be_removed_all_slices = temp_dictionary


    number_of_records = len(database_of_all_slices)
    number_of_records_to_add = len(needs_to_be_added_record)
    number_of_records_to_remove = len(needs_to_be_removed_all_slices)

    if debug_on:
        print 'Number of records            ' + str(number_of_records)
        print 'Number of records to Add     ' + str(number_of_records_to_add)
        print 'Number of records to Remove  ' + str(number_of_records_to_remove)

    slice_name_with_bad_link_counts = {}

    for checksum_key, link_count in daily_dump_link_count.iteritems():
        if link_count_all_slices.has_key(checksum_key):
            if link_count  !=  link_count_all_slices[checksum_key]:
                slice_name_with_bad_link_counts[checksum_key] = slice_name_all_slices[checksum_key]
                msg = 'Link count mismatch for slice name: ' + slice_name_with_bad_link_counts[checksum_key] + '\n'
                logger.info(msg)
                msg = 'All Slice database link count is: ' + str(link_count) + '     Daily Dump databases link count is: ' + str(daily_dump_link_count[checksum_key]) + '\n'
                logger.info(msg)

    number_of_bad_link_counts = len(slice_name_with_bad_link_counts)

    if debug_on:
        print 'Number of mismatched linked counts     ' + str(number_of_bad_link_counts)

    if number_of_bad_link_counts  >  0:
        msg = 'For the database:   ' + dbfile_all_slices
        logger.critical(msg)
        msg = 'Validation failed....   ' + str(number_of_bad_link_counts) + 'Mismatched link counts'
        logger.critical(msg)
        sys.exit(1)       


    if number_of_records_to_add  >  0:
        msg = 'For the database:   ' + dbfile_all_slices
        logger.critical(msg)
        msg = 'Validation failed....   ' + str(number_of_records_to_add) + 'Records to add to the All Slices database'
        logger.critical(msg)
        sys.exit(1)

    if number_of_records_to_remove  >  0:
        msg = 'For the database:   ' + dbfile_all_slices
        logger.critical(msg)
        msg = 'Validation failed....   ' + str(number_of_records_to_remove) + 'Records to remove from the All Slices database'
        logger.critical(msg)
        sys.exit(1)



    #  Convert the "slice_name" into a fully qualified path name into the object store
    ObjStore_stub = '/AFS_backups_in_Cloud/ObjectStore'

    if debug_on:
        print 'Write out the file list for all records within the All Slice database...\n'


    all_slices_file_list = dbfile_all_slices + '__file_list'

    fh_file_list = open(all_slices_file_list, 'w')

    for slice_name, checksum_key  in keys_of_all_slices.iteritems():
        first_letter = slice_name[0]
        second_letter = slice_name[1]
        third_letter = slice_name[2]
        temp_list = slice_name.split('/')
        file_name = temp_list[1]

        msg = ObjStore_stub + '/' + first_letter + '/' + second_letter + '/' + third_letter + '/' + file_name + '\n'
        fh_file_list.write(msg)

    fh_file_list.close()
    
    sys.exit(0)


