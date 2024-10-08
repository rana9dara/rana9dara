#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   FileDownloadHandler
#  Purpose             :   This module will perform the pre-configured steps before invoking FileDownloadUtility.py.
#  Input Parameters    :   dataset_id
#  Output Value        :
#  Pre-requisites      :
#  Last changed on     :   4th June 2018
#  Last changed by     :   Neelakshi Kulkarni
#  Reason for change   :   Enhancement to provide restartability at file level
# ######################################################################################################################

# Library and external modules declaration
import sys
import traceback
import os.path

import datetime

import re
import scipy.stats as stat

sys.path.insert(0, os.getcwd())
from pyspark.sql.functions import col, when, lit, length, count, concat
from pyspark.sql.types import StructType, StructField, StringType
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from PySparkUtility import PySparkUtility
from ConfigUtility import JsonConfigUtility
from S3MoveCopyUtility import S3MoveCopyUtility

from MySQLConnectionManager import MySQLConnectionManager

# all module level constants are defined here
MODULE_NAME = "FileDownloadHandler"
PROCESS_NAME = "Download Files from source location"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_KEY = "status"
ERROR_KEY = "error"
RESULT_KEY = "result"

USAGE_STRING = """
SYNOPSIS
    python FileDownloadHandler.py <dataset_id>

    Where
        input parameters : dataset_id

"""


class FileDownloadHandler:
    # Default constructor
    def __init__(self, dataset_id, cluster_id=None, workflow_id=None, process_id= None, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context

        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"dataset_id": dataset_id})
        self.execution_context.set_context({"cluster_id": cluster_id})
        self.execution_context.set_context({"workflow_id": workflow_id})
        self.execution_context.set_context({"process_id": process_id})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # ########################################### execute_command ######################################################
    # Purpose            :   Executing the file download command (main function)
    # Input              :   data source, subject area
    # Output             :   NA
    # ##################################################################################################################
    def execute_file_download(self, dataset_id=None, cluster_id=None, workflow_id=None, process_id=None):
        status_message = ""
        curr_batch_id = None
        log_info = {}
        audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        cluster_mode = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                             "cluster_mode"])
        batch_ids = ""
        process_status_list = []
        try:
            status_message = "Executing File Download function of module FileDownloadHandler"
            if dataset_id is None:
                raise Exception('Dataset ID is not provided')
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Get source file location from MySQL table for given data source and subject area
            dataset_info = CommonUtils(self.execution_context).get_dataset_info(dataset_id)
            source_file_location = dataset_info['file_source_location']
            header_available_flag = dataset_info['header_available_flag']
            file_format = dataset_info['file_format']
            if source_file_location is None:
                raise Exception(CommonConstants.SOURCE_LOCATION_NOT_PROVIDED)
            source_file_location = str(source_file_location).rstrip('/').__add__('/')
            source_file_pattern = dataset_info['file_name_pattern']
            file_pattern_flag = dataset_info['static_file_pattern']
            source_file_type = dataset_info['file_source_type']
            field_delimiter = dataset_info['file_field_delimiter']
            status_message = "Completed fetch of source file location for " + str(dataset_id)
            status_message = status_message + " - " + source_file_location
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Get target file location from MySQL table for given data source and subject area
            target_file_location = dataset_info['pre_landing_location']
            if target_file_location is None:
                raise Exception(CommonConstants.PRE_LANDING_LOCATION_NOT_PROVIDED)
            target_file_location = str(target_file_location).rstrip('/')
            status_message = "Completed fetch of target file location for " + str(dataset_id)
            status_message = status_message + " - " + target_file_location
            logger.debug(status_message, extra=self.execution_context.get_context())

            # retrieve schema from ctl_column_metadata
            schema_query_str = "select column_name from {audit_db}.{column_metadata_table} where dataset_id={dataset_id}"
            schema_query = schema_query_str.format(audit_db=audit_db,
                                                   column_metadata_table='ctl_column_metadata',
                                                   dataset_id=dataset_id)
            column_names_result = MySQLConnectionManager().execute_query_mysql(schema_query)
            column_name_list = []
            for column_result in column_names_result:
                column_name_list.append(column_result['column_name'])
            schema = StructType([
                StructField(column_name, StringType(), True) for (column_name) in column_name_list
            ])

            # To fetch s3 file list located at source location
            s3_file_list = CommonUtils(self.execution_context).list_files_in_s3_directory(source_file_location, cluster_mode, source_file_pattern)
            if not s3_file_list:
                raise Exception("No files available for Processing")
            load_files = []
            failed_query_str = "Select * from {audit_db}.{process_log_table_name} where dataset_id = {dataset_id} and  cluster_id = '{cluster_id}' and workflow_id = '{workflow_id}' and process_id = {process_id} and file_process_name='{pre_landing}' and file_process_status = ('{status_failed}')"
            failed_query=failed_query_str.format(audit_db=audit_db,
                                                process_log_table_name=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                                dataset_id=dataset_id,
                                                cluster_id=cluster_id,
                                                workflow_id=workflow_id,
                                                process_id=process_id,
                                                pre_landing=CommonConstants.FILE_PROCESS_NAME_PRE_LANDING,
                                                status_failed=CommonConstants.STATUS_FAILED)
            failed_result=MySQLConnectionManager(self.execution_context).execute_query_mysql(failed_query)
            succeeded_query_str = "Select * from {audit_db}.{process_log_table_name} where dataset_id = {dataset_id} and  cluster_id = '{cluster_id}' and workflow_id = '{workflow_id}' and process_id = {process_id} and file_process_name='{pre_landing}' and file_process_status in ('{status_success}','{status_inprogress}','{status_skipped}')"
            succeeded_query = succeeded_query_str.format(audit_db=audit_db,
                                                         process_log_table_name=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                                         dataset_id=dataset_id,
                                                         cluster_id=cluster_id,
                                                         workflow_id=workflow_id,
                                                         process_id=process_id,
                                                         pre_landing=CommonConstants.FILE_PROCESS_NAME_PRE_LANDING,
                                                         status_success=CommonConstants.STATUS_SUCCEEDED,
                                                         status_inprogress=CommonConstants.IN_PROGRESS_DESC,
                                                         status_skipped=CommonConstants.STATUS_SKIPPED)
            succeeded_result = MySQLConnectionManager(self.execution_context).execute_query_mysql(succeeded_query)
            file_to_batch_id = {}
            file_to_file_id = {}

            batch_ids_for_failed_set = set()
            batch_ids_for_succeeded_set = set()
            batch_ids_for_unprocessed_set = set()

            failed_files_list=[]
            succeeded_files_list=[]
            unprocessed_files_list=[]
            failed_files_set = set()
            succeeded_files_set = set()
            unprocessed_files_set = set(s3_file_list)
            len_failed_result = len(failed_result)
            for i in range(0, len_failed_result):
                failed_files_set.add(failed_result[i]['file_name'])
            failed_files_list = list(failed_files_set)
            len_succeeded_result = len(succeeded_result)
            for i in range (0, len_succeeded_result):
                succeeded_files_set.add(succeeded_result[i]['file_name'])
            succeeded_files_list = list(succeeded_files_set)
            unprocessed_files_list = list(unprocessed_files_set.difference(failed_files_set.union(succeeded_files_set)))

            status_message = "List of unprocessed files : ",str(unprocessed_files_list)
            logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "List of previously failed files : ",str(failed_files_list)
            logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "List of previously succeeded files : ",str(succeeded_files_list)
            logger.debug(status_message, extra=self.execution_context.get_context())

            if (len(unprocessed_files_list) != 0 or file_pattern_flag == 'Y'):
                for file in unprocessed_files_list:
                    file_name = file
                    existing_file_details = CommonUtils(self.execution_context).get_loaded_file_details(dataset_id, file_name,
                                                                                           PROCESS_NAME)
                    #file_pattern_flag == Y picks all the files of the same name that have already been processed
                    if not existing_file_details or file_pattern_flag == 'Y':
                        load_files.append(file)

                if not load_files:
                    status_message = "Files are already processed"
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    raise Exception(status_message)

                # Call a function which returns the file_name and batch_order mapping and store the mapping in a dictionary
                file_batch_mapping = self.get_file_batch_order(dataset_id, source_file_pattern, load_files)

                batch_order = []
                batch_dict = {}
                ## Create new batches for current run
                for f, bo in file_batch_mapping.items():

                    if len(batch_order) == 0 or file_batch_mapping[f] not in batch_order:
                        batch_order.append(bo)
                        curr_batch_id = CommonUtils(self.execution_context).create_batch_entry(audit_db, dataset_id, cluster_id,
                                                                                  workflow_id,
                                                                                  process_id)

                        log_info[CommonConstants.BATCH_ID] = curr_batch_id
                        log_info[CommonConstants.CLUSTER_ID] = cluster_id
                        log_info[CommonConstants.WORKFLOW_ID] = workflow_id
                        status_message = "Created new batch with id :" + str(curr_batch_id)
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        batch_dict[bo] = curr_batch_id
                        batch_ids_for_unprocessed_set.add(str(curr_batch_id))

                for f in load_files:
                    bo = file_batch_mapping.get(f)
                    bid = batch_dict.get(bo)
                    file_to_batch_id[f] = bid
                # To copy each file from source to target location
                status_message = "Started copy of files from source to target location for unprocesses files"
                logger.debug(status_message, extra=self.execution_context.get_context())

                for s3_file_path in load_files:
                    # Accumulation of process log entry and file audit details
                    try:
                        curr_batch_id = file_to_batch_id.get(s3_file_path)
                        file_audit_info = {CommonConstants.FILE_NAME: s3_file_path,
                                           CommonConstants.FILE_STATUS: CommonConstants.STATUS_RUNNING,
                                           CommonConstants.DATASET_ID: dataset_id,
                                           CommonConstants.BATCH_ID: curr_batch_id,
                                           CommonConstants.CLUSTER_ID: cluster_id,
                                           CommonConstants.WORKFLOW_ID: workflow_id,
                                           'process_id': process_id}
                        file_id = CommonUtils(self.execution_context).create_file_audit_entry(audit_db,
                                                                                 file_audit_info)
                        status_message = "Creating process log entry "
                        self.execution_context.set_context({CommonConstants.BATCH_ID: curr_batch_id})
                        self.execution_context.set_context({CommonConstants.FILE_ID: file_id})
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        log_info = {CommonConstants.BATCH_ID: curr_batch_id, CommonConstants.FILE_ID: file_id,
                                    CommonConstants.PROCESS_NAME: PROCESS_NAME,
                                    CommonConstants.FILE_NAME: s3_file_path,
                                    CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING,
                                    CommonConstants.DATASET_ID: dataset_id,
                                    CommonConstants.CLUSTER_ID: cluster_id,
                                    CommonConstants.WORKFLOW_ID: workflow_id,
                                    'process_id': process_id
                                    }
                        # Create process log entry
                        CommonUtils(self.execution_context).create_process_log_entry(audit_db, log_info)
                        status_message = "Created process log entry "
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        # Invoke copy command
                        file_name = str(CommonUtils(self.execution_context).get_s3_file_name(s3_file_path))
                        target_dir = target_file_location.__add__('/'+ CommonConstants.PARTITION_BATCH_ID+'=' + str(curr_batch_id)). \
                            __add__('/'+CommonConstants.PARTITION_FILE_ID+'=' + str(file_id))
                        target_file_name = str(target_dir).__add__('/' + file_name)
                        # added logic to check if batch_id and file_id columns are to be added to the file
                        column_list_std = [col.upper() for col in column_name_list]
                        if column_list_std.__contains__('BATCH_ID') and column_list_std.__contains__('FILE_ID'):
                            status_message = "Batch_Id and File_Id columns are configured in column metadata. Adding columns to data"
                            logger.debug(status_message, extra=self.execution_context.get_context())
                            # Get spark and sql context
                            module_name_for_spark = str(
                                MODULE_NAME + "_dataset_" + dataset_id + "_process_" + process_id)
                            spark_context = PySparkUtility(self.execution_context).get_spark_context(
                                module_name_for_spark,CommonConstants.HADOOP_CONF_PROPERTY_DICT)
                            status_message = 'Fetched spark sql context'
                            logger.debug(status_message, extra=self.execution_context.get_context())

                            if header_available_flag is not None and header_available_flag == 'Y':
                                if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                                    source_df = spark_context.read.format("parquet").load(s3_file_path)

                                elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                                    source_df = spark_context.read.format('com.crealytics.spark.excel').option\
                                        ('useHeader', 'true').load(target_file_name)

                                else:
                                    if CommonConstants.MULTILINE_READ_FLAG == "Y":
                                        source_df = spark_context.read.format("csv").option("multiline","true").option("header", "true").option(
                                            "delimiter", str(field_delimiter)).schema(schema).load(s3_file_path)

                                    else:
                                        source_df = spark_context.read.format("csv").option("header", "true").option(
                                            "delimiter", str(field_delimiter)).schema(schema).load(s3_file_path)

                            else:
                                if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                                    source_df = spark_context.read.format("parquet").load(s3_file_path)

                                elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                                    source_df = spark_context.read.format('com.crealytics.spark.excel').load(target_file_name)
                                else:
                                    if CommonConstants.MULTILINE_READ_FLAG == "Y":
                                        source_df = spark_context.read.format("csv").option("multiline","true").option(
                                            "delimiter", str(field_delimiter)).schema(schema).load(s3_file_path)

                                    else:
                                        source_df = spark_context.read.format("csv").option(
                                            "delimiter", str(field_delimiter)).schema(schema).load(s3_file_path)

                            source_df_with_batch_file = source_df.withColumn(CommonConstants.BATCH_ID, lit(curr_batch_id)).withColumn(CommonConstants.FILE_ID, lit(file_id))

                            if cluster_mode == 'EMR':
                                if header_available_flag is not None and header_available_flag == 'Y':
                                    if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                                        source_df_with_batch_file.write.parquet(target_dir)

                                    elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                                        source_df_with_batch_file.write.format('com.crealytics.spark.excel').option \
                                            ('useHeader', 'true').save(target_file_name)
                                    else:
                                        source_df_with_batch_file.write.option("delimiter", str(field_delimiter)).save(
                                            path=target_dir, format='csv', mode="overwrite", header=True)
                                else:
                                    if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                                        source_df_with_batch_file.write.parquet(target_dir)

                                    elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                                        source_df_with_batch_file.write.format('com.crealytics.spark.excel').save(target_file_name)
                                    else:
                                        source_df_with_batch_file.write.option("delimiter", str(field_delimiter)).save(
                                            path=target_dir, format='csv', mode="overwrite")
                            else:
                                if header_available_flag is not None and header_available_flag == 'Y':
                                    if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                                        source_df_with_batch_file.write.parquet(target_dir)

                                    elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                                        source_df_with_batch_file.write.format('com.crealytics.spark.excel').option \
                                            ('useHeader', 'true').save(target_file_name)
                                    else:
                                        source_df_with_batch_file.write.option("delimiter", str(field_delimiter)).save(
                                            path=target_dir, format='csv', mode="overwrite", header=True)
                                else:
                                    if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                                        source_df_with_batch_file.write.parquet(target_dir)

                                    elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                                        source_df_with_batch_file.write.format('com.crealytics.spark.excel').save(target_file_name)
                                    else:
                                        source_df_with_batch_file.write.option("delimiter", str(field_delimiter)).save(
                                            path=target_dir, format='csv', mode="overwrite")

                        else:
                            status_message = "Batch_Id and File_Id columns are not configured in column metadata. Copying file from source to target location"
                            logger.debug(status_message, extra=self.execution_context.get_context())
                            if cluster_mode == 'EMR':
                                S3MoveCopyUtility(self.execution_context).copy_s3_to_s3_with_s3cmd(s3_file_path, str(target_file_name))
                            else:
                                CommonUtils(self.execution_context).perform_hadoop_distcp(s3_file_path, str(target_file_name))
                            status_message = "Completed copy of file from " + s3_file_path + " to " + target_file_location
                            logger.debug(status_message, extra=self.execution_context.get_context())

                        if CommonUtils(self.execution_context).is_file_with_invalid_extension(target_file_name):
                            corrected_filename = CommonUtils(self.execution_context).correct_file_extension(target_file_name,
                                                                                               CommonConstants.PRE_LANDING_LOCATION_TYPE)
                        else:
                            corrected_filename = target_file_name

                        status_message = "Completed copy of file from " + s3_file_path + " to " + target_file_location
                        logger.debug(status_message, extra=self.execution_context.get_context())

                        process_status_list.append(CommonConstants.STATUS_SUCCEEDED)

                    except Exception as exception:
                        batch_ids_result = CommonUtils().get_batch_ids_by_process_info(audit_db,dataset_id,cluster_id,workflow_id,process_id)
                        for i in range(0,len(batch_ids_result)):
                            curr_batch_id = batch_ids_result[i]['batch_id']
                            log_info[CommonConstants.BATCH_ID] = curr_batch_id
                            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                            log_info['status_id'] = CommonConstants.FAILED_ID
                            log_info['record_count'] = str(0)
                            log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace('"', '')
                            CommonUtils(self.execution_context).update_process_log_by_batch(audit_db, log_info)
                            CommonUtils(self.execution_context).update_file_audit_status_by_batch_id(audit_db, log_info)
                            CommonUtils(self.execution_context).update_batch_status(audit_db, log_info)
                            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                                " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
                            self.execution_context.set_context({"traceback": error})
                            logger.error(error, extra=self.execution_context.get_context())
                        raise exception
                status_message = "Completed copy of all files from source to target location"
                logger.debug(status_message, extra=self.execution_context.get_context())

            if len(failed_files_list) != 0:
                # To copy each file from source to target location
                status_message = "Started copy of files from source to target location for failed files"
                logger.debug(status_message, extra=self.execution_context.get_context())
                #cleanup_success = CommonUtils().clean_hdfs_directory(target_file_location)
                #get files corresponding to failed batches
                len_failed_result = len(failed_result)
                load_files=[]
                for i in range(0,len_failed_result):
                    batch = failed_result[i]['batch_id']
                    file = failed_result[i]['file_name']
                    fid = failed_result[i]['file_id']
                    file_to_file_id[file] = fid
                    file_to_batch_id[file] = batch
                    load_files.append(file)
                    batch_ids_for_failed_set.add(str(batch))

                for s3_file_path in load_files:
                    # Accumulation of process log entry and file audit details
                    try:
                        curr_batch_id = file_to_batch_id.get(s3_file_path)
                        file_id = file_to_file_id.get(s3_file_path)
                        log_info = {CommonConstants.BATCH_ID: curr_batch_id,
                                    CommonConstants.FILE_ID: file_id,
                                    CommonConstants.PROCESS_NAME: PROCESS_NAME,
                                    CommonConstants.FILE_NAME: s3_file_path,
                                    CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING,
                                    CommonConstants.DATASET_ID: dataset_id,
                                    CommonConstants.CLUSTER_ID: cluster_id,
                                    CommonConstants.WORKFLOW_ID: workflow_id,
                                    'process_id': process_id
                                    }
                        # Invoke hadoop distcp command
                        file_name = str(CommonUtils(self.execution_context).get_s3_file_name(s3_file_path))
                        target_dir = target_file_location.__add__('/'+CommonConstants.PARTITION_BATCH_ID+'=' + str(curr_batch_id)). \
                            __add__('/'+CommonConstants.PARTITION_FILE_ID+'=' + str(file_id))
                        target_file_name = str(target_dir).__add__('/' + file_name)
                        # added logic to check if batch_id and file_id columns are to be added to the file
                        if column_name_list.__contains__('BATCH_ID') and column_name_list.__contains__('FILE_ID'):
                            status_message = "Batch_Id and File_Id columns are configured in column metadata. Adding columns to data"
                            logger.debug(status_message, extra=self.execution_context.get_context())
                            # Get spark and sql context
                            module_name_for_spark = str(MODULE_NAME+"_dataset_"+dataset_id+"_process_"+process_id)
                            spark_context = PySparkUtility(self.execution_context).get_spark_context(module_name_for_spark,CommonConstants.HADOOP_CONF_PROPERTY_DICT)
                            status_message = 'Fetched spark sql context'
                            logger.debug(status_message, extra=self.execution_context.get_context())

                            if header_available_flag is not None and header_available_flag == 'Y':
                                source_df = spark_context.read.format("csv").option("header", "true").option("delimiter",str(field_delimiter)).schema(schema).load(s3_file_path)
                            else:
                                source_df = spark_context.read.format("csv").option("delimiter",str(field_delimiter)).schema(schema).load(s3_file_path)
                            source_df_with_batch_file = source_df.withColumn(CommonConstants.BATCH_ID, lit(curr_batch_id)).withColumn(CommonConstants.FILE_ID, lit(file_id))
                            if cluster_mode == 'EMR':
                                if header_available_flag is not None and header_available_flag == 'Y':
                                    source_df_with_batch_file.write.option("delimiter", str(field_delimiter)).save(
                                        path=target_dir, format='csv', mode="overwrite", header=True)
                                else:
                                    source_df_with_batch_file.write.option("delimiter", str(field_delimiter)).save(
                                        path=target_dir, format='csv', mode="overwrite")

                            else:
                                if header_available_flag is not None and header_available_flag == 'Y':
                                    source_df_with_batch_file.write.option("delimiter", str(field_delimiter)).save(
                                        path=target_dir, format='csv', mode="overwrite", header=True)
                                else:
                                    source_df_with_batch_file.write.option("delimiter", str(field_delimiter)).save(
                                        path=target_dir, format='csv', mode="overwrite")
                        else:
                            status_message = "Batch_Id and File_Id columns are not configured in column metadata. Copying file from source to target location"
                            logger.debug(status_message, extra=self.execution_context.get_context())
                            if cluster_mode == 'EMR':
                                S3MoveCopyUtility(self.execution_context).copy_s3_to_s3_with_s3cmd(s3_file_path, str(
                                    target_file_name))
                            else:
                                CommonUtils(self.execution_context).perform_hadoop_distcp(s3_file_path,
                                                                                          str(target_file_name))
                            status_message = "Completed copy of file from " + s3_file_path + " to " + target_file_location
                            logger.debug(status_message, extra=self.execution_context.get_context())

                        if CommonUtils(self.execution_context).is_file_with_invalid_extension(target_file_name):
                            corrected_filename = CommonUtils(self.execution_context).correct_file_extension(
                                target_file_name,
                                CommonConstants.PRE_LANDING_LOCATION_TYPE)
                        else:
                            corrected_filename = target_file_name

                        status_message = "Completed copy of file from " + s3_file_path + " to " + target_file_location
                        logger.debug(status_message, extra=self.execution_context.get_context())

                        process_status_list.append(CommonConstants.STATUS_SUCCEEDED)

                    except Exception as exception:
                        batch_ids_result = CommonUtils().get_batch_ids_by_process_info(audit_db, dataset_id, cluster_id,
                                                                                       workflow_id, process_id)
                        for i in range(0, len(batch_ids_result)):
                            curr_batch_id = batch_ids_result[i]['batch_id']
                            log_info[CommonConstants.BATCH_ID] = curr_batch_id
                            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                            log_info['status_id'] = CommonConstants.FAILED_ID
                            log_info['record_count'] = str(0)
                            log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'",
                                                                                                                  "").replace(
                                '"', '')
                            CommonUtils(self.execution_context).update_process_log_by_batch(audit_db, log_info)
                            CommonUtils(self.execution_context).update_file_audit_status_by_batch_id(audit_db, log_info)
                            CommonUtils(self.execution_context).update_batch_status(audit_db, log_info)
                            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                                    " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
                            self.execution_context.set_context({"traceback": error})
                            logger.error(error, extra=self.execution_context.get_context())
                        raise exception
                status_message = "Completed copy of all files from source to target location"
                logger.debug(status_message, extra=self.execution_context.get_context())

            if len(succeeded_files_list) != 0:
                len_succeeded_result = len(succeeded_result)
                for i in range(0, len_succeeded_result):
                    batch = succeeded_result[i]['batch_id']
                    batch_ids_for_succeeded_set.add(str(batch))
            batch_ids_unprocessed_failed_list = list(batch_ids_for_unprocessed_set.union(batch_ids_for_failed_set))

            #logic for calculating and updating record count at batch level
            process_status_flag = process_status_list.__contains__(CommonConstants.STATUS_FAILED) or process_status_list.__contains__(CommonConstants.STATUS_RUNNING)\
            or process_status_list.__contains__(CommonConstants.STATUS_SKIPPED)

            if not process_status_flag:
                for b in batch_ids_unprocessed_failed_list:
                    try:
                        curr_batch_id = b
                        target_location_till_batch = target_file_location.__add__("/"+CommonConstants.PARTITION_BATCH_ID+"=").__add__(str(b))
                        if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                            record_count = PySparkUtility(self.execution_context).get_parquet_record_count(
                                target_location_till_batch)

                        elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                            record_count = PySparkUtility(self.execution_context).get_excel_record_count(
                                target_location_till_batch)

                        else:
                            record_count = PySparkUtility(self.execution_context).get_file_record_count(target_location_till_batch,header_available_flag)

                        log_info = {CommonConstants.BATCH_ID: b,
                                   CommonConstants.PROCESS_NAME: PROCESS_NAME,
                                   CommonConstants.DATASET_ID: dataset_id,
                                   CommonConstants.CLUSTER_ID: cluster_id,
                                   CommonConstants.WORKFLOW_ID: workflow_id,
                                   'process_id': process_id
                                   }
                        log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
                        log_info['status_id'] = CommonConstants.SUCCESS_ID
                        log_info['record_count'] = record_count
                        log_info['message'] = "Process Completed Successfully"
                        CommonUtils(self.execution_context).update_process_log_by_batch(audit_db,log_info)

                    except Exception as exception:
                        batch_ids_result = CommonUtils().get_batch_ids_by_process_info(audit_db, dataset_id, cluster_id,
                                                                                       workflow_id, process_id)
                        for i in range(0, len(batch_ids_result)):
                            curr_batch_id = batch_ids_result[i]['batch_id']
                            log_info[CommonConstants.BATCH_ID] = curr_batch_id
                            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                            log_info['status_id'] = CommonConstants.FAILED_ID
                            log_info['record_count'] = str(0)
                            log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'",
                                                                                                                  "").replace(
                                '"', '')
                            CommonUtils(self.execution_context).update_process_log_by_batch(audit_db, log_info)
                            CommonUtils(self.execution_context).update_file_audit_status_by_batch_id(audit_db, log_info)
                            CommonUtils(self.execution_context).update_batch_status(audit_db, log_info)
                            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                                    " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
                            self.execution_context.set_context({"traceback": error})
                            logger.error(error, extra=self.execution_context.get_context())
                        raise exception
            batch_ids_list = list(
                batch_ids_for_unprocessed_set.union(batch_ids_for_failed_set.union(batch_ids_for_succeeded_set)))
            for b in batch_ids_list:
                batch_ids = batch_ids + str(b) + ","
            len_batch_ids_1 = len(batch_ids) - 1
            batch_ids = batch_ids[0:len_batch_ids_1]
            return str(batch_ids)
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            batch_ids_result = CommonUtils().get_batch_ids_by_process_info(audit_db, dataset_id, cluster_id,
                                                                           workflow_id, process_id)
            for i in range(0, len(batch_ids_result)):
                curr_batch_id = batch_ids_result[i]['batch_id']
                log_info[CommonConstants.BATCH_ID] = curr_batch_id
                log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                log_info['status_id'] = CommonConstants.FAILED_ID
                log_info['record_count'] = str(0)
                log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace(
                    '"', '')
                CommonUtils(self.execution_context).update_batch_status(audit_db, log_info)
                error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                        " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
                self.execution_context.set_context({"traceback": error})
                logger.error(error, extra=self.execution_context.get_context())
            raise exception

    def get_file_batch_order(self, dataset_id, source_file_pattern, files):
        status_message = ""
        audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        cluster_mode = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                             "cluster_mode"])
        my_sql_utility_conn = MySQLConnectionManager(self.execution_context)
        multiple_batch_flag = CommonConstants.MULTIPLE_BATCH_FLAG
        try:
            status_message = "Starting function to generate file to batch mapping for dataset_id,files - "+ str(dataset_id) +" "+ str(files)
            logger.debug(status_message, extra=self.execution_context.get_context())

            file_order_par = []
            batch_order = []
            if multiple_batch_flag == "Y":
                if "YYYYMMDD" in source_file_pattern:
                    for f in files:
                        m = re.search("_([0-9]{4})(1[0-2]|0[1-9])([0-3])([0-9])", f)
                        tmp = m.group(0)
                        y = int(tmp[1:5])
                        m = int(tmp[5:7])
                        d = int(tmp[7:9])
                        fd = datetime.datetime(year=y, month=m, day=d)
                        file_order_par.append(fd)
                elif "MMDDYYYY" in source_file_pattern:
                    for f in files:
                        m = re.search("(1[0-2]|0[1-9])([0-3])([0-9])([0-9]{4})", f)
                        tmp = m.group(0)
                        m = int(tmp[0:2])
                        d = int(tmp[2:4])
                        y = int(tmp[4:8])
                        fd = datetime.datetime(year=y, month=m, day=d)
                        file_order_par.append(fd)
                elif "MmmYyy" in source_file_pattern:
                    for f in files:
                        m = re.search("(M1[0-2]|M0[1-9])(Y[0-9]{2})", f)
                        tmp = m.group(0)
                        m = tmp[1:3]
                        d = '01'
                        y = tmp[4:6]
                        fd = datetime.datetime.strptime(d + m + y, '%d%m%y').date()
                        file_order_par.append(fd)
                elif "_Www" in source_file_pattern:
                    for f in files:
                        m = re.search("(_W0[1-9]|_W[1-4][0-9]|_W5[0-2])", f)
                        tmp = m.group(0)
                        w = int(tmp[2:4])
                        file_order_par.append(w)
                else:
                    for f in files:
                        fd = 0
                        file_order_par.append(fd)
            else:
                for f in files:
                    fd = 0
                    file_order_par.append(fd)
            batch_order = stat.rankdata(file_order_par, method='dense')
            file_batch_order = dict(list(zip(files, batch_order)))
            status_message = "Completed function to generate file to batch mapping with file batch order : " + str(file_batch_order)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return file_batch_order
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            raise exception

if __name__ == '__main__':
    dataset_id = sys.argv[1]
    cluster_id = sys.argv[2]
    workflow_id = sys.argv[3]
    process_id = sys.argv[4]
    write_batch_id_to_file_flag = sys.argv[5]

    if dataset_id is None or cluster_id is None or workflow_id is None or process_id is None or write_batch_id_to_file_flag is None :
        raise Exception(CommonConstants.PROVIDE_ALL_ARGUMENTS)

    file_download_handler = FileDownloadHandler(dataset_id, cluster_id, workflow_id, process_id)
    batch_id = file_download_handler.execute_file_download(dataset_id, cluster_id, workflow_id, process_id)

    if write_batch_id_to_file_flag == 'Y':
        batch_file_path = sys.argv[6]
        if batch_file_path is None:
            raise Exception(CommonConstants.PROVIDE_BATCH_FILE_PATH)
        batch_dir_path = os.path.dirname(batch_file_path)
        if not os.path.exists(batch_dir_path):
            CommonUtils().execute_shell_command("mkdir -p " + str(batch_dir_path))

        file_content = open(str(batch_file_path), "w+")
        file_content.write(str(batch_id))

    status_msg = "Completed execution for File Download Handler"
    sys.stdout.write("batch_id=" + str(batch_id))
