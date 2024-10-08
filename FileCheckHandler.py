#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   FileCheckHandler
#  Purpose             :   This module will perform the pre-configured steps before invoking FileCheckUtility.py.
#  Input Parameters    :   data_source, subject_area
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   4th June 2018
#  Last changed by     :   Neelakshi Kulkarni
#  Reason for change   :   Enhancement to provide restartability at file level
# ######################################################################################################################

# Library and external modules declaration
import os
import traceback
import sys

sys.path.insert(0, os.getcwd())
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from FileCheckUtility import FileCheckUtility
from MySQLConnectionManager import MySQLConnectionManager
from PySparkUtility import PySparkUtility
from ConfigUtility import JsonConfigUtility

# all module level constants are defined here
MODULE_NAME = "FileCheckHandler"
PROCESS_NAME = "File Schema Check"

USAGE_STRING = """
SYNOPSIS
    python FileCheckHandler.py <data_source> <subject_area>

    Where
        input parameters : data_source , subject_area

"""


class FileCheckHandler:
    # Default constructor
    def __init__(self, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # ########################################### execute_command ######################################################
    # Purpose            :   Executing the file check
    # Input              :   data source, subject area
    # Output             :   NA
    # ##################################################################################################################

    def execute_file_check(self, file_master_id, cluster_id, workflow_id, process_id, batch_id, batch_id_list):
        status_message = ""
        curr_batch_id = batch_id
        log_info = {}
        try:
            status_message = "Executing File Check function of module FileCheckHandler"
            logger.debug(status_message, extra=self.execution_context.get_context())
            # Input Validations
            if file_master_id is None:
                raise Exception('File Master ID is not provided')
            if batch_id is None:
                raise Exception("Batch ID for current batch is invalid")
            self.execution_context.set_context({"file_master_id": file_master_id,
                                                "batch_id": batch_id})
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Get pre-landing location and data_set_id from MySQL table for given data source and subject area
            data_set_info = CommonUtils().get_dataset_info(file_master_id)
            header_available_flag = data_set_info['header_available_flag']
            # Get file details of current batch
            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, file_master_id)
            # Validate if file details exist for current batch
            if file_details is None:
                status_message = "No details found in file audit table for current batch"
                raise Exception(status_message)

            if header_available_flag == CommonConstants.VARIABLE_FILE_FORMAT_TYPE_KEY:
                status_message = "Header Flag is set to V (Variable File Ingestion). Thus, File Check " \
                                 "would not be performed and a Success Entry would be marked in the log tables"
                for file_detail in file_details:
                    file_id = file_detail['file_id']
                    file_name = file_detail['file_name']
                    log_info = {CommonConstants.BATCH_ID: curr_batch_id,
                                CommonConstants.FILE_ID: file_id,
                                CommonConstants.PROCESS_NAME: PROCESS_NAME,
                                CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_SKIPPED,
                                CommonConstants.FILE_NAME: file_name,
                                CommonConstants.DATASET_ID: file_master_id,
                                CommonConstants.CLUSTER_ID: cluster_id,
                                CommonConstants.WORKFLOW_ID: workflow_id,
                                'process_id': process_id
                                }
                    CommonUtils().create_process_log_entry(self.audit_db, log_info)
                    status_message = "Created process log entry  with status SKIPPED for all the files in this step "
                    logger.debug(status_message, extra=self.execution_context.get_context())
            else:
                pre_landing_location = data_set_info['pre_landing_location']

                if pre_landing_location is None:
                    raise Exception(CommonConstants.PRE_LANDING_LOCATION_NOT_PROVIDED)
                pre_landing_location = str(pre_landing_location).rstrip('/')

                file_num_of_fields = data_set_info['file_num_of_fields']
                field_delimiter = data_set_info['file_field_delimiter']
                # Added for parquet format support
                file_format = data_set_info['file_format']
                if file_num_of_fields is None or field_delimiter is None or header_available_flag is None:
                    raise Exception(CommonConstants.FILE_CHECK_VALID_ARGUMENTS)

                # Fetch column metadata from mysql table for provided dataset
                fetch_column_metadata_query = "select * from {audit_db}.{column_metadata_table} where " \
                                              "dataset_id = ".__add__(str(file_master_id)).format(audit_db=self.audit_db,
                                                                                                  column_metadata_table=CommonConstants.COLUMN_METADATA_TABLE,
                                                                                                  table_metadata_table=CommonConstants.TABLE_METADATA_TABLE)
                column_details = MySQLConnectionManager().execute_query_mysql(fetch_column_metadata_query)
                column_id_seq_dict = dict()
                column_name_list = []
                for column_detail in column_details:
                    column_id_seq_dict.__setitem__(column_detail['column_sequence_number'],
                                                   column_detail['source_column_name'])
                for column_seq in sorted(column_id_seq_dict.keys()):
                    column_name_list.append(column_id_seq_dict.get(column_seq))
                file_process_status = ""
                # Loop through each file and invoke File Check Utility if the file is not already processed
                for file_detail in file_details:
                    # Get detail for each file
                    try:
                        file_id = file_detail['file_id']
                        file_name = file_detail['file_name']
                        log_info = {CommonConstants.BATCH_ID: curr_batch_id,
                                    CommonConstants.FILE_ID: file_id,
                                    CommonConstants.PROCESS_NAME: PROCESS_NAME,
                                    CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_RUNNING,
                                    CommonConstants.FILE_NAME: file_name,
                                    CommonConstants.DATASET_ID: file_master_id,
                                    CommonConstants.CLUSTER_ID: cluster_id,
                                    CommonConstants.WORKFLOW_ID: workflow_id,
                                    'process_id': process_id
                                    }
                        # Logic added for restartability
                        query_str = "Select file_process_status from {audit_db}.{process_log_table_name} where dataset_id = {dataset_id} and  cluster_id = '{cluster_id}' and workflow_id = '{workflow_id}' and process_id = {process_id} and batch_id={batch_id} and file_id = {file_id} and file_process_name='{file_check}'"
                        query = query_str.format(audit_db=self.audit_db,
                                                 process_log_table_name=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                                 dataset_id=file_master_id,
                                                 cluster_id=cluster_id,
                                                 workflow_id=workflow_id,
                                                 process_id=process_id,
                                                 batch_id=batch_id,
                                                 file_id=file_id,
                                                 file_check=CommonConstants.FILE_PROCESS_NAME_FILE_CHECK)

                        result = MySQLConnectionManager().execute_query_mysql(query)

                        if result:
                            file_process_status = str(result[0]['file_process_status'])

                        if file_process_status == "" or file_process_status == CommonConstants.STATUS_FAILED:

                            if file_process_status == "":
                                # Creating process log entry
                                CommonUtils().create_process_log_entry(self.audit_db, log_info)
                                status_message = "Created process log entry "
                                logger.debug(status_message, extra=self.execution_context.get_context())

                            # Get the complete file url
                            file_complete_url = pre_landing_location.__add__('/').__add__(
                                CommonConstants.PARTITION_BATCH_ID + "=" + str(batch_id)).__add__(
                                '/').__add__(
                                CommonConstants.PARTITION_FILE_ID + "=" + str(file_id))
                            self.execution_context.set_context({'file_complete_path': file_complete_url})
                            status_message = 'Fetched complete file url '
                            logger.debug(status_message, extra=self.execution_context.get_context())

                            field_count_check = FileCheckUtility().perform_field_count_check(file_complete_url,
                                                                                             field_delimiter,
                                                                                             file_num_of_fields,
                                                                                             file_format,
                                                                                             file_name)
                            if field_count_check == True:
                                if header_available_flag == 'Y':
                                    header_check = FileCheckUtility().perform_header_check(file_complete_url,
                                                                                           field_delimiter,
                                                                                           column_name_list,
                                                                                           file_format,
                                                                                           file_name)
                                    if header_check == False:
                                        status_message = 'Header Check failed for file id :' + str(file_id)
                                        raise Exception(status_message)
                            else:
                                status_message = 'Field Count check failed for file id :' + str(file_id)
                                raise Exception(status_message)

                        else:
                            status_message = 'File : ' + str(file_name) + ' with Batch ID : ' + str(
                                batch_id) + ' is already processed'
                            logger.debug(status_message, extra=self.execution_context.get_context())

                    except Exception as e:
                        log_info[CommonConstants.BATCH_ID] = curr_batch_id
                        log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                        log_info['status_id'] = CommonConstants.FAILED_ID
                        log_info['record_count'] = str(0)
                        log_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace('"',
                                                                                                                       '')
                        CommonUtils().update_process_log_by_batch(self.audit_db, log_info)
                        batches = batch_id_list.split(",")
                        for batch in batches:
                            log_info[CommonConstants.BATCH_ID] = batch
                            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                            log_info['status_id'] = CommonConstants.FAILED_ID
                            log_info['record_count'] = str(0)
                            log_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace(
                                '"', '')

                            CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, log_info)
                            CommonUtils().update_batch_status(self.audit_db, log_info)

                        error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                                " ERROR MESSAGE: " + str(e)

                        self.execution_context.set_context({"traceback": error})
                        logger.error(error, extra=self.execution_context.get_context())
                        raise e
                if header_available_flag == CommonConstants.VARIABLE_FILE_FORMAT_TYPE_KEY:
                    status_message = "Header Flag is V ( Variable File Ingestion) " \
                                     "Thus Record Count would not be populated in the logging tables"
                    logger.info()
                if file_process_status == "" or file_process_status == CommonConstants.STATUS_FAILED:
                    file_url_till_batch = pre_landing_location.__add__('/').__add__(
                        CommonConstants.PARTITION_BATCH_ID + "=" + str(batch_id))
                    if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                        record_count = PySparkUtility(self.execution_context).get_parquet_record_count(
                            file_url_till_batch)

                    elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                        record_count = PySparkUtility(self.execution_context).get_excel_record_count(
                            file_url_till_batch)

                    else:
                        record_count = PySparkUtility(self.execution_context).get_file_record_count(file_url_till_batch,
                                                                                                    header_available_flag)
                    log_info['record_count'] = record_count
                    # Update process log
                    log_info['message'] = "Process Completed Successfully"
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
                    log_info['status_id'] = CommonConstants.SUCCESS_ID
                    CommonUtils().update_process_log_by_batch(self.audit_db, log_info)

                status_message = "Completed check of pre-landing files "
                self.execution_context.set_context({"pre_landing_location": pre_landing_location})
                logger.debug(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            log_info[CommonConstants.BATCH_ID] = curr_batch_id
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            log_info['status_id'] = CommonConstants.FAILED_ID
            log_info['record_count'] = str(0)
            log_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace('"', '')
            CommonUtils().update_process_log_by_batch(self.audit_db, log_info)
            batches = batch_id_list.split(",")
            for batch in batches:
                log_info[CommonConstants.BATCH_ID] = batch
                log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                log_info['status_id'] = CommonConstants.FAILED_ID
                log_info['record_count'] = str(0)
                log_info['message'] = str(e)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace('"', '')
                CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, log_info)
                CommonUtils().update_batch_status(self.audit_db, log_info)

            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())

            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e


if __name__ == '__main__':
    file_master_id = sys.argv[1]
    cluster_id = sys.argv[2]
    workflow_id = sys.argv[3]
    process_id = sys.argv[4]
    batch_id = sys.argv[5]
    batch_id_list = sys.argv[6]
    if file_master_id is None or cluster_id is None or workflow_id is None or process_id is None or batch_id is None or batch_id_list is None:
        raise Exception(CommonConstants.PROVIDE_ALL_ARGUMENTS)
    batch_id = str(batch_id).rstrip("\n\r")
    file_check_handler = FileCheckHandler()
    return_value = file_check_handler.execute_file_check(file_master_id, cluster_id, workflow_id, process_id, batch_id,
                                                         batch_id_list)
    status_msg = "Completed execution for File Check Handler with status "
    sys.stdout.write("batch_id=" + batch_id)

