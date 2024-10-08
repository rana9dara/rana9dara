#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   Archive Source File Wrapper
#  Purpose             :   This Wrapper Will Archive Source File
#  Input Parameters    :   Dataset Id
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   6th June 2018
#  Last changed by     :   Neelakshi Kulkarni
#  Reason for change   :   Enhancement to provide restartability at file level
# ######################################################################################################################

# Library and external modules declaration
import json
import datetime
import sys
import traceback
import time
import os

sys.path.insert(0, os.getcwd())
from S3MoveCopyUtility import S3MoveCopyUtility
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
from CommonUtils import CommonUtils
from LogSetup import logger
from ConfigUtility import JsonConfigUtility
from PySparkUtility import PySparkUtility

MODULE_NAME = "ArchiveSourceFileWrapper"
PROCESS_NAME = 'Archive File From Source'


class ArchiveFileHandler:
    def __init__(self, dataset_id=None, cluster_id=None, workflow_id=None, execution_context=None):
        self.execution_context = ExecutionContext()
        if execution_context is None:
            self.execution_context.set_context({"module_name": MODULE_NAME})
        else:
            self.execution_context = execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"dataset_id": dataset_id})
        self.execution_context.set_context({"cluster_id": cluster_id})
        self.execution_context.set_context({"workflow_id": workflow_id})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # ########################################### archive_source_file_handler ###############################################
    # Purpose            :   Archiving Source File Handler
    # Input              :   Dataset Id
    # Output             :   returns the status SUCCESS or FAILURE
    #
    #
    #
    # ##################################################################################################################
    def archive_source_file(self, dataset_id, cluster_id, workflow_id, process_id, batch_id, batch_id_list):
        curr_batch_id = batch_id
        audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        status_message = ""
        log_info = {}
        update_log_file_dtl_flag = True
        file_failed_flag = False
        file_not_processed_flag = False
        try:
            status_message = "Executing File Archiving For Dataset Id: " + dataset_id
            if dataset_id is None or batch_id is None:
                raise Exception('Dataset Id or Batch Id is not provided')
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Get source location from MySQL table for given dataset id
            dataset_info = CommonUtils().get_dataset_information_from_dataset_id(dataset_id)
            file_format = dataset_info['file_format']
            header_available_flag = dataset_info['header_available_flag']
            dataset_archive_location = str(dataset_info['archive_location']).rstrip('/')
            dataset_archive_location = dataset_archive_location.rstrip('/')
            dataset_file_source_location = dataset_info['file_source_location']
            dataset_file_source_location = dataset_file_source_location.rstrip('/')
            status_message = "Completed fetch of source_location for " + str(dataset_id)
            load_files = []
            status_message = status_message + " - " + dataset_archive_location
            logger.info(status_message, extra=self.execution_context.get_context())
            now = datetime.datetime.now().strftime("%Y-%m-%d")
            target_location_till_batch = str(dataset_archive_location) + "/" + str(
                now) + "/" + CommonConstants.PARTITION_BATCH_ID + "=" + str(
                batch_id)
            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, dataset_id)
            # Validate if file details exist for current batch
            if file_details is None:
                status_message = "No details found in file audit table for current batch"
                raise Exception(status_message)

            for file_detail in file_details:
                # Get detail for each file
                file_id = file_detail[CommonConstants.MYSQL_FILE_ID]
                file_url = file_detail[CommonConstants.MYSQL_FILE_NAME]
                load_files.append(file_url)
                log_info = {CommonConstants.BATCH_ID: curr_batch_id, CommonConstants.FILE_ID: file_id,
                            CommonConstants.PROCESS_NAME: PROCESS_NAME,
                            CommonConstants.PROCESS_STATUS: CommonConstants.IN_PROGRESS_DESC,
                            CommonConstants.FILE_NAME: file_url,
                            CommonConstants.DATASET_ID: dataset_id,
                            CommonConstants.CLUSTER_ID: cluster_id,
                            CommonConstants.WORKFLOW_ID: workflow_id,
                            'process_id': process_id
                            }
                # Logic added for restartability
                query_str = "Select file_process_status from {audit_db}.{process_log_table_name} where dataset_id = {dataset_id} and  cluster_id = '{cluster_id}' and workflow_id = '{workflow_id}' and process_id = {process_id} and batch_id={batch_id} and file_id = {file_id} and file_process_name='{archive}'"
                query = query_str.format(audit_db=self.audit_db,
                                         process_log_table_name=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                         dataset_id=dataset_id,
                                         cluster_id=cluster_id,
                                         workflow_id=workflow_id,
                                         process_id=process_id,
                                         batch_id=batch_id,
                                         file_id=file_id,
                                         archive=CommonConstants.FILE_PROCESS_NAME_ARCHIVE)

                result = MySQLConnectionManager().execute_query_mysql(query)
                file_process_status = ""
                if result:
                    file_process_status = str(result[0]['file_process_status'])

                try:
                    if file_process_status == "" or file_process_status == CommonConstants.STATUS_FAILED:
                        file_failed_flag = True
                        if file_process_status == "":
                            file_not_processed_flag = True
                            # Creating process log entry
                            CommonUtils().create_process_log_entry(audit_db, log_info)
                            status_message = "Created process log entry "
                            logger.debug(status_message, extra=self.execution_context.get_context())
                        # Create Source Location
                        source_location = str(file_url)
                        # print(str(source_location))

                        file_name = CommonUtils().get_s3_file_name(file_url)

                        target_location = target_location_till_batch + "/"+CommonConstants.PARTITION_FILE_ID+"=" + str(file_id) + "/" + str(file_name)

                        S3MoveCopyUtility(self.execution_context).copy_s3_to_s3_with_s3cmd(source_location, target_location, None)

                        # Correct file extension before loading to spark dataframe
                        if CommonUtils().is_file_with_invalid_extension(target_location):
                            corrected_filename = CommonUtils().correct_file_extension(target_location, "S3")
                        else:
                            corrected_filename = target_location

                    else:
                        update_log_file_dtl_flag = False
                        status_message = 'File : ' + str(file_url) + ' with Batch ID : ' + str(
                            batch_id) + ' is already processed'
                        logger.debug(status_message, extra=self.execution_context.get_context())
                except Exception as exception:
                    print((str(exception)))
                    status_message = "Archival Failed " + str(file_id) + "due to" + str(exception)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    log_info[CommonConstants.BATCH_ID] = curr_batch_id
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                    log_info['status_id'] = CommonConstants.FAILED_ID
                    log_info['record_count'] = str(0)
                    log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace('"',
                                                                                                                   '')
                    CommonUtils().update_process_log_by_batch(self.audit_db, log_info)
                    batches = batch_id_list.split(",")
                    for batch in batches:
                        log_info[CommonConstants.BATCH_ID] = batch
                        log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                        log_info['status_id'] = CommonConstants.FAILED_ID
                        log_info['record_count'] = str(0)
                        log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'", "").replace(
                            '"', '')

                        CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, log_info)
                        CommonUtils().update_batch_status(self.audit_db, log_info)

                    error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                            " ERROR MESSAGE: " + str(exception) + str(traceback.format_exc())
                    self.execution_context.set_context({"traceback": error})
                    logger.error(error, extra=self.execution_context.get_context())
                    raise exception
            #Update record count
            if file_not_processed_flag or file_failed_flag:
                try:
                    if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                        record_count = PySparkUtility(self.execution_context).get_parquet_record_count(
                            target_location_till_batch)

                    elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                        record_count = PySparkUtility(self.execution_context).get_excel_record_count(
                            target_location_till_batch)

                    else:
                        record_count = PySparkUtility(self.execution_context).get_file_record_count(
                            target_location_till_batch, header_available_flag)
                    log_info = {CommonConstants.BATCH_ID: batch_id,
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
                    CommonUtils(self.execution_context).update_process_log_by_batch(audit_db, log_info)
                except Exception as exception:
                    batches = batch_id_list.split(",")
                    for batch in batches:
                        log_info[CommonConstants.BATCH_ID] = batch
                        log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                        log_info['status_id'] = CommonConstants.FAILED_ID
                        log_info['record_count'] = str(0)
                        log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'",
                                                                                                              "").replace(
                            '"', '')
                        CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, log_info)
                        CommonUtils().update_batch_status(self.audit_db, log_info)
                    log_info[CommonConstants.BATCH_ID] = batch_id
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                    log_info['status_id'] = CommonConstants.FAILED_ID
                    log_info['record_count'] = str(0)
                    log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'",
                                                                                                          "").replace(
                        '"',
                        '')
                    error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                            " ERROR MESSAGE: " + str(traceback.format_exc())
                    self.execution_context.set_context({"traceback": error})
                    logger.error(status_message, extra=self.execution_context.get_context())
                    self.execution_context.set_context({"traceback": ""})
                    self.exc_info = sys.exc_info()
                    raise exception

            status_message = "Archival Completed for dataset id:" + str(dataset_id)
            self.execution_context.set_context({"dataset_archive_location(Source Location)": dataset_archive_location})
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            print((str(exception)))
            batches = batch_id_list.split(",")
            for batch in batches:
                log_info[CommonConstants.BATCH_ID] = batch
                log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                log_info['status_id'] = CommonConstants.FAILED_ID
                log_info['record_count'] = str(0)
                log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'",
                                                                                                      "").replace(
                    '"', '')

                CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, log_info)
                CommonUtils().update_batch_status(self.audit_db, log_info)
            log_info[CommonConstants.BATCH_ID] = batch_id
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
            log_info['status_id'] = CommonConstants.FAILED_ID
            log_info['record_count'] = str(0)
            log_info['message'] = str(exception)[0:int(CommonConstants.MESSAGE_MAX_CHAR)].replace("'",
                                                                                                  "").replace(
                '"',
                '')
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            self.exc_info = sys.exc_info()
            raise exception

    def main(self, dataset_id, cluster_id, workflow_id, process_id, batch_id, batch_id_list):
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for Source File Archival"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.archive_source_file(dataset_id, cluster_id, workflow_id, process_id, batch_id,
                                                         batch_id_list)
            # Exception is raised if the program returns failure as execution status
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function for Source File Archival"
            logger.info(status_message, extra=self.execution_context.get_context())
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception


if __name__ == '__main__':
    dataset_id = sys.argv[1]
    batch_id = sys.argv[5]
    cluster_id = sys.argv[2]
    workflow_id = sys.argv[3]
    process_id = sys.argv[4]
    batch_id_list = sys.argv[6]
    if dataset_id is None or cluster_id is None or workflow_id is None or process_id is None or batch_id is None or batch_id_list is None:
        raise Exception(CommonConstants.PROVIDE_ALL_ARGUMENTS)
    batch_id = str(batch_id).rstrip("\n\r")
    archive_source_file = ArchiveFileHandler(dataset_id, cluster_id, workflow_id)
    result_dict = archive_source_file.main(dataset_id, cluster_id, workflow_id, process_id, batch_id, batch_id_list)
    STATUS_MSG = "\nCompleted execution for Archival Utility with status " + json.dumps(result_dict) + "\n"
    sys.stdout.write(STATUS_MSG)
