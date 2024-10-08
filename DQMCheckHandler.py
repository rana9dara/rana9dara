#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   DQMHandler
#  Purpose             :   This module will perform the pre-configured steps and read DQM configs before invoking
#                          DQMCheckUtility.py.
#  Input Parameters    :   data_source, subject_area
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   6th June 2018
#  Last changed by     :   Neelakshi Kulkarni
#  Reason for change   :   Enhancement to provide restartability at file level
# ######################################################################################################################


# Library and external modules declaration
import json
import traceback
import sys
import os
import datetime
import time
sys.path.insert(0, os.getcwd())
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from DQMCheckUtility import DQMCheckUtility
from MySQLConnectionManager import MySQLConnectionManager
from PySparkUtility import PySparkUtility
from ConfigUtility import JsonConfigUtility
# all module level constants are defined here
MODULE_NAME = "DQMCheckHandler"
PROCESS_NAME = "DQM Check"

USAGE_STRING = """
SYNOPSIS
    python DqmCheckHandler.py <dataset_id> <batch_id> <cluster_id> <workflow_id>

    Where
        input parameters : dataset_id , batch_id, cluster_id, workflow_id

"""


class DqmCheckHandler:
    # Default constructor
    def __init__(self, DATASET_ID, CLUSTER_ID, WORKFLOW_ID):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"dataset_id": DATASET_ID})
        self.execution_context.set_context({"cluster_id": CLUSTER_ID})
        self.execution_context.set_context({"workflow_id": WORKFLOW_ID})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # ########################################### execute_dqm_check ####################################################
    # Purpose            :   Executing the DQM check
    # Input              :   dataset_id, batch id
    # Output             :   NA
    # ##################################################################################################################
    def execute_dqm_check(self, dataset_id, cluster_id, workflow_id, process_id, batch_id,batch_id_list):
        curr_batch_id = batch_id
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        status_message = ""
        no_dqm_flag = False
        log_info = {}
        update_log_file_dtl_flag = True
        spark_context = None
        try:
            module_name_for_spark = str(
                MODULE_NAME + "_dataset_" + dataset_id + "_process_" + process_id)
            spark_context = PySparkUtility(self.execution_context).get_spark_context(module_name_for_spark,CommonConstants.HADOOP_CONF_PROPERTY_DICT)
            status_message = "Starting DQM Check function of module DQMCheckHandler"
            logger.info(status_message, extra=self.execution_context.get_context())
            # Input Validations
            if dataset_id is None:
                raise Exception('Dataset Id is not provided')
            if batch_id is None:
                raise Exception("Batch ID for current batch is invalid")
            self.execution_context.set_context({"dataset_id": dataset_id, "batch_id": batch_id})
            logger.info(status_message, extra=self.execution_context.get_context())
            log_info = {CommonConstants.BATCH_ID: batch_id,
                        CommonConstants.PROCESS_NAME: PROCESS_NAME,
                        CommonConstants.DATASET_ID: dataset_id,
                        CommonConstants.CLUSTER_ID: cluster_id,
                        CommonConstants.WORKFLOW_ID: workflow_id,
                        'process_id': process_id
                        }
            # Get landing location and data_set_id from MySQL table for given data source and subject area
            data_set_info = CommonUtils().get_dataset_information_from_dataset_id(dataset_id)
            landing_location = data_set_info['s3_pre_dqm_location']
            landing_location = str(landing_location).rstrip('/')
            row_id_exclusion_flag = str(data_set_info['row_id_exclusion_flag'])
            file_field_delimeter = str(data_set_info['file_field_delimiter'])
            # Get file details of current batch
            file_details = CommonUtils().get_file_details_for_current_batch(batch_id, dataset_id)
            # Validate if file details exist for current batch
            if file_details is None:
                status_message = "No details found in file audit table for current batch"
                raise Exception(status_message)
            # Fetch dqm metadata from mysql table for provided datase
            fetch_dqm_metadata_query = "SELECT * from " + self.audit_db + "." + \
                                       CommonConstants.DQM_METADATA_TABLE + " where " + CommonConstants.MYSQL_DATASET_ID + "=" + \
                                       str(dataset_id) + " and " + CommonConstants.MYSQL_ACTIVE_IND + "='" + CommonConstants.ACTIVE_IND_VALUE + "'"
            status_message = "Query for fetching DQM metadata"
            logger.info(status_message, extra=self.execution_context.get_context())

            dqm_config_checks = MySQLConnectionManager().execute_query_mysql(fetch_dqm_metadata_query, False)
            status_message = "DQM Checks fetched : ",dqm_config_checks
            logger.info(status_message, extra=self.execution_context.get_context())
            # To check whether dqm config checks are empty
            if len(dqm_config_checks) == 0:
                no_dqm_flag = True
            else:
                if landing_location == 'None':
                    landing_location = data_set_info['s3_landing_location']
                    landing_location = str(landing_location).rstrip('/')
                    if str(landing_location) == 'None':
                        status_message = "Landing Location is not configured.  Hence failing the process"
                        raise Exception(status_message)

                else:
                    landing_location = data_set_info['s3_pre_dqm_location'].rstrip('/')


            # Loop through each file and invoke DQM Check Utility
            file_not_processed_flag = False
            file_failed_flag = False
            for file_detail in file_details:
                # Get detail for each file
                file_id = file_detail[CommonConstants.MYSQL_FILE_ID]
                file_name = file_detail[CommonConstants.MYSQL_FILE_NAME]
                # Logic added for restartability
                log_info = {CommonConstants.BATCH_ID: batch_id, CommonConstants.FILE_ID: file_id, \
                            CommonConstants.PROCESS_NAME: PROCESS_NAME, \
                            CommonConstants.PROCESS_STATUS: CommonConstants.IN_PROGRESS_DESC, \
                            CommonConstants.FILE_NAME: file_name,
                            CommonConstants.DATASET_ID: dataset_id,
                            CommonConstants.CLUSTER_ID: cluster_id,
                            CommonConstants.WORKFLOW_ID: workflow_id,
                            'process_id': process_id
                            }
                query_str = "Select file_process_status from {audit_db}.{process_log_table_name} where dataset_id = {dataset_id} and  cluster_id = '{cluster_id}' and workflow_id = '{workflow_id}' and process_id = {process_id} and batch_id={batch_id} and file_id = {file_id} and file_process_name='{dqm_check}'"
                query = query_str.format(audit_db=self.audit_db,
                                         process_log_table_name=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                         dataset_id=dataset_id,
                                         cluster_id=cluster_id,
                                         workflow_id=workflow_id,
                                         process_id=process_id,
                                         batch_id=batch_id,
                                         file_id=file_id,
                                         dqm_check=CommonConstants.FILE_PROCESS_NAME_DQM)
                result = MySQLConnectionManager().execute_query_mysql(query)
                file_process_status = ""
                if result:
                    file_process_status = str(result[0]['file_process_status'])
                if file_process_status == "" or file_process_status == CommonConstants.STATUS_FAILED:
                    if file_process_status == "":
                        file_not_processed_flag = True
                        # Creating process log entry
                        CommonUtils().create_process_log_entry(self.audit_db, log_info)
                    if file_process_status == CommonConstants.STATUS_FAILED:
                        file_failed_flag = True
            try:
                if file_not_processed_flag or file_failed_flag:
                    if no_dqm_flag == False:
                        status_message = "Created process log entry "
                        logger.debug(status_message, extra=self.execution_context.get_context())
                        if row_id_exclusion_flag == CommonConstants.ROW_ID_EXCLUSION_IND_YES:
                            status_message = "ROW ID Exclusion flag is set however checks are " \
                                             "configured for dataset id:" + str(dataset_id) + " Hence skipping DQM"
                            logger.info(status_message, extra=self.execution_context.get_context())
                            raise Exception
                        else:
                            # Get the complete file url
                            file_complete_url = landing_location + "/"+CommonConstants.PARTITION_BATCH_ID+"=" + str(batch_id)
                            self.execution_context.set_context({'file_complete_path': file_complete_url})
                            status_message = 'Fetched complete file url:' + file_complete_url
                            logger.info(status_message, extra=self.execution_context.get_context())
                            # Invoke DQMChecker Utility
                            DQMCheckUtility().perform_dqm_check(file_complete_url, dqm_config_checks, batch_id,
                                                                spark_context, file_field_delimeter)
                            # Update to SUCCESS
                            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
                            log_info['message'] = "DQM Completed successfully"
                            # Update process log entry to SUCCESS
                            log_info[CommonConstants.BATCH_ID] = batch_id
                            log_info['status_id'] = CommonConstants.SUCCESS_ID
                    else:
                        status_message = "No DQM checks configured for dataset id:" + str(dataset_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        raise Exception
                else:
                    update_log_file_dtl_flag = False
                    status_message = 'Files with Batch ID : ' + str(
                        batch_id) + ' are already processed'
                    logger.debug(status_message, extra=self.execution_context.get_context())
            except Exception as exception:
                status_message = "DQM failed due to" + str(exception)
                logger.error(status_message, extra=self.execution_context.get_context())
                if no_dqm_flag == True:
                    log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SKIPPED
                    log_info[CommonConstants.PROCESS_MESSAGE_KEY] = "No DQM Configured, Hence Skipping DQM."
                    log_info[CommonConstants.BATCH_ID] = batch_id
                    log_info["end_time"] = datetime.datetime.fromtimestamp(time.time()).strftime(
                        '%Y-%m-%d %H:%M:%S')
                    log_info['status_id'] = CommonConstants.FAILED_ID
                else:
                    if row_id_exclusion_flag == CommonConstants.ROW_ID_EXCLUSION_IND_YES:
                        log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SKIPPED
                        log_info[CommonConstants.PROCESS_MESSAGE_KEY] = "ROW ID Exclusion flag is set however " \
                                                                       "DQM checks configured. Hence Skipping DQM"
                        log_info[CommonConstants.BATCH_ID] = batch_id
                        log_info["end_time"] = datetime.datetime.fromtimestamp(time.time()).strftime(
                            '%Y-%m-%d %H:%M:%S')
                        log_info['status_id'] = CommonConstants.FAILED_ID
                    else:
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
            finally:
                if update_log_file_dtl_flag:
                    # updating process log entry
                    CommonUtils().update_process_log_by_batch(self.audit_db, log_info)
                    status_message = "Updated process log entry for all files in batch id" + str(batch_id)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                else:
                    status_message = "File was processed in a previous run, hence process log was not updated"
                    logger.info(status_message, extra=self.execution_context.get_context())

            status_message = "Completed DQM check of landing files for dataset id:" + str(dataset_id)
            self.execution_context.set_context({"landing location": landing_location})
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            log_info[CommonConstants.BATCH_ID] = batch_id
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
                    '"',
                    '')
                CommonUtils().update_file_audit_status_by_batch_id(self.audit_db, log_info)
                CommonUtils().update_batch_status(self.audit_db, log_info)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                 CommonConstants.ERROR_KEY: str(exception)}
            raise exception
        finally:
            status_message = "In Finally Block Of DQMCheck Handler"
            logger.info(status_message, extra=self.execution_context.get_context())
            if spark_context:
                spark_context.stop()
            status_message = "Spark Context Closed In DQM Check Handler Finally Block"
            logger.info(status_message, extra=self.execution_context.get_context())

    # ############################################# Main ###############################################################
    # Purpose   : Handles the process of executing DQM queries and returning the status
    #             and records (if any)
    # Input     : Requires dataset_id,batch id
    # Output    : Returns execution status and records (if any)
    # ##################################################################################################################

    def main(self, dataset_id, cluster_id, workflow_id, process_id, batch_id,batch_id_list):
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for DQMCheckHandlerUtility"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.execute_dqm_check(dataset_id, cluster_id, workflow_id, process_id, batch_id,batch_id_list)
            # Exception is raised if the program returns failure as execution status
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function for DQMCheckHandlerUtility"
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
    DATASET_ID = sys.argv[1]
    BATCH_ID = sys.argv[5]
    CLUSTER_ID = sys.argv[2]
    WORKFLOW_ID = sys.argv[3]
    PROCESS_ID = sys.argv[4]
    BATCH_ID_LIST = sys.argv[6]
    if DATASET_ID is None or CLUSTER_ID is None or WORKFLOW_ID is None or PROCESS_ID is None or BATCH_ID is None or BATCH_ID_LIST is None:
        raise Exception(CommonConstants.PROVIDE_ALL_ARGUMENTS)
    BATCH_ID = str(BATCH_ID).rstrip("\n\r")
    DQM_CHECK_HANDLER = DqmCheckHandler(DATASET_ID, CLUSTER_ID, WORKFLOW_ID)
    RESULT_DICT = DQM_CHECK_HANDLER.main(DATASET_ID, CLUSTER_ID, WORKFLOW_ID, PROCESS_ID, BATCH_ID,BATCH_ID_LIST)
    STATUS_MSG = "\nCompleted execution for DQM Utility with status " + json.dumps(RESULT_DICT) + "\n"
    sys.stdout.write(STATUS_MSG)
    sys.stdout.write("batch_id=" + str(BATCH_ID))

