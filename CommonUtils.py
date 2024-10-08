#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

######################################################Module Information################################################
#   Module Name         :   CommonUtils
#   Purpose             :   Common util functions used by all utility modules
#   Input Parameters    :   NA
#   Output              :   NA
#   Execution Steps     :   Instantiate and object of this class and call the class functions
#   Predecessor module  :   This module is  a generic module
#   Successor module    :   NA
#   Last changed on     :   20 November 2017
#   Last changed by     :   Amal Kurup
#   Reason for change   :   Adding new common utility module for Common Components Project
########################################################################################################################

from datetime import datetime
import re
import traceback
import random
import json
import getpass
import boto
import subprocess
import os
import time
import sys
#os.environ["HADOOP_HOME"] = "/usr/lib/hadoop"
#import pydoop.hdfs
#import pydoop.hdfs.path as hpath
from pyhive import hive
from awscli.clidriver import create_clidriver
import requests

sys.path.insert(0, os.getcwd())
from Pattern_Validator import PatternValidator
from ExecutionContext import ExecutionContext
from LogSetup import logger
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager
import CommonConstants as CommonConstants


import math

# all module level constants are defined here
MODULE_NAME = "CommonUtils"


class CommonUtils(object):
    # Initializer method of the Common Functions
    def __init__(self, execution_context=None):

        if execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

        self.configuration = JsonConfigUtility(
            CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.hive_port = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "hive_port"])
        self.private_key_location = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
        self.cluster_mode = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "cluster_mode"])

    ########################################## get_hdfs_files_list #################################################
    # Purpose   :   This method is used to get the list of files present in the HDFS source folder
    # Input     :   The base path of the folder, current path for traversal, list of files with relative paths and the
    #                 other with absolute paths
    # Output    :   Return True if the traversal was successful else return False
    # ##################################################################################################################
    #def get_hdfs_files_list(self, hdfs_path):
        #status_message = ""
        #absolute_file_list = []
        #try:
            #status_message = "Starting function to fetch HDFS files list."
            #logger.debug(status_message, extra=self.execution_context.get_context())
            #if hpath.isdir(hdfs_path):
                #curr_files_list = pydoop.hdfs.ls(hdfs_path)
                #for file_name in curr_files_list:
                   # if hpath.isdir(file_name):
                        #self.get_hdfs_files_list(file_name)
                    #else:
                       #absolute_file_list.append(file_name)
            #else:
                #absolute_file_list.append(hdfs_path)
            #return absolute_file_list

        #except KeyboardInterrupt:
            #raise KeyboardInterrupt

        #except Exception as e:
            #error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    #" ERROR MESSAGE: " + str(traceback.format_exc())
            #self.execution_context.set_context({"traceback": error})
            #logger.error(status_message, extra=self.execution_context.get_context())
            #self.execution_context.set_context({"traceback": ""})
            #raise e

    def get_user_name(self):
        """
            Purpose   :   This method is used to get logged in unix user name
            Input     :   NA
            Output    :   user name
        """
        command = 'who am i | cut -d " " -f 1'
        user_output_command = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        command_output, command_error = user_output_command.communicate()
        user_name = command_output.strip().decode("utf-8")
        logger.debug(user_name, extra=self.execution_context.get_context())
        return user_name

    ########################################## clean hdfs directory #################################################
    # Purpose   :   This method is used to get the list of files present in the HDFS source folder
    # Input     :   hdfs directory path
    # Output    :   NA
    # ##################################################################################################################
    def clean_hdfs_directory(self, hdfs_dir=None):
        status_message = ""
        absolute_file_list = []
        try:
            status_message = "Starting function to clean HDFS directory."
            self.execution_context.set_context({"HDFS_DIR": hdfs_dir})
            logger.debug(status_message, extra=self.execution_context.get_context())
            #if hpath.exists(hdfs_dir) and pydoop.hdfs.ls(hdfs_dir):
                #pydoop.hdfs.rmr(hdfs_dir + '/*')
            status_message = "Completed function to clean HDFS directory."
            logger.debug(status_message, extra=self.execution_context.get_context())
            return True

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_header(self, dataset_id):
        status_message = "Starting to prepare query for fetching column names for dataset id:" + str(dataset_id)
        logger.info(status_message, extra=self.execution_context.get_context())
        fetch_columns_query = "Select " + CommonConstants.COLUMN_NAME + " from " + self.audit_db + "." + CommonConstants.COLUMN_METADATA_TABLE + " where dataset_id='" + str(
            dataset_id) + "' order by " + CommonConstants.COLUMN_SEQUENCE_NUMBER
        column_name_resultset = MySQLConnectionManager().execute_query_mysql(fetch_columns_query, False)
        status_message = "Columns fetched", column_name_resultset
        logger.info(status_message, extra=self.execution_context.get_context())
        column_names_list = []
        for column_result in column_name_resultset:
            column_names_list.append(column_result['column_name'])
        logger.info(status_message, extra=self.execution_context.get_context())
        return column_names_list

    def get_file_name_from_file_id (self, file_id = None):
        """
        Purpose   :   This method is used to fetch the file name from batch_id

        Input   :   batch_id
        Output  :   file_name for that batch_id
        """

        try:
            status_message = "Starting method to fetch file name from file id"
            logger.info(status_message,extra = self.execution_context.get_context())

            file_name_query = "select distinct {file_name} from {audit_db}.{log_file_dtl} where file_id = {file_id}" \
            .format(file_name = "file_name", audit_db = self.audit_db,
                    log_file_dtl = CommonConstants.PROCESS_LOG_TABLE_NAME,file_id=file_id)
            file_name = MySQLConnectionManager().execute_query_mysql(file_name_query, True)
            status_message = "Result of  file name fetched from the log_file_dtl table is : " \
                             + json.dumps(file_name)
            logger.info(status_message, extra=self.execution_context.get_context())
            file_name = file_name['file_name']
            return file_name

        except Exception as e:
            status_message = "Error occured while getting file name:"
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e

    def get_column_information(self,dataset_id):
        """
        Purpose   :   This method is used to fetch the max column_id and max column_sequence_number (for  dataset_id _
                      from the ctl_column_metadata table
        Input     :   Dataset_id
        Output    :   column_id and column_sequence_number
       """

        try:
            status_message = "Starting method to fetch the column_id and the column_sequence_information from " \
                             "ctl_column_metadata column"
            logger.info(status_message,extra = self.execution_context.get_context())

            column_id_query = "select max({column_id}) from {audit_db}.{ctl_column_metadata} " \
            .format(column_id = "column_id", audit_db = self.audit_db,
                    ctl_column_metadata = CommonConstants.COLUMN_METADATA_TABLE)
            max_column_id = MySQLConnectionManager().execute_query_mysql(column_id_query, True)
            status_message = "result of  max column_id fetched from the column metadata table is : " \
                             + json.dumps(max_column_id)
            logger.info(status_message, extra=self.execution_context.get_context())

            column_sequence_query = "select max({column_sequence_number}) from {audit_db}.{ctl_column_metadata} " \
                                    "where dataset_id = {dataset_id}".\
                format(column_sequence_number=CommonConstants.COLUMN_SEQUENCE_NUMBER,
                       audit_db=CommonConstants.AUDIT_DB_NAME, ctl_column_metadata=CommonConstants.COLUMN_METADATA_TABLE,
                       dataset_id=str(dataset_id))
            max_column_sequence_number = MySQLConnectionManager().execute_query_mysql(column_sequence_query, True)
            status_message = "result of  max column_id fetched from the column metadata table is : " \
                             + json.dumps(max_column_sequence_number)
            logger.info(status_message, extra=self.execution_context.get_context())
            max_column_sequence_number = max_column_sequence_number['max(column_sequence_number)']
            max_column_id = max_column_id['max(column_id)']
            return max_column_id , max_column_sequence_number

        except Exception as e:
            status_message = "Error occured during the execution of Variable File Format Ingestion:"
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise e

    # ############################################### get_sequence #################################################
    # Purpose   :   This method is used to get the next value from an already created sequence in the automation log
    #               database
    # Input     :   Sequence name, Encryption enabled flag (Optional), Automation logging configuration(Optional) &
    #               Oracle connection object (Optional)
    # Output    :   The next value of the sequence
    # ##############################################################################################################
    def get_sequence(self, sequence_name):
        status_message = ""
        my_sql_utility_conn = MySQLConnectionManager()
        try:
            status_message = "Starting function to get sequence"
            logger.info(status_message, extra=self.execution_context.get_context())
            # Prepare the my-sql query to get the sequence
            query_string = "SELECT " + sequence_name + ".NEXTVAL FROM dual"
            sequence_id = my_sql_utility_conn.execute_query_mysql(query_string)
            status_message = "Sequence generation result: " + sequence_id
            logger.debug(status_message, extra=self.execution_context.get_context())
            return sequence_id
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    ################################## Get s3 details for current user #############################################
    # Purpose   :   This method is used to create a log entry for a newly started load in automation log database
    # Input     :
    # Output    :   S3 connection dictionary
    # ##############################################################################################################
    def fetch_s3_conn_details_for_user(self):
        status_message = ""
        try:
            status_message = 'Started fetch of s3 connection details for current user'
            current_user = getpass.getuser()
            self.execution_context.set_context({"current_user": current_user})
            s3_cred_file_name = self.replace_variables(CommonConstants.S3_CREDENTIAL_FILE_NAME,
                                                       {"current_user": current_user})
            logger.info(s3_cred_file_name, extra=self.execution_context.get_context())
            s3_cred_params = JsonConfigUtility(conf_file=s3_cred_file_name)
            status_message = "Fetched S3 details for user " + current_user
            logger.info(status_message, extra=self.execution_context.get_context())
            return s3_cred_params
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    ################################## create process log_entry ########################################################
    # Purpose   :   This method is used to create a log entry for a newly started load in automation log database
    # Input     :   Automation logging database name , Status of the load, Log info containing data source name,
    #               load step, Run Id for the current load and environment
    # Output    :   True for success, False for failure
    # ##################################################################################################################
    def create_process_log_entry(self, automation_log_database, log_info):
        status_message = ""
        try:
            status_message = "Starting function to create process log for input : " + str(log_info)
            logger.info(status_message, extra=self.execution_context.get_context())
            batch_id = log_info[CommonConstants.BATCH_ID]
            file_id = log_info[CommonConstants.FILE_ID]
            file_name = str("'") + log_info[CommonConstants.FILE_NAME] + str("'")
            process_name = str("'") + log_info[CommonConstants.PROCESS_NAME] + str("'")
            input_process_status = log_info.get(CommonConstants.PROCESS_STATUS, None)
            if input_process_status is not None:
                process_status = str("'") + input_process_status + str("'")
            else:
                process_status = str("'") + CommonConstants.IN_PROGRESS_DESC + str("'")
            proc_table = CommonConstants.PROCESS_LOG_TABLE_NAME
            audit_db = automation_log_database
            dataset_id = log_info[CommonConstants.DATASET_ID]
            cluster_id = str("'") + log_info[CommonConstants.CLUSTER_ID] + str("'")
            workflow_id = str("'") + log_info[CommonConstants.WORKFLOW_ID] + str("'")
            process_id = log_info['process_id']
            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration
            query_string = "INSERT INTO " + audit_db + "." + proc_table + " (batch_id, file_id, file_name, " \
                                                                          "file_process_name, file_process_status, file_process_start_time, file_process_end_time, cluster_id, workflow_id, dataset_id, process_id) VALUES ({batch_id}, " \
                                                                          "{file_id}, {file_name}, {process_name}, {process_status}, {start_time}, {end_time}, {cluster_id}, {workflow_id}, {dataset_id}, {process_id}) ".format(
                batch_id=batch_id, file_id=file_id, file_name=file_name, process_name=process_name,
                process_status=process_status, start_time="NOW() ", end_time="NULL", cluster_id=cluster_id,
                workflow_id=workflow_id, dataset_id=dataset_id, process_id=process_id)
            status_message = "Input query for creating load automation log : " + query_string
            logger.info(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager().execute_query_mysql(query_string)
            status_message = "Load automation log creation result : " + json.dumps(result)
            logger.info(status_message, extra=self.execution_context.get_context())
            status_message = "Completing function to create load automation log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def check_if_processed_batch_entry(self, dataset_id=None, batch_id=None, cluster_id=None, process_id=None):
        status_message = ""
        status_skipped_flag = False
        try:
            automation_log_database = self.configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
            status_message = "Starting function to check if batch status is skipped for dataset_id : " + str(
                dataset_id) + " ,process id : " + str(process_id)

            logger.debug(status_message, extra=self.execution_context.get_context())

            query_string = "SELECT batch_status from {audit_db}.{batch_details_table} where batch_id = '{batch_id}' " \
                           " and process_id={process_id} and cluster_id = '{cluster_id}'"
            query_format = query_string.format(audit_db=automation_log_database,
                                               batch_details_table=CommonConstants.BATCH_TABLE,
                                               batch_id=batch_id,
                                               dataset_id=str(dataset_id).strip(),
                                               process_id=str(process_id).strip(),
                                               cluster_id=cluster_id.strip()
                                               )
            output_status = MySQLConnectionManager().execute_query_mysql(query_format)
            for status in output_status:
                if (status['batch_status'] == CommonConstants.SKIPPED_DESC):
                    status_skipped_flag = True

            return status_skipped_flag
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e



    def create_file_audit_entry(self, automation_log_database, file_audit_info):
        status_message = ""
        try:
            status_message = "Starting function to create file log for input : " + str(file_audit_info)
            logger.info(status_message, extra=self.execution_context.get_context())

            batch_id = file_audit_info[CommonConstants.BATCH_ID]
            file_name = str("'") + file_audit_info[CommonConstants.FILE_NAME] + str("'")
            file_status = str("'") + file_audit_info[CommonConstants.FILE_STATUS] + str("'")
            dataset_id = file_audit_info[CommonConstants.DATASET_ID]
            cluster_id = file_audit_info[CommonConstants.CLUSTER_ID]
            workflow_id = file_audit_info[CommonConstants.WORKFLOW_ID]
            process_id = file_audit_info['process_id']

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration
            query_string = "INSERT INTO {audit_db}.{file_audit_table} (file_name, dataset_id, " \
                           "file_status, batch_id, " \
                           "file_process_start_time, cluster_id, workflow_id, process_id) VALUES ({file_name}, " \
                           "{dataset_id}, " \
                           "{status_desc}, {batch_id}, {update_time}, '{cluster_id}', " \
                           "'{workflow_id}', {process_id})".format(
                audit_db=automation_log_database,
                file_audit_table=CommonConstants.FILE_AUDIT_TABLE,
                batch_id=batch_id, file_name=file_name, dataset_id=dataset_id,
                status_desc="'" + CommonConstants.IN_PROGRESS_DESC + "'",
                update_time="NOW()", cluster_id=cluster_id,
                workflow_id=workflow_id, process_id=process_id)
            status_message = "Input query for creating file audit log : " + query_string
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager(self.execution_context).execute_query_mysql(query_string)
            status_message = "File automation log creation result : " + json.dumps(result)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Completing function to create file automation log"
            logger.info(status_message, extra=self.execution_context.get_context())
            return result

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def create_batch_entry(self, automation_log_database, dataset_id=None, cluster_id=None, workflow_id=None,
                           process_id=None):
        status_message = ""
        try:
            status_message = "Starting function to create batch entry for dataset_id : " + str(
                dataset_id) + " ,process id : " + str(process_id) + " ,cluster_id : " + str(cluster_id)

            logger.debug(status_message, extra=self.execution_context.get_context())

            batch_status_id = CommonConstants.IN_PROGRESS_ID
            batch_status_desc = str("'") + CommonConstants.IN_PROGRESS_DESC + str("'")

            cluster_id = str("'") + str(cluster_id) + str("'")
            workflow_id = str("'") + str(workflow_id) + str("'")

            retry_count = 0
            # Check Flag is 0 when retries have to carry on. When batch ID is found unique check flag is mad 1
            check_flag = 0
            max_retry_limit = CommonConstants.MAX_RETRY_LIMIT
            while retry_count < max_retry_limit and check_flag == 0:
                batch_id = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
                batch_id = batch_id[:-1]
                status_message = "Batch ID generated and yet to check if there exists duplicate entry"
                logger.info(status_message, extra=self.execution_context.get_context())
                query_string = "INSERT INTO {audit_db}.{batch_details_table} (batch_id, dataset_id," \
                               "batch_status, batch_start_time, cluster_id, workflow_id, process_id)" \
                               "VALUES ({batch_id},{dataset_id}, {batch_status_desc}, {start_dt}," \
                               "{cluster_id}, {workflow_id}, {process_id})"
                check_flag = 1
                try:
                    query_format = query_string.format(
                        audit_db=automation_log_database,
                        batch_details_table=CommonConstants.BATCH_TABLE,
                        batch_status_id=batch_status_id, batch_status_desc=batch_status_desc,
                        start_dt="NOW()",
                        dataset_id=dataset_id, cluster_id=cluster_id, workflow_id=workflow_id, process_id=process_id,
                        batch_id=batch_id)
                    MySQLConnectionManager().execute_query_mysql(query_format)
                except:
                    logger.warn("Duplicate batch ID detected, regenerating new batch ID", extra=self.execution_context.get_context())
                    random_int = random.randint(1, 10)
                    time.sleep(random_int)
                    retry_count += 1
                    check_flag = 0
                    pass

            if check_flag == 0 and retry_count == max_retry_limit:
                raise Exception("Duplicate Batch IDs found and Maximum retry limit reached")

            return batch_id
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    ################################## Inser logging details in predqm log table #############################################
    # Purpose   :   This method is used to insert a log entry for a newly started load in pre dqm logging table
    # Input     :
    # Output    :
    # ##############################################################################################################

    def insert_pre_dqm_status(self, log_info):
        status_message = ""
        try:
            status_message = "Starting function to create process log"
            logger.debug(status_message, extra=self.execution_context.get_context())
            audit_db = self.audit_db
            process_name = str("'") + log_info[CommonConstants.PROCESS_NAME] + str("'")
            dataset_id = log_info[CommonConstants.DATASET_ID]
            workflow_id = str("'") + log_info[CommonConstants.WORKFLOW_ID] + str("'")
            batch_id = log_info[CommonConstants.BATCH_ID]
            function_name = log_info['function_name']
            function_params = log_info['function_params']
            process_id = log_info['process_id']
            workflow_name = log_info['workflow_name']
            c_name = log_info['c_name']

            file_id_result = self.get_file_ids_by_batch_id(audit_db, batch_id)
            for i in range(0, len(file_id_result)):
                file_id = file_id_result[i]['file_id']
                status_message = "Inserting the status for the pre-dqm checks for file id : ", file_id
                logger.debug(status_message, extra=self.execution_context.get_context())
                query = "insert into {audit_db}.{log_pre_dqm_dtl} (process_id, process_name, workflow_id, workflow_name, dataset_id, file_id, batch_id, column_name,function_name, function_params, status, updated_timestamp) " \
                        "values({process_id},{process_name},{workflow_id},'{workflow_name}','{dataset_id}','{file_id}','{batch_id}','{c_name}','{function_name}','{function_params}','{inprogress}',now())" \
                    .format(audit_db=audit_db, log_pre_dqm_dtl=CommonConstants.LOG_PRE_DQM_DTL,
                            process_name=process_name, process_id=process_id, workflow_name=workflow_name,
                            c_name=c_name,
                            function_name=function_name, function_params=function_params,
                            inprogress=CommonConstants.STATUS_RUNNING,
                            workflow_id=workflow_id, dataset_id=dataset_id, batch_id=batch_id, file_id=file_id)
                result = MySQLConnectionManager().execute_query_mysql(query)
                status_message = "Load Pre DQM log creation result : " + json.dumps(result)
                logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "Completing function to create load automation log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_pre_dqm_success_status(self, log_info):
        status_message = ""
        try:
            status_message = "Starting function to create process log"
            logger.debug(status_message, extra=self.execution_context.get_context())
            dataset_id = log_info[CommonConstants.DATASET_ID]
            batch_id = log_info[CommonConstants.BATCH_ID]
            c_name = log_info['c_name']
            audit_db = self.audit_db
            file_id_result = self.get_file_ids_by_batch_id(audit_db, batch_id)
            for i in range(0, len(file_id_result)):
                file_id = file_id_result[i]['file_id']
                status_message = "Updating Success status for the pre-dqm checks for file id : ", file_id
                logger.debug(status_message, extra=self.execution_context.get_context())
                query_update = (
                    "update {audit_db}.{log_pre_dqm_dtl} set status = '{success}' where column_name = '{c_name}' and dataset_id = '{dataset_id}' and file_id = {file_id} and batch_id = {batch_id}").format(
                    audit_db=audit_db, success=CommonConstants.STATUS_SUCCEEDED,
                    log_pre_dqm_dtl=CommonConstants.LOG_PRE_DQM_DTL,
                    c_name=c_name, dataset_id=dataset_id, batch_id=batch_id, file_id=file_id)

                result = MySQLConnectionManager().execute_query_mysql(query_update)
                status_message = "Load Pre DQM log creation result : " + json.dumps(result)
                logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "Completing function to create load automation log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_pre_dqm_failure_status(self, log_info):
        status_message = ""
        try:
            status_message = "Starting function to create process log"
            logger.debug(status_message, extra=self.execution_context.get_context())
            dataset_id = log_info[CommonConstants.DATASET_ID]
            batch_id = log_info[CommonConstants.BATCH_ID]
            c_name = log_info['c_name']

            audit_db = self.audit_db
            file_id_result = self.get_file_ids_by_batch_id(audit_db, batch_id)
            for i in range(0, len(file_id_result)):
                file_id = file_id_result[i]['file_id']
                status_message = "Updating Failure status for the pre-dqm checks for filr id : ", file_id
                logger.debug(status_message, extra=self.execution_context.get_context())
                query_update_failure = (
                    "update {audit_db}.{log_pre_dqm_dtl} set status = '{failed}' where column_name = '{c_name}' and dataset_id = '{dataset_id}' and file_id = {file_id} and batch_id = {batch_id}").format(
                    audit_db=audit_db, failed=CommonConstants.STATUS_FAILED,
                    log_pre_dqm_dtl=CommonConstants.LOG_PRE_DQM_DTL,
                    c_name=c_name, dataset_id=dataset_id, batch_id=batch_id, file_id=file_id)
                result = MySQLConnectionManager().execute_query_mysql(query_update_failure)
                status_message = "Load Pre DQM log creation result : " + json.dumps(result)
                logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "Completing function to create load automation log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_batch_status(self, automation_log_database=None, log_info=None):
        status_message = ""
        try:
            status_message = "Starting function to update batch status for input : " + str(log_info)
            logger.info(status_message, extra=self.execution_context.get_context())

            batch_id = log_info[CommonConstants.BATCH_ID]
            status = str("'") + log_info[CommonConstants.PROCESS_STATUS] + str("'")
            status_id = log_info['status_id']

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration
            query_string = "UPDATE {automation_db_name}.{batch_table} set  " \
                           "batch_status = {status_desc}, batch_end_time = {end_time} " \
                           "where batch_id = {batch_id} "
            query = query_string.format(automation_db_name=automation_log_database,
                                        batch_table=CommonConstants.BATCH_TABLE,
                                        batch_status_id=status_id, batch_id=batch_id, status_desc=status,
                                        end_time="NOW() ")
            status_message = "Input query for updating batch table entry : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager(self.execution_context).execute_query_mysql(query)
            status_message = "Update Load automation result : " + json.dumps(result)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Completed function to update batch entry"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def update_process_status(self, automation_log_database=None, log_info=None):
        status_message = ""
        try:
            status_message = "Starting function to update process log status with input : " + str(log_info)
            logger.debug(status_message, extra=self.execution_context.get_context())

            batch_id = log_info[CommonConstants.BATCH_ID]
            file_id = log_info[CommonConstants.FILE_ID]
            process_name = str("'") + log_info[CommonConstants.PROCESS_NAME] + str("'")
            process_status = str("'") + log_info[CommonConstants.PROCESS_STATUS] + str("'")

            record_count = str(0)
            message = str("'") + str("NA") + str("'")
            if 'record_count' in log_info:
                record_count = str(log_info['record_count'])

            if 'message' in log_info:
                message = str("'") + str(log_info['message']) + str("'")

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration
            query_string = "UPDATE {automation_db_name}.{process_log_table} set file_process_status = {process_status}, " \
                           " file_process_end_time = {end_time},record_count = {record_count}, " \
                           " process_message = {message}  where batch_id = {batch_id} and file_id = {file_id} " \
                           "and file_process_name = {" \
                           "process_name} "
            query = query_string.format(automation_db_name=automation_log_database,
                                        process_log_table=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                        process_status=process_status, batch_id=batch_id, file_id=file_id,
                                        process_name=process_name, end_time="NOW() ",
                                        message=message, record_count=record_count)

            status_message = "Input query for creating load automation log : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager(self.execution_context).execute_query_mysql(query)
            status_message = "Update Load automation result : " + json.dumps(result)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Completed function to update load automation log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def update_file_audit_status(self, automation_log_database=None, log_info=None):
        status_message = ""
        try:
            status_message = "Starting function to file audit status"
            logger.debug(status_message, extra=self.execution_context.get_context())

            batch_id = log_info[CommonConstants.BATCH_ID]
            file_id = log_info[CommonConstants.FILE_ID]
            process_status = str("'") + log_info[CommonConstants.PROCESS_STATUS] + str("'")
            process_status_id = log_info['status_id']

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration
            query_string = "UPDATE {automation_db_name}.{file_audit_table} " \
                           "set " \
                           "file_status = {status_desc}" \
                           ", file_process_end_time = {upd_time} " \
                           "where batch_id = {batch_id} and file_id = {file_id} "
            query = query_string.format(automation_db_name=automation_log_database,
                                        file_audit_table=CommonConstants.FILE_AUDIT_TABLE,
                                        status_id=process_status_id, status_desc=process_status,
                                        batch_id=batch_id, file_id=file_id,
                                        upd_time="NOW() ")

            status_message = "Input query for updating file audit status : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager().execute_query_mysql(query)
            status_message = "Completed function to update file audit status"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_file_audit_status_by_batch_id(self, automation_log_database=None, log_info=None):
        status_message = ""
        try:
            status_message = "Starting function to update file audit status for input " + str(log_info)
            logger.info(status_message, extra=self.execution_context.get_context())

            batch_id = log_info['batch_id']
            process_status = str("'") + log_info[CommonConstants.PROCESS_STATUS] + str("'")
            process_status_id = log_info['status_id']

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration
            query_string = "UPDATE {automation_db_name}.{file_audit_table} " \
                           "set " \
                           "file_status = {status_desc}" \
                           ", file_process_end_time = {upd_time} " \
                           "where batch_id= {batch_id}  "
            query = query_string.format(automation_db_name=automation_log_database,
                                        file_audit_table=CommonConstants.FILE_AUDIT_TABLE,
                                        status_id=process_status_id, status_desc=process_status,
                                        batch_id=str(batch_id), upd_time="NOW() ")

            status_message = "Input query for updating file audit status : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager(self.execution_context).execute_query_mysql(query)
            status_message = "Completed function to update file audit status"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def update_proces_log_status_by_batch_id(self, automation_log_database=None, log_info=None):
        status_message = ""
        try:
            status_message = "Starting function to update file process status"
            logger.debug(status_message, extra=self.execution_context.get_context())

            batch_id = log_info['batch_id']
            process_status = str("'") + log_info[CommonConstants.PROCESS_STATUS] + str("'")
            process_status_id = log_info['status_id']
            process_name = str("'") + log_info[CommonConstants.PROCESS_NAME] + str("'")
            query_string = "UPDATE {automation_db_name}.{process_log} " \
                           "set " \
                           "file_process_status = {status_desc}" \
                           ", file_process_end_time = {upd_time} " \
                           "where batch_id= {batch_id}  and file_process_name = {process_name}"
            query = query_string.format(automation_db_name=automation_log_database,
                                        process_log=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                        status_desc=process_status,
                                        batch_id=str(batch_id), process_name=process_name, upd_time="NOW() ")

            status_message = "Input query for updating file audit status : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager().execute_query_mysql(query)
            status_message = "Completed function to update file audit status"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_process_log_by_batch(self, automation_log_database=None, log_info=None):
        status_message = ""
        try:
            status_message = "Starting function to update file process status"
            logger.debug(status_message, extra=self.execution_context.get_context())

            batch_id = log_info['batch_id']
            process_status = log_info[CommonConstants.PROCESS_STATUS]
            # process_status_id = log_info['status_id']
            process_name = log_info[CommonConstants.PROCESS_NAME]
            process_message = log_info['message']
            record_count = None
            if process_name != CommonConstants.FILE_PROCESS_NAME_DQM and process_status == CommonConstants.STATUS_SUCCEEDED:
                record_count = log_info['record_count']

            query_string = "UPDATE {automation_db_name}.{process_log} " \
                           "set " \
                           "file_process_status = '{status_desc}'" \
                           ", file_process_end_time = {upd_time} " \
                           ", process_message = '{message}'" \
                           " where batch_id= {batch_id}  and file_process_name = '{process_name}'"
            query = query_string.format(automation_db_name=automation_log_database,
                                        process_log=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                        status_desc=process_status,
                                        batch_id=str(batch_id), process_name=process_name, upd_time="NOW() ",
                                        message=process_message)

            status_message = "Input query for updating file audit status : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager().execute_query_mysql(query)

            # Update record count for each file id if the file process status is SUCCEEDED
            if process_status == CommonConstants.STATUS_SUCCEEDED:
                file_id_result = self.get_file_ids_by_batch_id(automation_log_database, batch_id)
                for i in range(0, len(file_id_result)):
                    file_id = file_id_result[i]['file_id']
                    if process_name != CommonConstants.FILE_PROCESS_NAME_DQM:
                        if record_count.filter(record_count[CommonConstants.PARTITION_FILE_ID] == file_id).collect() != []:
                            count = record_count.filter(record_count[CommonConstants.PARTITION_FILE_ID] == file_id).collect()[0]['count']
                        else:
                            count = 0
                    else:
                        count = 0
                    CommonUtils().update_record_count_for_file(automation_log_database, file_id, count, process_name)

            status_message = "Completed function to update file audit status"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_record_count_for_file(self, automation_log_database, file_id, count, process_name):
        status_message = ""
        try:
            status_message = "Starting function to update record count for file_id : " + str(
                file_id) + " and process_name : " + str(process_name)
            logger.debug(status_message, extra=self.execution_context.get_context())

            query_string = "update {automation_log_database}.{log_file_dtl} set record_count={count} where file_id={file_id} and file_process_name='{process_name}'"
            query = query_string.format(automation_log_database=automation_log_database,
                                        log_file_dtl=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                        count=count,
                                        file_id=file_id,
                                        process_name=process_name)
            result = MySQLConnectionManager().execute_query_mysql(query)

            status_message = "Completed function to update record count for file_id : " + str(
                file_id) + " process_name : " + str(process_name)
            logger.info(status_message, extra=self.execution_context.get_context())
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_batch_ids_by_process_info(self, automation_log_database, dataset_id, cluster_id, workflow_id, process_id):
        status_message = ""
        try:
            status_message = "Starting function to get batches from log_batch_dtl"
            logger.debug(status_message, extra=self.execution_context.get_context())

            query_string = "select batch_id from {automation_log_database}.{log_batch_dtl}" \
                           " where dataset_id={dataset_id} and cluster_id='{cluster_id}' and workflow_id='{workflow_id}'" \
                           " and process_id={process_id}"
            query = query_string.format(automation_log_database=automation_log_database,
                                        log_batch_dtl=CommonConstants.BATCH_TABLE,
                                        dataset_id=dataset_id,
                                        cluster_id=cluster_id,
                                        workflow_id=workflow_id,
                                        process_id=process_id)
            result = MySQLConnectionManager().execute_query_mysql(query)
            status_message = "Completed function to get batch ids from log_batch_dtl table"
            logger.info(status_message, extra=self.execution_context.get_context())
            return result

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e
    def get_list_of_batch_id_by_dataset(self, dataset_id):
        """
            Purpose :   This method will fetch list of batch ids
            Input   :   Dataset Id
            Output  :   Batch Id list
        """
        try:
            batch_query_str = "select batch_id from {audit_db}.{batch_log_table} where dataset_id={dataset_id} and batch_status = 'IN PROGRESS'"
            batch_query = batch_query_str.format(audit_db=self.audit_db,
                                                 batch_log_table=CommonConstants.BATCH_TABLE,
                                                 dataset_id=dataset_id)
            batch_id_result = MySQLConnectionManager().execute_query_mysql(batch_query)
            status_message = "Batch Ids retrieved : " + str(batch_id_result)
            logger.info(status_message, extra=self.execution_context.get_context())
            return batch_id_result
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            self.execution_context.set_context({"traceback": str(e)})
            logger.error(str(e), extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_file_ids_by_batch_id(self, automation_log_database, batch_id):
        status_message = ""
        try:
            status_message = "Starting function to get file ids for batch id : ", str(batch_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            query_to_get_files_for_batch_str = "Select file_id from {automation_db_name}.{log_file_smry} where batch_id={batch_id}"
            query_to_get_files_for_batch = query_to_get_files_for_batch_str.format(
                automation_db_name=automation_log_database,
                log_file_smry=CommonConstants.FILE_AUDIT_TABLE,
                batch_id=batch_id)
            file_id_result = MySQLConnectionManager().execute_query_mysql(query_to_get_files_for_batch)
            status_message = "Completed function to get file ids for batch id : ", str(batch_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "File ids retrieved : ", str(file_id_result)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return file_id_result
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def check_dqm_error_location(self, s3_dir_path, cluster_mode=None):
        status_message = ""
        check_flag = False
        try:
            status_message = "Started function to list all the files in DQM error location : "
            logger.info(status_message, extra=self.execution_context.get_context())
            cluster_mode = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                 "cluster_mode"])
            # Fetch s3 connection
            if cluster_mode == 'EMR':
                # conn = boto.s3.connect_to_region(str(s3_aws_region), aws_access_key_id=s3_access_key,
                #                                  aws_secret_access_key=s3_secret_key, is_secure=True,
                #                                  calling_format=boto.s3.connection.OrdinaryCallingFormat())
                conn = boto.connect_s3(host = CommonConstants.BOTO_HOST)
            else:
                s3_cred_params = self.fetch_s3_conn_details_for_user()
                s3_access_key = s3_cred_params.get_configuration([CommonConstants.S3_ACCESS_KEY])
                s3_secret_key = s3_cred_params.get_configuration([CommonConstants.S3_SECRET_KEY])
                s3_aws_region = s3_cred_params.get_configuration([CommonConstants.S3_AWS_REGION])
                logger.debug(s3_access_key, extra=self.execution_context.get_context())
                logger.debug(s3_secret_key, extra=self.execution_context.get_context())
                logger.debug(s3_aws_region, extra=self.execution_context.get_context())
                conn = boto.connect_s3(aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)
            status_message = "Created s3 connection object with provided connecton details"
            logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "Started process to get all file paths (with complete url) in s3 folder " + s3_dir_path
            logger.debug(status_message, extra=self.execution_context.get_context())
            bucket_name = self.get_s3_bucket_name(s3_dir_path)
            bucket = conn.get_bucket(bucket_name)
            s3_folder_path = self.get_s3_folder_path(s3_dir_path)
            s3_folder_list = bucket.list(s3_folder_path)
            s3_file_list = []
            file_count = 0
            if not s3_folder_list:
                status_message = "Provided S3 directory is empty " + s3_dir_path
                return True
            else:
                for file in s3_folder_list:
                    file_name = self.get_file_name(file.name)
                    s3_file_list.append(file_name)

                    if str(file.name).endswith('.parquet'):
                        file_count = file_count + 1

                if file_count <= 0:
                    check_flag = True

            status_message = "Completed function to list all the files in S3 directory"
            logger.debug(status_message, extra=self.execution_context.get_context())
            return check_flag
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_file_name(self, file_path):
        name_list = file_path.split("/")
        return name_list[len(name_list) - 1]

    def insert_dqm_status(self, log_info):
        try:
            status_message = "Starting function to create logs for dqm"
            logger.debug(status_message, extra=self.execution_context.get_context())
            audit_db = self.audit_db
            application_id = log_info['application_id']
            dataset_id = log_info['dataset_id']
            batch_id = log_info['batch_id']
            qc_id = log_info['qc_id']
            column_name = log_info['column_name']
            qc_type = log_info['qc_type']
            qc_param = log_info['qc_param']
            criticality = log_info['criticality']
            error_count = log_info['error_count']
            qc_message = log_info['qc_message']
            qc_failure_percentage = log_info['qc_failure_percentage']
            qc_status = log_info['qc_status']
            qc_start_time = log_info['qc_start_time']
            qc_create_by = log_info['qc_create_by']
            qc_create_date = log_info['qc_create_date']
            file_id_result = self.get_file_ids_by_batch_id(audit_db, batch_id)
            for i in range(0, len(file_id_result)):
                file_id = file_id_result[i]['file_id']
                status_message = "Inserting the status for the pre-dqm checks for file id : ", file_id
                logger.debug(status_message, extra=self.execution_context.get_context())
                query_str = "insert into {audit_db}.{log_dqm_smry} (application_id,dataset_id,batch_id,file_id,qc_id," \
                            "column_name,qc_type,qc_param,criticality,error_count,qc_message,qc_failure_percentage," \
                            "qc_status,qc_start_time,qc_create_by,qc_create_date)" \
                            "values('{app_id}',{dataset_id},{batch_id},{file_id},{qc_id},'{column_name}','{qc_type}'," \
                            "'{qc_param}','{criticality}',{error_count},'{qc_message}','{qc_failure_percentage}'," \
                            "'{qc_status}','{qc_start_time}','{create_by}','{create_ts}')"
                query = query_str.format(audit_db=audit_db,
                                         log_dqm_smry=CommonConstants.LOG_DQM_SMRY,
                                         app_id=application_id,
                                         dataset_id=dataset_id,
                                         batch_id=batch_id,
                                         file_id=file_id,
                                         qc_id=qc_id,
                                         column_name=column_name,
                                         qc_type=qc_type,
                                         qc_param=qc_param,
                                         criticality=criticality,
                                         error_count=error_count,
                                         qc_message=qc_message,
                                         qc_failure_percentage=qc_failure_percentage,
                                         qc_status=qc_status,
                                         qc_start_time=qc_start_time,
                                         create_by=qc_create_by,
                                         create_ts=qc_create_date)
                status_message = "Query DQM log creation result : ", query
                logger.debug(status_message, extra=self.execution_context.get_context())
                result = MySQLConnectionManager().execute_query_mysql(query)
                status_message = "Load DQM log creation result : " + json.dumps(result)
                logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "Completing function to create DQM log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_dqm_status(self, log_info):
        status_message = ""
        try:
            status_message = "Starting function to create logs for dqm"
            logger.debug(status_message, extra=self.execution_context.get_context())
            audit_db = self.audit_db
            application_id = log_info['application_id']
            dataset_id = log_info['dataset_id']
            batch_id = log_info['batch_id']
            qc_id = log_info['qc_id']
            qc_status = log_info['qc_status']
            qc_end_time = log_info['qc_end_time']
            file_id_result = self.get_file_ids_by_batch_id(audit_db, batch_id)
            if qc_status == CommonConstants.STATUS_FAILED:
                for i in range(0, len(file_id_result)):
                    file_id = file_id_result[i]['file_id']
                    status_message = "Updating status for the dqm checks for file id : ", file_id
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    query_update_str = "update {audit_db}.{log_dqm_smry} set qc_status ='{qc_status}'," \
                                       "qc_end_time ='{qc_end_time}' where application_id ='{application_id}'" \
                                       " and dataset_id={dataset_id} and batch_id= {batch_id} and file_id={file_id} and qc_id={qc_id}"
                    query_update = query_update_str.format(audit_db=audit_db,
                                                           log_dqm_smry=CommonConstants.LOG_DQM_SMRY,
                                                           qc_status=qc_status,
                                                           qc_end_time=qc_end_time,
                                                           application_id=application_id,
                                                           dataset_id=dataset_id,
                                                           batch_id=batch_id,
                                                           file_id=file_id,
                                                           qc_id=qc_id)
                    status_message = "Query to update DQM log : " + json.dumps(query_update)
                    logger.debug(status_message, extra=self.execution_context.get_context())
                    result = MySQLConnectionManager().execute_query_mysql(query_update)
                    status_message = "Load DQM log creation result : " + json.dumps(result)
                    logger.debug(status_message, extra=self.execution_context.get_context())
            if qc_status == CommonConstants.STATUS_SUCCEEDED:
                file_id = log_info['file_id']
                error_count = log_info['error_count']
                qc_message = log_info['qc_message']
                qc_failure_percentage = log_info['qc_failure_percentage']
                query_update_str = "update {audit_db}.{log_dqm_smry} set qc_status ='{qc_status}'," \
                                   "qc_end_time ='{qc_end_time}',error_count={error_count},qc_message='{qc_message}'," \
                                   "qc_failure_percentage='{qc_failure_percentage}' where application_id ='{application_id}' " \
                                   "and dataset_id={dataset_id} and batch_id= {batch_id} and file_id={file_id} and qc_id={qc_id}"
                query_update = query_update_str.format(audit_db=audit_db,
                                                       log_dqm_smry=CommonConstants.LOG_DQM_SMRY,
                                                       qc_status=qc_status,
                                                       qc_end_time=qc_end_time,
                                                       error_count=error_count,
                                                       qc_message=qc_message,
                                                       qc_failure_percentage=qc_failure_percentage,
                                                       application_id=application_id,
                                                       dataset_id=dataset_id,
                                                       batch_id=batch_id,
                                                       file_id=file_id,
                                                       qc_id=qc_id)
                result = MySQLConnectionManager().execute_query_mysql(query_update)

            status_message = "Completing function to create load automation log"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def _get_complete_s3_url(self, s3_location, s3_access_key, s3_secret_key):
        """
        Purpose   :   This method will form the fully qualified s3 url with access, secret key and bucket
        Input     :   s3 location without access and secret key, access key and secret key
        Output    :   Returns the s3 url in the form - s3://access_key:secret_key@bucket/path
        """
        status_message = "Starting function to get complete s3 url"
        logger.debug(status_message, extra=self.execution_context.get_context())
        if s3_access_key is None or s3_secret_key is None or s3_access_key == "" or s3_secret_key == "":
            return s3_location
        # This will extract url is s3, s3a, s3n, etc.
        s3_uri = s3_location[0:s3_location.index("://") + 3]
        # Replacing forward slash with "%2f" for encoding http url
        s3_access_key = s3_access_key.replace("/", "%2F")
        s3_secret_key = s3_secret_key.replace("/", "%2F")
        # This will form the url prefix with access key and secret key
        s3_prefix_path = s3_uri + s3_access_key + ":" + s3_secret_key + "@"
        # Forming the complete s3 url by replacing the s3X://
        s3_complete_location = s3_location.replace(s3_uri, s3_prefix_path)
        status_message = "S3 url with access key and secret key is created"
        logger.debug(status_message, extra=self.execution_context.get_context())
        return s3_complete_location

    def replace_variables(self, raw_string, dict_replace_params):
        status_message = ""
        try:
            # Sort the dictionary keys in ascending order
            dict_keys = list(dict_replace_params.keys())
            dict_keys.sort(reverse=True)
            # Iterate through the dictionary containing parameters for replacements and replace all the variables in the
            # raw string with their values
            for key in dict_keys:
                raw_string = raw_string.replace(
                    str(CommonConstants.VARIABLE_PREFIX + key + CommonConstants.VARIABLE_PREFIX),
                    dict_replace_params[key])
            if str(raw_string).__contains__(CommonConstants.VARIABLE_PREFIX):
                status_message = "Variable value(s) not found in query parameter config files" + str(raw_string)
                raise Exception(status_message)

            return raw_string
        except Exception as e:
            raise e

    def replace_values(self, original_value, replaced_value):
        """ Replaces set of values with new values """
        for key, val in list(replaced_value.items()):
            original_value = original_value.replace(key, val)
        return original_value

    def list_files_in_s3_directory(self, s3_dir_path, cluster_mode=None, file_pattern=None):
        status_message = ""
        try:
            status_message = "Started function to list all the files in S3 directory for s3_dir_path : " + str(
                s3_dir_path) + " and file pattern : " + str(file_pattern)
            logger.info(status_message, extra=self.execution_context.get_context())
            cluster_mode = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                 "cluster_mode"])
            # Fetch s3 connection
            if cluster_mode == 'EMR':
                # conn = boto.s3.connect_to_region(str(s3_aws_region), aws_access_key_id=s3_access_key,
                #                                  aws_secret_access_key=s3_secret_key, is_secure=True,
                #                                  calling_format=boto.s3.connection.OrdinaryCallingFormat())
                conn = boto.connect_s3(host = CommonConstants.BOTO_HOST)
            else:
                s3_cred_params = self.fetch_s3_conn_details_for_user()
                s3_access_key = s3_cred_params.get_configuration([CommonConstants.S3_ACCESS_KEY])
                s3_secret_key = s3_cred_params.get_configuration([CommonConstants.S3_SECRET_KEY])
                s3_aws_region = s3_cred_params.get_configuration([CommonConstants.S3_AWS_REGION])
                logger.debug(s3_access_key, extra=self.execution_context.get_context())
                logger.debug(s3_secret_key, extra=self.execution_context.get_context())
                logger.debug(s3_aws_region, extra=self.execution_context.get_context())
                conn = boto.connect_s3(aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)
            status_message = "Created s3 connection object with provided connecton details"
            logger.debug(status_message, extra=self.execution_context.get_context())

            status_message = "Started process to get all file paths (with complete url) in s3 folder " + s3_dir_path
            logger.debug(status_message, extra=self.execution_context.get_context())
            bucket_name = self.get_s3_bucket_name(s3_dir_path)
            bucket = conn.get_bucket(bucket_name)
            s3_folder_path = self.get_s3_folder_path(s3_dir_path)
            s3_folder_list = bucket.list(s3_folder_path)
            s3_file_list = []
            if not s3_folder_list:
                status_message = "Provided S3 directory is empty " + s3_dir_path
                raise Exception(status_message)
            for file in s3_folder_list:
                file_name = self.get_file_name(file.name)
                if not str(file.name).endswith('/'):
                    s3_file_full_url = None
                    if file_pattern is None:
                        s3_file_full_url = str(CommonConstants.S3A_PREFIX). \
                            __add__(bucket_name).__add__('/').__add__(str(file.name))
                        s3_file_list.append(s3_file_full_url)
                    else:
                        is_pattern_match = PatternValidator().patter_validator(file_name, file_pattern)
                        if is_pattern_match:
                            s3_file_full_url = str(CommonConstants.S3A_PREFIX). \
                                __add__(bucket_name).__add__('/').__add__(str(file.name))
                            s3_file_list.append(s3_file_full_url)
            status_message = "Completed function to list all the files in S3 directory"
            logger.debug(status_message, extra=self.execution_context.get_context())
            return s3_file_list
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_s3_bucket_name(self, s3_full_dir_path):
        status_message = ""
        s3_tmp_path = ""
        try:
            status_message = "Getting s3 bucket name from s3 complete url"
            logger.debug(status_message, extra=self.execution_context.get_context())
            if s3_full_dir_path is not None:
                s3_tmp_path = str(s3_full_dir_path).replace("s3://", "").replace("s3n://", "").replace("s3a://", "")
            else:
                status_message = "S3 full directory path is not provided"
                raise Exception(status_message)
            dirs = s3_tmp_path.split('/')
            s3_bucket_name = dirs[0]
            status_message = "Completed fetch of s3 bucket name from s3 complete url(with s3n or s3a prefixes) with bucket name = " + str(
                s3_bucket_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return s3_bucket_name
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_s3_folder_path(self, s3_full_dir_path):
        status_message = ""
        s3_tmp_path = ""
        try:
            status_message = "Getting s3 folder path from s3 complete url(with s3n or s3a prefixes)"
            logger.debug(status_message, extra=self.execution_context.get_context())
            if s3_full_dir_path is not None:
                s3_tmp_path = str(s3_full_dir_path).replace("s3://", "").replace("s3n://", "").replace("s3a://", "")

            dirs = s3_tmp_path.split('/')
            s3_bucket_name = dirs[0]
            s3_folder_path = s3_tmp_path.lstrip(s3_bucket_name).lstrip('/')
            status_message = "Completed fetch of s3 folder path from s3 complete url with s3n or s3a prefixes"
            logger.debug(status_message, extra=self.execution_context.get_context())
            return s3_folder_path
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def get_dataset_information(self, dataset_id=None):
        status_message = ""
        source_file_location = None
        try:
            status_message = "Executing get_source_file_location function of module FileCheckHandler for dataset_id : " + str(
                dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            mysql_manager = MySQLConnectionManager(self.execution_context)

            if mysql_manager is not None:
                query_string = "SELECT * from " + self.audit_db + "." + \
                               CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME + " where dataset_id = " + \
                               str(dataset_id)
                result = mysql_manager.execute_query_mysql(query_string, True)
                status_message = "Dataset details for dataset_id : " + str(dataset_id) + " - " + str(result)
                logger.debug(status_message, extra=self.execution_context.get_context())
                return result
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_dataset_information_from_dataset_id(self, dataset_id=None):
        status_message = ""
        source_file_location = None
        try:
            status_message = "Executing get_dataset_information_from_dataset_id function with dataset id : " + str(
                dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            mysql_manager = MySQLConnectionManager(self.execution_context)

            if mysql_manager is not None:
                query_string = "SELECT * from " + self.audit_db + "." + \
                               CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME + " where dataset_id = " + \
                               str(dataset_id)
                result = mysql_manager.execute_query_mysql(query_string, True)
                status_message = "Dataset details for dataset_id : " + str(dataset_id) + " - " + str(result)
                logger.debug(status_message, extra=self.execution_context.get_context())
                return result
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def get_file_details_for_current_batch(self, curr_batch_id=None, dataset_id=None):
        status_message = ""
        file_details = dict()
        pre_landing_location = None
        try:
            status_message = "Get file details from previous load step"
            logger.debug(status_message, extra=self.execution_context.get_context())
            mysql_manager = MySQLConnectionManager()
            if mysql_manager is not None:
                query_string = "SELECT * from " + self.audit_db + "." + \
                               CommonConstants.FILE_AUDIT_TABLE + " where batch_id = " + \
                               str(curr_batch_id) + " and dataset_id = " + str(
                    dataset_id)
                result = mysql_manager.execute_query_mysql(query_string)
                return result
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def execute_shell_command(self, command):
        status_message = ""
        try:
            status_message = "Started executing shell command " + command
            logger.debug(status_message, extra=self.execution_context.get_context())
            command_output = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            standard_output, standard_error = command_output.communicate()
            if standard_output:
                standard_output_lines = standard_output.splitlines()
                for line in standard_output_lines:
                    logger.debug(line, extra=self.execution_context.get_context())
            if standard_error:
                standard_error_lines = standard_error.splitlines()
                for line in standard_error_lines:
                    logger.debug(line, extra=self.execution_context.get_context())
            if command_output.returncode == 0:
                return True
            else:
                status_message = "Error occurred while executing command on shell :" + command
                raise Exception(status_message)
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def execute_restricted_shell_command(self, command):
        status_message = ""
        try:
            status_message = "Started executing restricted shell command "
            logger.debug(status_message, extra=self.execution_context.get_context())
            command_output = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            standard_output, standard_error = command_output.communicate()
            logger.debug(standard_output, extra=self.execution_context.get_context())
            logger.debug(standard_error, extra=self.execution_context.get_context())
            if command_output.returncode == 0:
                return True
            else:
                status_message = "Error occurred while executing restricted command on shell :"
                raise Exception(status_message)
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_dataset_info(self, file_master_id=None):
        status_message = ""
        source_file_location = None
        try:
            status_message = "Executing get_source_file_location function of module FileCheckHandler"
            logger.debug(status_message, extra=self.execution_context.get_context())
            mysql_manager = MySQLConnectionManager()

            if mysql_manager is not None:
                query_string = "SELECT * from " + self.audit_db + "." + \
                               CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME + " where dataset_id = " + \
                               str(file_master_id)
                result = mysql_manager.execute_query_mysql(query_string, True)
                status_message = "Dataset details for dataset_id : " + str(file_master_id) + " - " + str(result)
                logger.debug(status_message, extra=self.execution_context.get_context())
                return result
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def get_loaded_file_details(self, file_master_id=None, file_name=None, process_name=None):
        status_message = ""
        file_details = dict()
        pre_landing_location = None
        try:
            status_message = "Starting function get_loaded_file_details for dataset id: " + str(
                file_master_id) + " and file name : " + str(file_name)
            logger.info(status_message, extra=self.execution_context.get_context())
            mysql_manager = MySQLConnectionManager(self.execution_context)
            if mysql_manager is not None:
                query = "select * from {audit_information}.{file_audit_information} " \
                        "where dataset_id = {file_master_id} and file_name = '{file_name}' " \
                        "and file_status IN ('{process_success}', '{process_inprogress}','{process_skipped}') ". \
                    format(audit_information=self.audit_db,
                           file_audit_information=CommonConstants.FILE_AUDIT_TABLE,
                           file_master_id=str(file_master_id),
                           file_name=file_name,
                           process_success=CommonConstants.STATUS_SUCCEEDED,
                           process_skipped=CommonConstants.STATUS_SKIPPED,
                           process_inprogress=CommonConstants.IN_PROGRESS_DESC)
                result = mysql_manager.execute_query_mysql(query)
                return result
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def create_load_summary_entry(self, file_master_id=None, batch_id=None, file_id=None, prelanding_count=None):
        status_message = ""
        try:
            status_message = "Starting function to create load summary entry"
            logger.debug(status_message, extra=self.execution_context.get_context())
            # Prepare the query string for creating an entry for the newly started batch process
            query_string = "INSERT INTO {audit_db}.{load_summary_table} (dataset_id,batch_id, file_id" \
                           "pre_landing_record_count) VALUES ({file_master_id}, {batch_id}, {file_id}, " \
                           "{prelanding_count)".format(
                audit_db=self.audit_db,
                load_summary_table=CommonConstants.LOAD_SUMMARY_TABLE,
                file_master_id=file_master_id, batch_id=batch_id, file_id=file_id,
                prelanding_count=prelanding_count)

            status_message = "Input query for creating load summary entry : " + query_string
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            MySQLConnectionManager().execute_query_mysql(query_string)
            status_message = 'Completed the creation of load summary entry'
            logger.debug(status_message, extra=self.execution_context.get_context())
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_load_summary_count(self, file_master_id=None, batch_id=None, file_id=None, location_type=None,
                                  record_count=None):
        status_message = ""
        try:
            status_message = "Starting function to update process log status"
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Prepare the query string for creating an entry for the newly started load process
            # in the log table and the query type & automation log configuration

            query_string = "UPDATE {automation_db_name}.{load_summary_table} set {file_location_type} = " \
                           "{record_count} " \
                           "where dataset_id = {file_master_id} and batch_id = {batch_id} and file_id = {" \
                           "file_id} "

            query = query_string.format(automation_db_name=self.audit_db,
                                        file_location_type=location_type,
                                        record_count=record_count, batch_id=batch_id, file_id=file_id)

            status_message = "Input query for updating the load summary table : " + query
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager().execute_query_mysql(query)
            status_message = "Update Load summary result : " + json.dumps(result)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Completed function to update load summary count"
            logger.info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def update_emr_termination_status(self, cluster_id=None, db_name=None):
        status_message = ""
        if db_name is None:
            db_name = self.audit_db
        try:
            status_message = "Starting function to update emr termination status and time"
            print(status_message)
            # .debug(status_message, extra=self.execution_context.get_context())
            cluster_terminate_user = CommonUtils().get_user_name()
            query_string = "UPDATE {db_name}.{emr_cluster_details} set cluster_status ='{cluster_emr_terminate}', cluster_stop_time = NOW(),cluster_terminate_user='{cluster_terminate_user}' " \
                           " where cluster_id = '{cluster_id}'"
            query = query_string.format(db_name=db_name,
                                        emr_cluster_details=CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
                                        cluster_emr_terminate=CommonConstants.CLUSTER_EMR_TERMINATE_STATUS,
                                        cluster_id=cluster_id, cluster_terminate_user=cluster_terminate_user)
            print(query)
            status_message = "Input query for creating emr termianting status : " + query
            # .debug(status_message, extra=self.execution_context.get_context())

            # Call the My-sql Utility with the query string
            result = MySQLConnectionManager().execute_query_mysql(query)
            status_message = "Update Load automation result : " + json.dumps(result)
            # .debug(status_message, extra=self.execution_context.get_context())
            status_message = "Completed function to update load automation log"
            # .info(status_message, extra=self.execution_context.get_context())

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            # .error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def correct_file_extension(self, file_complete_uri=None, location_type=None):
        status_message = "Starting correct_file_extension for  file_complete_uri : " + str(
            file_complete_uri) + " and location type : " + str(location_type)
        logger.info(status_message, extra=self.execution_context.get_context())

        try:
            if file_complete_uri and not str(file_complete_uri).endswith('/'):
                location_extension = os.path.splitext(file_complete_uri)
                if self.is_file_with_invalid_extension:
                    path_with_corrected_extension = location_extension[0] + location_extension[1].lower()
                    if location_type == 'S3':
                        driver = create_clidriver()
                        s3_move_cmd = 's3 mv ' + file_complete_uri + ' ' + path_with_corrected_extension
                        driver.main(s3_move_cmd.split())
                        return path_with_corrected_extension
                    elif location_type == 'HDFS':
                        hdfs.rename(file_complete_uri, path_with_corrected_extension)
                        return path_with_corrected_extension
                    else:
                        status_message = 'Invalid location type is not provided or invalid'
                        raise Exception(status_message)
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    # will go in CommonUtils
    def is_file_with_invalid_extension(self, file_complete_uri=None):

        status_message = "Starting is_file_with_invalid_extension for  file_complete_uri : " + str(file_complete_uri)
        logger.info(status_message, extra=self.execution_context.get_context())

        if file_complete_uri and not str(file_complete_uri).endswith('/'):
            file_extension = os.path.splitext(file_complete_uri)[1]
            return not file_extension.islower()

    # will go in CommonUtils
    def get_s3_file_name(self, s3_complete_uri=None):
        status_message = "Starting get_s3_file_name for  s3_complete_uri : " + str(s3_complete_uri)
        logger.info(status_message, extra=self.execution_context.get_context())

        s3_file_name = ""
        if s3_complete_uri:
            splits = str(s3_complete_uri).split('/')
            if splits.__len__() > 0:
                s3_file_name = splits[splits.__len__() - 1]
        return str(s3_file_name)

    # will go in CommonUtils
    def perform_hadoop_distcp(self, source_location=None, target_location=None, multipart_size_mb=None):
        try:
            cluster_mode = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                                 "cluster_mode"])
            status_message = "Starting perform_hadoop_distcp for source location : " + source_location + " and target_location : " + str(
                target_location)
            logger.info(status_message, extra=self.execution_context.get_context())

            if cluster_mode != 'EMR':
                # Fetch s3 connection details for current user
                aws_s3_cred_params = self.fetch_s3_conn_details_for_user()
                s3_access_key = aws_s3_cred_params.get_configuration([CommonConstants.S3_ACCESS_KEY])
                s3_secret_key = aws_s3_cred_params.get_configuration([CommonConstants.S3_SECRET_KEY])
                distcp_cmd = str('hadoop distcp ').__add__('-Dfs.s3a.awsAccessKeyId=').__add__(s3_access_key).__add__(
                    ' -Dfs.s3a.awsSecretAccessKey=').__add__(s3_secret_key).__add__(' -overwrite ').__add__(
                    source_location). \
                    __add__(' ').__add__(target_location)
            else:
                distcp_cmd = str('hadoop distcp ').__add__(' -overwrite ').__add__(
                    source_location). \
                    __add__(' ').__add__(target_location)

            status_message = "Performing hadoop distcp command  : " + str(distcp_cmd)
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Execute s3 cmd shell command for file copy
            self.execute_shell_command(distcp_cmd)
            status_message = 'Executed distcp command successfully '
            logger.debug(status_message, extra=self.execution_context.get_context())

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(exception)
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def delete_file(self, file_complete_uri=None, location_type=None):
        if location_type == 'S3':
            driver = create_clidriver()
            s3_rm_cmd = 's3 rm ' + file_complete_uri
            driver.main(s3_rm_cmd.split())
        #elif location_type == 'HDFS':
            #pydoop.hdfs.rmr(file_complete_uri)

    def create_step_audit_entry(self, process_id, frequency, step_name, data_date, cycle_id):
        """
            Purpose :   This method inserts step entry in step log table
            Input   :   Process Id, Frequency, Step Name, Data Date , Cycle Id
            Output  :   NA
        """
        try:
            status = CommonConstants.STATUS_RUNNING
            status_message = "Started preparing query for doing step audit entry for process_id:" + str(process_id) + \
                             " " + "and frequency:" + frequency + " " + "and data_date:" + str(data_date) \
                             + " " + "cycle id:" + str(cycle_id) + " " + "and step name:" + str(step_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            step_audit_entry_query = "Insert into {audit_db}.{step_audit_table} (process_id, frequency, data_date, " \
                                     "cycle_id,step_name, step_status, step_start_time) " \
                                     "values({process_id}, '{frequency}', '{data_date}', {cycle_id}, '{step_name}'," \
                                     " '{step_status}', {step_start_time})". \
                format(audit_db=self.audit_db, step_audit_table=CommonConstants.LOG_STEP_DTL,
                       process_id=process_id, frequency=frequency, data_date=data_date,
                       cycle_id=cycle_id, step_name=step_name,
                       step_start_time="NOW()", step_status=status)
            logger.debug(step_audit_entry_query, extra=self.execution_context.get_context())
            MySQLConnectionManager().execute_query_mysql(step_audit_entry_query)
            status_message = "Completed executing query for doing step audit entry for process_id:" + str(process_id) + \
                             " " + "and frequency:" + frequency + " " + "and data_date:" + str(data_date) \
                             + " " + "cycle id:" + str(cycle_id) + " " + "and step name:" + str(step_name)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Error occured in doing step audit entry for process_id:" + str(process_id) + " " \
                             + "and frequency:" + frequency + " " + "and data_date:" + str(data_date) \
                             + " " + "cycle id:" + str(cycle_id) + " " + "and step name:" + str(step_name)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def update_step_audit_entry(self, process_id, frequency, step_name, data_date, cycle_id, status):
        """
            Purpose :   This method updates step execution status in step log table
            Input   :   Process Id, Frequency, Step Name, Data Date , Cycle Id, Status
            Output  :   NA
        """
        try:
            status_message = "Started preparing query to update step audit entry for process_id:" + str(process_id) + \
                             " " + "and frequency:" + frequency + " " + "and data_date:" + str(data_date) \
                             + " " + "cycle id:" + str(cycle_id) + " " + "and step name:" + str(step_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            step_update_audit_status_query = "Update {audit_db}.{step_audit_table} set step_status='{status}'," \
                                             "step_end_time={step_end_time} where " \
                                             "process_id={process_id} and frequency='{frequency}' and " \
                                             "step_name='{step_name}' and data_date='{data_date}' and " \
                                             "cycle_id = '{cycle_id}'".format(audit_db=self.audit_db,
                                                                              step_audit_table=CommonConstants.LOG_STEP_DTL,
                                                                              process_id=process_id, status=status,
                                                                              step_end_time="NOW()",
                                                                              frequency=frequency,
                                                                              step_name=step_name,
                                                                              data_date=data_date, cycle_id=cycle_id)
            logger.debug(step_update_audit_status_query, extra=self.execution_context.get_context())
            MySQLConnectionManager().execute_query_mysql(step_update_audit_status_query)
            status_message = "Completed executing query to update step audit entry for process_id:" + str(process_id) + \
                             " " + "and frequency:" + frequency + " " + "and data_date:" + str(data_date) \
                             + " " + "and cycle id:" + str(cycle_id) + " " + "and step name:" + str(step_name)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Error occured in  updating step audit entry for process_id:" + str(process_id) + \
                             " " + "and frequency:" + frequency + " " + "and data_date:" + str(data_date) \
                             + " " + "and cycle id:" + str(cycle_id) + " " + "and step name:" + str(step_name)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def replace_key(self, path=None):
        try:
            status_message = "In Replace Key Function For Path : " + str(path)
            logger.debug(status_message, extra=self.execution_context.get_context())
            if path is None:
                raise Exception("Input Path Can Not Be None")

            if path == "Null" or path == "NONE" or path == "":
                raise Exception("Input Path Is Null/None/Empty")

            regex_for_key = "((\\$\\$(\\w+)\\$\\$))"
            compiled_regex = re.compile(regex_for_key)

            keys_arr = compiled_regex.findall(path)

            i = 0
            for key in keys_arr:
                search_key = str(key[2])
                i = i + 1
                if search_key is not None:
                    value = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, search_key])
                    if value is None:
                        status_message = "Key Not Found For Path : " + str(path)
                        logger.error(status_message, extra=self.execution_context.get_context())
                        raise Exception("Key Not Found In Env Json For Path" + str(path))
                    else:
                        path = path.replace('$$' + search_key + '$$', str(value))

            return path
        except Exception as exception:
            status_message = "Exception In Replace Key Function Due To : " + str(exception)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def execute_hive_query(self, query=None):
        """
        Purpose   :   This method is used to execute hive query
        Input     :   query  (Mandatory)
        Output    :   Returns the result
        """
        conn = None
        try:
            status_message = "Executing execute_hive_query function for query : " + str(query)
            logger.debug(status_message, extra=self.execution_context.get_context())

            if query is None or query == "":
                raise Exception("Query Can Not be None/Empty")

            job_flow = json.load(open(CommonConstants.EMR_JOB_FLOW_FILE_PATH, "r"))
            master_host = job_flow[CommonConstants.MASTER_PRIVATE_DNS_KEY]

            if self.hive_port is None:
                h_port = 10000
            elif self.hive_port == "":
                h_port = 10000
            elif not self.hive_port:
                h_port = 10000
            else:
                h_port = self.hive_port

            conn = hive.Connection(host=str(master_host), port=int(h_port))
            cursor = conn.cursor()
            cursor.execute(query)

            if query.lower().startswith("describe"):
                return cursor.fetchall()

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception
        finally:
            if conn is not None:
                conn.close()

    def update_cycle_audit_entry(self, process_id, frequency, data_date, cycle_id, status):
        """
            Purpose :   This method updates cycle execution status in cycle log table
            Input   :   Process Id, Frequency, Data Date , Cycle Id, Status
            Output  :   NA
        """
        try:
            status_message = "Started preparing query to update " + status + " cycle status for process_id:" \
                             + str(process_id) + " " + "and frequency:" + frequency + " " + "and data_date:" + \
                             str(data_date) + " " + "cycle id:" + str(cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            if status != CommonConstants.STATUS_RUNNING:
                cycle_update_audit_status_query = "Update {audit_db}.{cycle_details} set cycle_status='{status}'," \
                                                  "cycle_end_time={cycle_end_time} where process_id={process_id} and " \
                                                  "frequency='{frequency}' and data_date='{data_date}' and " \
                                                  "cycle_id = '{cycle_id}'".format(audit_db=self.audit_db,
                                                                                   cycle_details=CommonConstants.LOG_CYCLE_DTL,
                                                                                   process_id=process_id, status=status,
                                                                                   cycle_end_time="NOW()",
                                                                                   frequency=frequency,
                                                                                   data_date=data_date,
                                                                                   cycle_id=cycle_id)
            else:
                cycle_update_audit_status_query = "Update {audit_db}.{cycle_details} set cycle_status='{status}' " \
                                                  "where process_id={process_id} and " \
                                                  "frequency='{frequency}' and data_date='{data_date}' and " \
                                                  "cycle_id = '{cycle_id}'".format(audit_db=self.audit_db,
                                                                                   cycle_details=CommonConstants.LOG_CYCLE_DTL,
                                                                                   process_id=process_id, status=status,
                                                                                   frequency=frequency,
                                                                                   data_date=data_date,
                                                                                   cycle_id=cycle_id)
            logger.debug(cycle_update_audit_status_query, extra=self.execution_context.get_context())
            MySQLConnectionManager().execute_query_mysql(cycle_update_audit_status_query)
            status_message = "Completed executing query to update " + status + " cycle status for process_id:" \
                             + str(process_id) + " " + "and frequency:" + frequency + " " + "and data_date:" + \
                             str(data_date) + " " + "cycle id:" + str(cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Error occured in to updating " + status + " cycle status for process_id:" \
                             + str(process_id) + " " + "and frequency:" + frequency + " " + "and data_date:" + \
                             str(data_date) + " " + "cycle id:" + str(cycle_id)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def get_target_datasets_by_process_id(self,process_id):
        """
                    Purpose :   This method will fetch list of target data set ids
                    Input   :   process id
                    Output  :   dataset id list
        """
        try:
            dataset_id_str = "select dataset_id from {audit_db}.{ctl_process_dependency_master} where process_id={process_id} and table_type = 'Target'"
            dataset_id_query = dataset_id_str.format(audit_db=self.audit_db,
                                                 ctl_process_dependency_master=CommonConstants.PROCESS_DEPENDENCY_MASTER_TABLE,
                                                 process_id=process_id)
            dataset_id_result = MySQLConnectionManager().execute_query_mysql(dataset_id_query)
            status_message = "Dataset Ids retrieved : " + str(dataset_id_result)
            logger.info(status_message, extra=self.execution_context.get_context())
            return dataset_id_result
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            self.execution_context.set_context({"traceback": str(e)})
            logger.error(str(e), extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e


    def fetch_cycle_id_for_data_date(self, process_id, frequency, data_date):
        """
            Purpose :   This method fetches in progress cycle id for input process , frequency and data date
            Input   :   Process Id, Frequency,  Data Date
            Output  :   Cycle Id
        """
        try:
            status_message = "Started preparing query to fetch in progress cycle id for process_id:" + str(process_id) + \
                             " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_in_progress_cycle_query = "Select cycle_id from {audit_db}." \
                                            "{cycle_details} where process_id={process_id} and data_date='{data_date}' and " \
                                            " frequency='{frequency}' and cycle_status='{cycle_status}'". \
                format(audit_db=self.audit_db,
                       cycle_details=CommonConstants.LOG_CYCLE_DTL, process_id=process_id,
                       frequency=frequency, data_date=data_date,
                       cycle_status=CommonConstants.STATUS_RUNNING)
            logger.debug(fetch_in_progress_cycle_query, extra=self.execution_context.get_context())
            cycle_id_result = MySQLConnectionManager().execute_query_mysql(fetch_in_progress_cycle_query, True)
            status_message = "Completed executing query to fetch in progress cycle id for process_id:" + str(
                process_id) + \
                             " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
            logger.info(status_message, extra=self.execution_context.get_context())
            if not cycle_id_result:
                status_message = " No cycle id is in " + CommonConstants.STATUS_RUNNING + " for process_id:" \
                                 + str(process_id) + " " + "and frequency:" + str(frequency) + " " + \
                                 "and data date:" + str(data_date)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            logger.debug(cycle_id_result, extra=self.execution_context.get_context())
            cycle_id = cycle_id_result['cycle_id']
            return cycle_id
        except Exception as exception:
            status_message = "Error occured to fetch in progress cycle id for process_id:" + str(process_id) + \
                             " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def fetch_data_date_process(self, process_id, frequency):
        """
            Purpose :   This method fetches data date for input process id and frequency
            Input   :   Process Id, Frequency
            Output  :   Data Date
        """
        try:
            status_message = "Started preparing query to fetch data date for process_id:" + str(process_id) + " " + \
                             "and frequency:" + str(frequency)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_data_date_query = "Select data_date from {audit_db}.{process_date_table} where " \
                                    "process_id={process_id} and frequency='{frequency}'". \
                format(audit_db=self.audit_db,
                       process_date_table=CommonConstants.PROCESS_DATE_TABLE,
                       process_id=process_id, frequency=frequency
                       )
            logger.debug(fetch_data_date_query, extra=self.execution_context.get_context())
            data_date_result = MySQLConnectionManager().execute_query_mysql(fetch_data_date_query, True)
            status_message = "Completed executing query to fetch data date for process_id:" + str(process_id) + " " + \
                             "and frequency:" + str(frequency)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(data_date_result, extra=self.execution_context.get_context())
            if not data_date_result:
                status_message = "No entry in " + CommonConstants.PROCESS_DATE_TABLE + " for process_id:" + \
                                 str(process_id) + " " + "and frequency:" + str(frequency)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            data_date = data_date_result['data_date']
            return data_date
        except Exception as exception:
            status_message = "Exception in fetching data date for process id:" + str(process_id) + " " + \
                             "and frequency:" + str(frequency)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def copy_hdfs_to_s3(self, table_name):
        """
            Purpose :   This method is a wrapper to copy data from hdfs to s3
            Input   :   Hive Table Name
            Output  :   NA
        """
        try:
            status_message = "Starting handler function to invoke Copy HDFS to S3 for table name:" + str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Starting to prepare command to copy " + str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            copy_command = "python3 ClusterToS3LoadHandler.py -ht " + str(table_name) + " "
            logger.debug(copy_command, extra=self.execution_context.get_context())
            copy_command_response = subprocess.Popen(copy_command, stdout=subprocess.PIPE,
                                                     stderr=subprocess.PIPE, shell=True)
            copy_command_output, copy_command_error = copy_command_response.communicate()
            if copy_command_response.returncode != 0:
                logger.error(copy_command_error, extra=self.execution_context.get_context())
                raise Exception(copy_command_error)
            logger.debug(copy_command_output, extra=self.execution_context.get_context())
            status_message = "Completed executing handler function to Copy data from HDFS to S3 for " \
                             "table name:" + str(table_name)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Exception occured in copy hdfs to s3 for table name" + str(table_name)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def get_master_ip_from_cluster_id(self, cluster_id=None):
        try:
            status_message = "Fetching EMR master ip for cluster id: " + str(cluster_id)
            logger.info(status_message, extra=self.execution_context.get_context())

            query = "select master_node_dns from {audit_db}.{log_cluster_dtl} where cluster_id='{cluster_id}'".format(
                audit_db=self.audit_db, log_cluster_dtl=CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
                cluster_id=str(cluster_id))

            status_message = "Query to fetch master node DNS " + str(query)
            logger.debug(status_message, extra=self.execution_context.get_context())

            result_set = MySQLConnectionManager().execute_query_mysql(query)

            if len(result_set) == 0:
                raise Exception("No Cluster Found with cluster id: " + str(cluster_id))

            if result_set:
                master_node_dns = result_set[0]['master_node_dns']

            host_ip_address = master_node_dns.split('.')[0].replace('ip-', '').replace('-', '.')


            #extracted_ip = re.search('ip.(.+?).ec2', master_node_dns).group(1)
            #host_ip_address = extracted_ip.replace("-", ".")

            status_message = " master node DNS " + str(host_ip_address)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return host_ip_address
        except Exception as exception:
            status_message = "Exception in get_master_ip_from_cluste_id due to  " + str(exception)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def fetch_column_details(self, dataset_id, column_identifier_name):
        """
            Purpose :   This method fetch column information for input column name
            Input   :   Dataset Id , Column Name
            Output  :   Column Identifier Name Result List
        """
        try:
            status_message = "Starting function to fetch column details for dataset id:" + str(dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Column name is:" + str(column_identifier_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_column_metadata_query = "Select {column_identifier},column_sequence_number from {audit_db}." \
                                          "{column_metadata_table} where dataset_id={dataset_id}". \
                format(audit_db=self.audit_db, column_metadata_table=CommonConstants.COLUMN_METADATA_TABLE,
                       dataset_id=dataset_id, column_identifier=column_identifier_name)
            logger.debug(fetch_column_metadata_query, extra=self.execution_context.get_context())
            column_details = MySQLConnectionManager().execute_query_mysql(fetch_column_metadata_query)
            column_id_seq_dict = dict()
            column_identifier_list = []
            status_message = "Starting to prepare list for " + str(column_identifier_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            for column_detail in column_details:
                column_id_seq_dict.__setitem__(column_detail['column_sequence_number'],
                                               column_detail[column_identifier_name])
            for column_seq in sorted(column_id_seq_dict.keys()):
                column_identifier_list.append(column_id_seq_dict.get(column_seq))
            status_message = "Completed preparing list for " + str(column_identifier_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return column_identifier_list
        except Exception as exception:
            status_message = "Exception occured while fetching column details for dataset_id:" + str(dataset_id)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def get_partition(self, max_size=None):

        status_message = "Starting get_partition value for Total size : " + str(max_size)
        logger.debug(status_message, extra=self.execution_context.get_context())

        if max_size is None:
            raise Exception("total_size can not be None")

        result = int(max_size) / (int(CommonConstants.BLOCK_SIZE) * int(CommonConstants.BYTES_TO_MB))
        rounding_result = math.ceil(result)

        if int(rounding_result) < 2:
            return 2
        else:
            return int(rounding_result)

    def get_extension(self, file_path):

        status_message = "Starting to get extension for file for input path : " + str(file_path)
        logger.debug(status_message, extra=self.execution_context.get_context())

        if file_path is None:
            raise Exception("File Path Can Not be None")

        if len(file_path) > 0:
            return file_path.split(".")[::-1][0]
        else:
            raise Exception("Error Occured In Getting File Extension For Input Path: " + str(file_path))

    def get_dynamic_repartition_value(self, s3_dir_path=None):

        try:

            status_message = "Starting get dynamic repartion value for s3 path : " + str(s3_dir_path)
            logger.debug(status_message, extra=self.execution_context.get_context())

            if s3_dir_path is None:

                raise Exception("Input Path Can Not Be None")

            s3_loc = s3_dir_path.replace("*", "")
            conn = boto.connect_s3(host = CommonConstants.BOTO_HOST)
            bucket_name = self.get_s3_bucket_name(s3_loc)
            bucket = conn.get_bucket(bucket_name)
            s3_folder_path = self.get_s3_folder_path(s3_loc)
            s3_folder_list = bucket.list(s3_folder_path)
            # total_size = 0
            max_size = 0
            ext = ""

            #            for file in s3_folder_list:
            #                total_size = total_size + file.size
            #                ext = self.get_extension(file.key)

            for file in s3_folder_list:
                if file.size > max_size:
                    max_size = file.size
                ext = self.get_extension(file.key)

            status_message = "Extension For File Is  : " + str(ext)
            logger.debug(status_message, extra=self.execution_context.get_context())

            if ext.upper() == 'GZ':
                max_size = max_size * CommonConstants.GZ_COMPRESSION_RATIO
            if ext.upper() == 'ZIP':
                max_size = max_size * CommonConstants.ZIP_COMPRESSION_RATIO
            if ext.upper() == 'BZ2':
                max_size = max_size * CommonConstants.BZ2_COMPRESSION_RATIO

            status_message = "Total size for location : " + str(s3_dir_path) + " is : " + str(max_size)
            logger.debug(status_message, extra=self.execution_context.get_context())

            if max_size > CommonConstants.BYTES_TO_GB:
                status_message = "File Greater then 1 GB"
                logger.debug(status_message, extra=self.execution_context.get_context())

            elif max_size <= CommonConstants.BYTES_TO_GB and max_size >= CommonConstants.BYTES_TO_MB:
                status_message = "File Size in MB"
                logger.debug(status_message, extra=self.execution_context.get_context())

            else:
                status_message = "File size in KB"
                logger.debug(status_message, extra=self.execution_context.get_context())

            partition = self.get_partition(max_size)
            status_message = "Number Of Partitions Assigned For The File Path  : " + str(s3_dir_path) + " Is : " \
                             + str(partition)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return partition
        except Exception as exception:
            status_message = "Exception In Get Dynamic repartiton Due To : " + str(exception)
            logger.error(status_message, extra=self.execution_context.get_context())
            raise Exception(status_message)

    def fetch_new_cycle_id(self, process_id, frequency, data_date):
        """
            Purpose :   This method returns cycle id in "NEW" state for input process id, frequency and data date
            Input   :   Process Id, Frequency, Data Date
            Output  :   Cycle Id
        """
        try:
            status_message = "Started preparing query to fetch new cycle id for process_id:" + str(process_id) + \
                             " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
            logger.debug(status_message, extra=self.execution_context.get_context())
            check_new_cycle_status_query = "Select cycle_id from {audit_db}.{cycle_details} where " \
                                           "process_id={process_id} and frequency='{frequency}' and " \
                                           "data_date='{data_date}' and cycle_status='{cycle_status}'" \
                .format(audit_db=self.audit_db,
                        cycle_details=CommonConstants.LOG_CYCLE_DTL,
                        process_id=process_id,
                        frequency=frequency,
                        data_date=data_date,
                        cycle_status=CommonConstants.STATUS_NEW)
            logger.debug(check_new_cycle_status_query, extra=self.execution_context.get_context())
            cycle_id_result = MySQLConnectionManager().execute_query_mysql(check_new_cycle_status_query, True)
            status_message = "Completed executing query to fetch new cycle id for process_id:" + str(process_id) + \
                             " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(cycle_id_result, extra=self.execution_context.get_context())
            cycle_id = cycle_id_result['cycle_id']
            if cycle_id is None:
                status_message = "No cycle id is in " + CommonConstants.STATUS_NEW + " for process id:" + str(
                    process_id) + \
                                 " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            return cycle_id
        except Exception as exception:
            status_message = "Error occured in fetching new cycle id for process_id:" + str(process_id) + \
                             " " + "and frequency:" + str(frequency) + " " + "and data date:" + str(data_date)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def update_cycle_cluster_id_entry(self, process_id, frequency, data_date, cycle_id, cluster_id):
        """
            Purpose :   This method updates cluster id in cycle log table
            Input   :   Process Id, Frequency, Data Date , Cycle Id, Cluster Id
            Output  :   NA
        """
        try:
            status_message = "Started preparing query to update cluster id '" + cluster_id + "' for process_id:" \
                             + str(process_id) + " " + "and frequency:" + frequency + " " + "and data_date:" + \
                             str(data_date) + " " + "cycle id:" + str(cycle_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            cycle_update_audit_cluster_id_query = "Update {audit_db}.{cycle_details} set cluster_id='{cluster_id}' " \
                                                  "where process_id={process_id} and frequency='{frequency}' " \
                                                  "and data_date='{data_date}' and cycle_id = '{cycle_id}' and " \
                                                  "cycle_status='{cycle_status}'".format(audit_db=self.audit_db,
                                                                                         cycle_details=CommonConstants.LOG_CYCLE_DTL,
                                                                                         process_id=process_id,
                                                                                         frequency=frequency,
                                                                                         data_date=data_date,
                                                                                         cycle_id=cycle_id,
                                                                                         cluster_id=cluster_id,
                                                                                         cycle_status=CommonConstants.STATUS_RUNNING)
            logger.debug(cycle_update_audit_cluster_id_query, extra=self.execution_context.get_context())
            MySQLConnectionManager().execute_query_mysql(cycle_update_audit_cluster_id_query)
            status_message = "Completed executing query to update cluster id '" + cluster_id + "' for process_id:" \
                             + str(process_id) + " " + "and frequency:" + frequency + " " + "and data_date:" + \
                             str(data_date) + " " + "cycle id:" + str(cycle_id)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Error occured in updating cluster id '" + cluster_id + "' for process_id:" \
                             + str(process_id) + " " + "and frequency:" + frequency + " " + "and data_date:" + \
                             str(data_date) + " " + "cycle id:" + str(cycle_id)
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def execute_threading(self, threads, thread_limit, queue_instance, failed_flag):
        """
            Purpose :   This method invokes thread execution for each thread
            Input   :   Thread List , Thread Limit
            Output  :   NA
        """
        thread_chunks = [threads[x:x + thread_limit] for x in range(0, len(threads), thread_limit)]
        for thread in thread_chunks:
            for thread_instance in thread:
                try:
                    thread_instance.start()
                except Exception as exception:
                    status_message = "Error in Thread Chunks start method"
                    error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                            " ERROR MESSAGE: " + str(traceback.format_exc())
                    self.execution_context.set_context({"traceback": error})
                    logger.error(status_message, extra=self.execution_context.get_context())
                    raise exception
            for thread_instance in thread:
                try:
                    thread_instance.join()
                    while not queue_instance.empty():
                        result = queue_instance.get()
                        if result[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                            failed_flag = True
                except Exception as exception:
                    status_message = "Error in Thread Chunks join method"
                    error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                            " ERROR MESSAGE: " + str(traceback.format_exc())
                    self.execution_context.set_context({"traceback": error})
                    logger.error(status_message, extra=self.execution_context.get_context())
                    raise exception
        return failed_flag

    def copy_hdfs_to_s3_optimized(self, table_name, spark_config=None):
        """ This is wrapper method to copy data from hdfs to s3 in a optimized manner
        Key Arguments:
        table_name : Hive table name, Spark Config
        """
        try:
            status_message = "Starting handler function to invoke Copy HDFS to S3 for table name:" \
                             + str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            status_message = "Starting to prepare command to copy " + str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            target_hdfs_location = "/" + str(CommonConstants.HDFS_REPARTION_PATH).strip("/") + "/" + table_name
            if spark_config is None:
                spark_config = ' '
            copy_command = "cd " + CommonConstants.AIRFLOW_CODE_PATH + \
                           "; " + CommonConstants.SPARK_BIN_PATH + "spark-submit " \
                           + str(spark_config) + " " + CommonConstants.AIRFLOW_CODE_PATH + \
                           "/" + "HiveToHDFSHandler.py  -st " + str(table_name) + " -tp " + str(target_hdfs_location)

            logger.debug(copy_command, extra=self.execution_context.get_context())
            copy_command_response = subprocess.Popen(copy_command, stdout=subprocess.PIPE,
                                                     stderr=subprocess.PIPE, shell=True)
            copy_command_output, copy_command_error = copy_command_response.communicate()
            if copy_command_response.returncode != 0:
                logger.error(copy_command_error, extra=self.execution_context.get_context())
                raise Exception(copy_command_error)
            logger.debug(copy_command_output, extra=self.execution_context.get_context())
            status_message = "Completed executing handler function to Copy data from Hive to HDFS " \
                             "for table name:" + str(table_name)
            logger.info(status_message, extra=self.execution_context.get_context())
            query = "select dataset_type,s3_post_dqm_location,table_s3_path,table_hdfs_path from {audit_db}." \
                    "{ctl_dataset_information} where table_name ='{table_name}'".format(
                audit_db=self.audit_db,
                ctl_dataset_information=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME,
                table_name=table_name)
            result_set = MySQLConnectionManager().execute_query_mysql(query, False)
            if len(result_set) != 1:
                raise Exception("Improper result ontained from mysql")
            if str(result_set[0]['dataset_type']).lower() == CommonConstants.RAW_DATASET_TYPE:
                target_s3_path = str(result_set[0]['s3_post_dqm_location'])
            elif str(result_set[0]['dataset_type']).lower() == CommonConstants.PROCESSED_DATASET_TYPE:
                target_s3_path = str(result_set[0]['table_s3_path'])
            else:
                raise Exception("Table Type Can Be Raw Or Processed")
            if target_s3_path.startswith('s3a://'):
                target_s3_path = target_s3_path.replace('s3a://', 's3://')
            copy_command = "cd " + CommonConstants.AIRFLOW_CODE_PATH + \
                           "; " + "python3  " \
                           + CommonConstants.AIRFLOW_CODE_PATH + \
                           "/" + "ClusterToS3LoadHandler.py -s " + str(target_hdfs_location) + " -t " + str(target_s3_path)
            logger.debug(copy_command, extra=self.execution_context.get_context())
            copy_command_response = subprocess.Popen(copy_command, stdout=subprocess.PIPE,
                                                     stderr=subprocess.PIPE, shell=True)
            copy_command_output, copy_command_error = copy_command_response.communicate()
            if copy_command_response.returncode != 0:
                logger.error(copy_command_error, extra=self.execution_context.get_context())
                raise Exception(copy_command_error)
            logger.debug(copy_command_output, extra=self.execution_context.get_context())
            status_message = "Completed executing command to copy data from " + \
                             str(target_hdfs_location) + " to " \
                             + str(target_s3_path)
            logger.info(status_message, extra=self.execution_context.get_context())
        except Exception as exception:
            status_message = "Exception occurred in copy hdfs to s3 for table name" + str(table_name)
            self.execution_context.set_context({"traceback": str(traceback.format_exc())})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def get_staging_partition(self,dir_path=None):
        """Method to get Staging partition"""
        try:
            status_message = "Starting staging process to get number of partitions"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())
            args_list = ['hdfs', 'dfs', '-du','-s', dir_path]
            proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            s_output, s_err = proc.communicate()
            status_message = "Size of directory " + dir_path + " is " + str(s_output)
            logger.debug(status_message, extra=self.execution_context.get_context())
            #dir_size=  bytes(s_output,'UTF-8')
            print(s_output)
            s_output = s_output.decode('UTF-8')
            print(s_output)
            dir_size = s_output.split(' ')[0].strip()

            no_of_partitions = float(dir_size)/(int(CommonConstants.BLOCK_SIZE)*int(CommonConstants
                                                                                    .BYTES_TO_MB))
            status_message = "number of partitions for staging :" + str(no_of_partitions)
            logger.debug(status_message, extra=self.execution_context.get_context())
            if no_of_partitions < 1:
                return 1
            else:
                return int(no_of_partitions)
        except Exception as exception:
                self.execution_context.set_context({"function_status": 'FAILED',
                                                    "traceback": str(traceback.format_exc())})
                logger.error(str(exception), extra=self.execution_context.get_context())
                self.execution_context.set_context({"function_status": '', "traceback": ""})
                raise exception

    def fetch_successful_batch_id_for_dataset(self, dataset_id):
        """
            Purpose :   This method will fetch latest successful batch Id
            Input   :   Dataset Id
            Output  :   Batch Id
        """
        try:
            status_message = "Started preparing query to fetch latest successful batch id for dataset_id:" \
                             "" + str(dataset_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            fetch_successful_batch_dataset_query = "select max(batch_id) as batch_id from " \
                                                   "(select  distinct(A.batch_id),A.batch_status from {audit_db}.{batch_details_table} as A inner join {audit_db}.{file_details_table} as B " \
                                                   "on A.batch_id=B.batch_id where A.dataset_id = {dataset_id} " \
                                                   "and A.batch_id not in (select batch_id from {audit_db}.{file_details_table} where lower(file_process_name) = '{withdraw_process_name}' and file_process_status = '{batch_status}'))" \
                                                   " as T where T.batch_status ='{batch_status}'".format(audit_db=self.audit_db,batch_details_table=CommonConstants.BATCH_TABLE,file_details_table=CommonConstants.PROCESS_LOG_TABLE_NAME,dataset_id=dataset_id,withdraw_process_name=CommonConstants.WITHDRAW_PROCESS_NAME,batch_status=CommonConstants.STATUS_SUCCEEDED)
            logger.debug(fetch_successful_batch_dataset_query, extra=self.execution_context.get_context())

            batch_id_result = MySQLConnectionManager(self.execution_context).execute_query_mysql\
                (fetch_successful_batch_dataset_query, True)
            status_message = "Completed executing query to fetch latest successful batch id for dataset_id:" \
                             "" + str(dataset_id)
            logger.info(status_message, extra=self.execution_context.get_context())
            logger.debug(batch_id_result, extra=self.execution_context.get_context())
            batch_id = batch_id_result['batch_id']
            if batch_id is None:
                status_message = "There is no successful batch id for dataset_id:" + str(dataset_id)
                logger.error(status_message, extra=self.execution_context.get_context())
                raise Exception(status_message)
            return batch_id
        except Exception as exception:
            status_message = "Exception occured in fetching successful batch id from dataset_id:" + str(dataset_id)
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            logger.error(status_message + str(traceback.format_exc()))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception


    def get_list_of_file_id_by_batch(self, batch_id):
        """
            Purpose :   This method will fetch list of file ids
            Input   :   Batch Id
            Output  :   File Id list
        """
        try:
            file_query_str = "select file_id from {audit_db}.{file_log_table} where batch_id={batch_id}"
            file_query = file_query_str.format(audit_db=self.audit_db,
                                               file_log_table=CommonConstants.FILE_AUDIT_TABLE,
                                               batch_id=batch_id)
            file_id_result = MySQLConnectionManager().execute_query_mysql(file_query)
            status_message = "File Ids retrieved : " + str(file_id_result)
            logger.info(status_message, extra=self.execution_context.get_context())
            return file_id_result
        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            self.execution_context.set_context({"traceback": str(e)})
            logger.error(str(e), extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def set_inprogress_records_to_failed(self, step_name, batch_id):
        try:
            query = "Select * from {audit_db}.{log_file_dtl} where file_process_status='{status}' " \
                    "and batch_id='{batch_id}' and file_process_name='{step_name}'".format(
                audit_db=self.audit_db,
                log_file_dtl=CommonConstants.PROCESS_LOG_TABLE_NAME,
                status=CommonConstants.STATUS_RUNNING,
                batch_id=batch_id,
                step_name=step_name
            )
            inprogress_result = MySQLConnectionManager().execute_query_mysql(query)
            logger.info("Inprogress Process List:"+str(inprogress_result), extra=self.execution_context.get_context())
            if len(inprogress_result):
                for row in inprogress_result:
                    row['message'] = "FAILED Due to Unknown Reason"
                    row[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_FAILED
                    row[CommonConstants.PROCESS_NAME] = step_name
                    self.update_process_log_by_batch(self.audit_db, row)
                raise Exception("File Status was left IN PROGRESS even after completion of process")

        except Exception as exc:
            logger.error(exc, extra=self.execution_context.get_context())
            raise exc

    def get_yarn_app_id_status(self, app_name, cluster_ip, port=8088):
        response = {}
        try:
            url = "http://{cluster_ip}:{port}/ws/v1/cluster/apps".format(
                cluster_ip=cluster_ip,
                port=port,
            )
            get_response = requests.get(url)
            app_id_response = get_response.json()
            app_id_list = []
            for app in app_id_response["apps"]["app"]:
                if app_name in app["name"] :
                    app_id_list.append(app["id"])

            app_id_list.sort(reverse=True)

            latest_app_id = app_id_list[0]
            for app in app_id_response["apps"]["app"]:
                if app["id"] == latest_app_id:
                    latest_app_id_state = app["state"]
                    break

            app_url = "http://{cluster_ip}:{port}/ws/v1/cluster/apps/{application_id}".format(
                cluster_ip=cluster_ip,
                port=port,
                application_id=latest_app_id
            )
            get_app_response = requests.get(app_url)
            app_json = get_app_response.json()

            response["diagnostic_message"] = app_json["app"]["diagnostics"]
            response["app_id"] = latest_app_id
            response["state"] = latest_app_id_state
            return response
        except Exception as exc:
            logger.error(exc, extra=self.execution_context.get_context())
            raise exc