#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

import os
import traceback
import json
import argparse
import textwrap
import sys
from urllib.parse import urlparse
import threading
import queue
import boto3
import CommonConstants as CommonConstants
from LogSetup import logger
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
from FileTransferUtility import FileTransferUtility
from CommonUtils import CommonUtils
from MySQLConnectionManager import MySQLConnectionManager

sys.path.insert(0, os.getcwd())
# all module level constants are defined here
MODULE_NAME = "FileWithdrawUtility"
PROCESS_NAME = "Withdraw"

"""
Module Name         :   FileWithdrawUtility
Purpose             :   This module will read parameters provided and deletes files from S3 locations accordingly
                        parsing and validating the required parameters
Input Parameters    :   Parameters: batch_ids, file_ids, file_process_names,cycle_ids,process_ids,dataset_ids,datadates,frquecnies
Output Value        :   Return Dictionary with status(Failed/Success)
Last changed on     :   21st Feb 2019
Last changed by     :   Himanshu Khatri
Reason for change   :   converted python 2 to 3
Pending Development :
"""


class FileWithdrawUtility(object):
    """

    """

    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_name": PROCESS_NAME})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def file_withdraw(self, input_dict):
        """
        Purpose   :   This method validates input parameters and removes files/batches from respective s3 folders based on process_name
        Input     :   Input dictionary
        Output    :   Returns dictionary with status(Failed/Success)
        """

        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_RUNNING}
        try:
            status_message = "Executing file_withdraw function"
            logger.info(status_message, extra=self.execution_context.get_context())

            case = -1

            file_id_list=[]
            batch_id_list=[]
            cycle_id_list=[]
            file_process_locations=[]
            dw_target_table_locations=[]

            file_ids = input_dict["file_id"]
            batch_ids = input_dict["batch_id"]
            cycle_ids = input_dict["cycle_id"]

            if file_ids is not None and file_ids != "":
                file_id_list = file_ids.split(",")
            if batch_ids is not None and batch_ids != "":
                batch_id_list = batch_ids.split(",")
            if cycle_ids is not None and cycle_ids != "":
                cycle_id_list = cycle_ids.split(",")

            if file_ids is not None and cycle_ids is None:
                if batch_ids is None:
                    case = 0
                else:
                    case = 1
            elif batch_ids is not None and cycle_ids is None:
                if file_ids is None:
                    case = 2
            else:
                if cycle_ids is not None and (file_ids is None and batch_ids is None):
                    case = 3
                else:
                    case = -1

            status_message = "For the inputs provided Case value is : " + str(case)
            logger.info(status_message, extra=self.execution_context.get_context())

            if case == -1:
                status_message = "Input Parameters do not match any criteria to process file withdrawal"
                logger.error(status_message, extra=self.execution_context.get_context())
                result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED}
                return result_dictionary
            else:
                i = 0
                threads = []
                thread_limit = int(self.configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "spark_max_thread_limit"]))
                que = queue.Queue()
                failed_flag = False
                if case == 0:
                    # batch_ids not provided as inputs. Only file_ids are given
                    for file_id in file_id_list:
                        status_message = "Invoking withdraw function for file : " + str(file_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        t = threading.Thread(target=lambda q, file_id, batch_id, cycle_id,: q.put(
                            self.file_withdraw_task(file_id, None, None)), args=(que, file_id, None, None))
                        i = i + 1
                        threads.append(t)
                elif case == 1:
                    #both batch_ids and file_ids provided as inputs
                    for file_id in file_id_list:
                        status_message = "Invoking withdraw function for file : " + str(file_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        t = threading.Thread(target=lambda q, file_id, batch_id, cycle_id,: q.put(
                            self.file_withdraw_task(file_id, None, None)), args=(que, file_id, None, None))
                        i = i + 1
                        threads.append(t)
                    for batch_id in batch_id_list:
                        status_message = "Invoking withdraw function for batch : " + str(batch_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        t = threading.Thread(target=lambda q, file_id, batch_id, cycle_id,: q.put(
                            self.file_withdraw_task(None, batch_id, None)), args=(que, None, batch_id, None))
                        i = i + 1
                        threads.append(t)
                elif case == 2:
                    #only batch_ids provided as inputs
                    for batch_id in batch_id_list:
                        status_message = "Invoking withdraw function for batch : " + str(batch_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        t = threading.Thread(target=lambda q, file_id, batch_id, cycle_id,: q.put(
                            self.file_withdraw_task(None, batch_id, None)), args=(que, None, batch_id, None))
                        i = i + 1
                        threads.append(t)
                elif case == 3:
                    #cycle_ids are provided as inputs
                    for cycle_id in cycle_id_list:
                        status_message = "Invoking withdraw function for cycle : " + str(cycle_id)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        t = threading.Thread(target=lambda q, file_id, batch_id, cycle_id,: q.put(
                            self.file_withdraw_task(None, None, cycle_id)), args=(que, None, None, cycle_id))
                        i = i + 1
                        threads.append(t)
                thread_chunks = [threads[x:x + thread_limit] for x in range(0, len(threads), thread_limit)]
                for x in thread_chunks:
                    for y in x:
                        try:
                            y.start()
                        except Exception as e:
                            logger.error("Error in Thread Chunks")
                            logger.error(str(e))
                            logger.error(str(traceback.format_exc()))
                            raise e
                    for y in x:
                        try:
                            y.join()
                            while not que.empty():
                                result = que.get()
                                if result[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                                    failed_flag = True
                        except Exception as e:
                            logger.error("Error in Thread Chunks join()")
                            logger.error(str(e))
                            logger.error(str(traceback.format_exc()))
                            raise e

            status_message = "Completed file withdraw function"
            logger.info(status_message, extra=self.execution_context.get_context())
            if not failed_flag:
                result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
            else:
                result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED}
            return result_dictionary
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            raise exception

    def file_withdraw_task(self,file_id,batch_id,cycle_id):
        result_dict = {}
        status_message = "Starting file withdraw task "
        logger.info(status_message, extra=self.execution_context.get_context())
        status=""
        message="Withdraw Completed Successfully"
        try:
            if file_id is not None:
                batch_for_file = self.get_batch_id_by_file(file_id)
                self.create_logs(file_id,batch_for_file,None)
                file_process_locations = self.get_file_process_locations(file_id, None,None)
                if not file_process_locations or len(file_process_locations) == 0:
                    status_message = "No file process locations found in ctl_dataset_master for the given file_id,dataset_id"
                    logger.error(status_message)
                    status = CommonConstants.STATUS_FAILED
                    message = "Process failed in function get_file_process_locations"
                else:
                    for location in file_process_locations:
                        result_dict = self.withdraw_file_from_s3(file_id, batch_for_file,None, location)
                    if result_dict[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                        status = CommonConstants.STATUS_FAILED
                        message = "Process failed in function withdraw_file_from_s3"
                    else:
                        status = CommonConstants.STATUS_SUCCEEDED
                self.update_logs(file_id,batch_for_file,None,status,message)
            elif batch_id is not None:
                self.create_logs(None, batch_id,None)
                file_process_locations = self.get_file_process_locations(None, batch_id,None)
                if not file_process_locations or len(file_process_locations) == 0:
                    status_message = "No file process locations found in ctl_dataset_master for the given batch_id,dataset_id"
                    logger.error(status_message)
                    status = CommonConstants.STATUS_FAILED
                    message = "Process failed in function get_file_process_locations"
                else:
                    for location in file_process_locations:
                        result_dict = self.withdraw_file_from_s3(None, batch_id,None, location)
                    if result_dict[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                        status = CommonConstants.STATUS_FAILED
                        message = "Process failed in function withdraw_file_from_s3"
                    else:
                        status = CommonConstants.STATUS_SUCCEEDED
                self.update_logs(None, batch_id,None,status,message)
            elif cycle_id is not None:
                self.create_logs(None,None,cycle_id)
                file_process_locations = self.get_file_process_locations(None,None,cycle_id)
                if not file_process_locations or len(file_process_locations) == 0:
                    status_message = "No file process locations found in ctl_dataset_master for the given cycle_id,dataset_id"
                    logger.error(status_message)
                    status = CommonConstants.STATUS_FAILED
                    message = "Process failed in function get_file_process_locations"
                else:
                    for location in file_process_locations:
                        result_dict = self.withdraw_file_from_s3(None,None,cycle_id, location)
                    if result_dict[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                        status = CommonConstants.STATUS_FAILED
                        message = "Process failed in function withdraw_file_from_s3"
                    else:
                        status = CommonConstants.STATUS_SUCCEEDED
                self.update_logs(None,None,cycle_id,status,message)
            else:
                status_message = "Only one of the inputs can be non-NONE"
                logger.error(status_message + str(traceback.format_exc()))
                raise Exception(status_message)
            status_message = "Completed file withdraw task "
            logger.info(status_message, extra=self.execution_context.get_context())

        except Exception as exception:
            status_message = "In exception block for function file_withdraw_task"
            logger.info(status_message, extra=self.execution_context.get_context())
            if file_id is not None or batch_id is not None:
                log_info = {CommonConstants.BATCH_ID: batch_id,
                            CommonConstants.PROCESS_NAME: PROCESS_NAME,
                            'message': "Withdraw Failed : "+str(exception),
                            CommonConstants.PROCESS_STATUS: CommonConstants.STATUS_FAILED,
                            CommonConstants.FILE_ID: file_id}
                status_message = "Log info : " + str(log_info)
                logger.info(status_message, extra=self.execution_context.get_context())
                CommonUtils().update_process_status(self.audit_db, log_info)
            else:
                #add code for updating log_setp_dtl
                log_info_query_str = "select distinct process_id,frequency,data_date from {audit_db}.{log_step_dtl} where cycle_id={cycle_id}"
                log_info_query = log_info_query_str.format(audit_db=self.audit_db,
                                                           log_step_dtl=CommonConstants.LOG_STEP_DTL,
                                                           cycle_id=cycle_id)
                status_message = "Log query : " + str(log_info_query)
                logger.info(status_message, extra=self.execution_context.get_context())
                log_result = MySQLConnectionManager().execute_query_mysql(log_info_query)
                status_message = "Logs retrived : " + str(log_result)
                logger.info(status_message, extra=self.execution_context.get_context())
                process_id = log_result[0]["process_id"]
                frequency = log_result[0]["frequency"]
                step_name = PROCESS_NAME
                data_date = log_result[0]["data_date"]
                status = CommonConstants.STATUS_FAILED
                CommonUtils().update_step_audit_entry(process_id, frequency, step_name, data_date, cycle_id, status)

        return result_dict

    def get_file_process_locations(self,file_id,batch_id,cycle_id):
        status_message = "Starting function to get file process locations "
        logger.info(status_message, extra=self.execution_context.get_context())
        file_process_locations=[]
        try:
            if file_id is not None or batch_id is not None:
                if file_id is not None:
                    dataset_id_query_str = "select distinct(dataset_id) from {audit_db}.{process_log_table} where file_id={file_id}"
                    dataset_id_query = dataset_id_query_str.format(audit_db=self.audit_db,
                                                               process_log_table=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                                               file_id=file_id)
                    status_message = "Dataset_id query : "+str(dataset_id_query)
                    logger.info(status_message, extra=self.execution_context.get_context())
                elif batch_id is not None:
                    dataset_id_query_str = "select distinct(dataset_id) from {audit_db}.{process_log_table} where batch_id={batch_id}"
                    dataset_id_query = dataset_id_query_str.format(audit_db=self.audit_db,
                                                                   process_log_table=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                                                   batch_id=batch_id)
                    status_message = "Dataset_id query : " + str(dataset_id_query)
                    logger.info(status_message, extra=self.execution_context.get_context())
                else:
                    status_message = "Either file_id or batch_id must be NONE"
                    logger.error(status_message + str(traceback.format_exc()))
                    raise Exception(status_message)
                dataset_id_result = MySQLConnectionManager().execute_query_mysql(dataset_id_query)
                status_message = "Dataset_id retrieved : " + str(dataset_id_result)
                logger.info(status_message, extra=self.execution_context.get_context())
                dataset_id = dataset_id_result[0]['dataset_id']
                data_set_info = CommonUtils().get_dataset_information_from_dataset_id(dataset_id)
                pre_landing_location = data_set_info['pre_landing_location']
                pre_landing_location = str(pre_landing_location).rstrip('/')
                landing_location = data_set_info['s3_landing_location']
                landing_location = str(landing_location).rstrip('/')
                pre_dqm_location = data_set_info['s3_pre_dqm_location']
                pre_dqm_location = str(pre_dqm_location).rstrip('/')
                dqm_s3_error_location = str(self.configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "dqm_s3_error_location"]))
                dqm_s3_summary_location = str(self.configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "dqm_s3_summary_location"]))
                post_dqm_location = data_set_info['s3_post_dqm_location']
                post_dqm_location = str(post_dqm_location).rstrip('/')
                archive_location = data_set_info['archive_location']
                archive_location = str(archive_location).rstrip('/')
                publish_flag = data_set_info['dataset_publish_flag']
                publish_location = ""
                if publish_flag == 'Y':
                    publish_location = data_set_info['publish_s3_path']
                    publish_location = str(publish_location).rstrip('/')
                file_process_locations.append(pre_landing_location)
                file_process_locations.append(landing_location)
                file_process_locations.append(pre_dqm_location)
                file_process_locations.append(dqm_s3_error_location)
                file_process_locations.append(dqm_s3_summary_location)
                file_process_locations.append(post_dqm_location)
                file_process_locations.append(archive_location)
                file_process_locations.append(publish_location)
            else:
                if cycle_id is not None:
                    dataset_id_query_str = "select dataset_id from {audit_db}.{dependency_details} " \
                                           "where write_dependency_value={cycle_id} and table_type ='{table_type}' "
                    dataset_id_query = dataset_id_query_str.format(
                        audit_db=self.audit_db,
                        dependency_details=CommonConstants.PROCESS_DEPENDENCY_DETAILS_TABLE,
                        cycle_id=cycle_id,
                        table_type=CommonConstants.TARGET_TABLE_TYPE)
                    status_message = "Dataset_id query : " + str(dataset_id_query)
                    logger.info(status_message, extra=self.execution_context.get_context())
                    dataset_id_result = MySQLConnectionManager().execute_query_mysql(dataset_id_query)
                    if len(dataset_id_result) == 0:
                        raise Exception("No dataset id found for the cycle")
                    for i in range(0, len(dataset_id_result)):
                        dataset_id = dataset_id_result[i]["dataset_id"]
                        location_query_str = "select table_s3_path, publish_s3_path, dataset_publish_flag" \
                                             " from {audit_db}.{dataset_master} " \
                                             "where dataset_id={dataset_id} and dataset_type='processed'"
                        location_query = location_query_str.format(
                            audit_db=self.audit_db,
                            dataset_master=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME,
                            dataset_id=dataset_id)
                        status_message = "Location query : " + str(location_query)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        data_set_info = MySQLConnectionManager().execute_query_mysql(location_query)
                        if data_set_info:
                            location = data_set_info[0]["table_s3_path"]
                            status_message = "Table S3 Path : " + str(location)
                            logger.info(status_message, extra=self.execution_context.get_context())
                            file_process_locations.append(location)
                            publish_flag = data_set_info[0]['dataset_publish_flag']
                            if publish_flag == 'Y':
                                publish_location = data_set_info[0]['publish_s3_path']
                                publish_location = str(publish_location).rstrip('/')
                                file_process_locations.append(publish_location)
                        else:
                            logger.info("No info found for dataset: " + str(dataset_id),
                                        extra=self.execution_context.get_context())

                else:
                    status_message = "All inputs acnnot be NONE"
                    logger.error(status_message + str(traceback.format_exc()))
                    raise Exception(status_message)
            status_message = "File process locations : " + str(file_process_locations)
            logger.info(status_message, extra=self.execution_context.get_context())

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            #raise exception
        return file_process_locations

    def withdraw_file_from_s3(self, file_id, batch_id, cycle_id, location):
        status_message = "Executing S3 remove command  "
        logger.info(status_message, extra=self.execution_context.get_context())
        result_dict = {}
        file_path = ""

        if file_id is not None or batch_id is not None:
            if file_id is not None:
                file_path = location + "/" + CommonConstants.PARTITION_BATCH_ID + "=" + str(batch_id) + "/" + CommonConstants.PARTITION_FILE_ID + "=" + str(file_id) + "/"
                status_message = "File path : " + str(file_path)
                logger.debug(status_message, extra=self.execution_context.get_context())
            elif batch_id is not None:
                file_path = location + "/" + CommonConstants.PARTITION_BATCH_ID + "=" + str(batch_id) + "/"
                status_message = "File path : " + str(file_path)
                logger.debug(status_message, extra=self.execution_context.get_context())
            else:
                status_message = "Either file_id or batch_id must be NONE"
                logger.error(status_message + str(traceback.format_exc()))
                result_dict[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
                raise Exception(status_message)
        else:
            if cycle_id is not None:
                data_date = self.get_data_date_from_cycle(cycle_id)
                data_date = str(data_date).replace("-", "")
                file_path = location + "/" + CommonConstants.DATE_PARTITION + "=" + data_date + "/" + CommonConstants.CYCLE_PARTITION + "=" + str(cycle_id) + "/"
                status_message = "File path : " + str(file_path)
                logger.debug(status_message, extra=self.execution_context.get_context())
            else:
                status_message = "All inputs cannot NONE"
                logger.error(status_message + str(traceback.format_exc()))
                result_dict[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
                raise Exception(status_message)
        try:
            session = boto3.session.Session()
            s3 = session.client('s3')
            parsed_url = urlparse(file_path)
            s3_bucket = parsed_url.netloc
            file_path = os.path.join(parsed_url.path, "").lstrip("/")
            objects_to_delete = []
            if 'Contents' in s3.list_objects(Bucket=s3_bucket, Prefix=file_path) and isinstance(s3.list_objects(Bucket=s3_bucket, Prefix=file_path)['Contents'], list) and len(s3.list_objects(Bucket=s3_bucket, Prefix=file_path)['Contents']) is not None:
                for obj in s3.list_objects(Bucket=s3_bucket, Prefix=file_path)['Contents']:
                    objects_to_delete.append({'Key': obj["Key"]})
                    message_del = "DELETING THIS FILE:" + str(obj["Key"])
                    logger.info(message_del, extra=self.execution_context.get_context())
                s3_cmd_result = s3.delete_objects(Bucket=s3_bucket, Delete={'Objects': objects_to_delete})
                logger.info(str(objects_to_delete), extra=self.execution_context.get_context())
                status_message = "S3 command result : " + str(s3_cmd_result)
                logger.debug(status_message, extra=self.execution_context.get_context())
            else:
                status = "This path does not exist or is empty"
                logger.info(status, extra=self.execution_context.get_context())
            result_dict[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCEEDED

        except Exception as exception:
            status_message = "In exception block of withdraw_file_from_s3 function "
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dict[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            #raise exception
        status_message = "Completed S3 remove command for file " + str(file_id)
        logger.info(status_message, extra=self.execution_context.get_context())
        return result_dict

    def get_data_date_from_cycle(self,cycle_id):
        status_message = "Starting function to get data_date for cycle_id"
        logger.info(status_message, extra=self.execution_context.get_context())
        data_date = 0
        data_date_query_str = "select distinct data_date from {audit_db}.{log_step_dtl} where cycle_id={cycle_id}"
        data_date_query = data_date_query_str.format(audit_db=self.audit_db,
                                                     log_step_dtl = CommonConstants.LOG_STEP_DTL,
                                                     cycle_id = cycle_id)
        data_date_result = MySQLConnectionManager().execute_query_mysql(data_date_query)
        data_date = data_date_result[0]["data_date"]
        status_message = "Data date : " + str(data_date)
        logger.debug(status_message, extra=self.execution_context.get_context())
        return data_date

    def create_logs(self, file_id, batch_id,cycle_id):
        status_message = "Starting create_logs function "
        logger.info(status_message, extra=self.execution_context.get_context())
        if file_id is not None or batch_id is not None:
            log_info_query_str = "select distinct file_name,dataset_id,process_id from {audit_db}.{process_log_table} where batch_id={batch_id}"
            log_info_query = log_info_query_str.format(audit_db=self.audit_db,
                                                process_log_table=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                                 batch_id=batch_id)
            status_message = "Log query : " + str(log_info_query)
            logger.info(status_message, extra=self.execution_context.get_context())
            log_result = MySQLConnectionManager().execute_query_mysql(log_info_query)
            status_message = "Logs retrived : " + str(log_result)
            logger.info(status_message, extra=self.execution_context.get_context())

            if file_id is None:
                file_ids_result = self.get_file_ids_by_batch(batch_id)
                for i in range(0,len(file_ids_result)):
                    file_id_retrived = file_ids_result[i]['file_id']
                    process_info = {CommonConstants.FILE_NAME: log_result[0]['file_name'],
                                    'file_id':file_id_retrived,
                                       CommonConstants.FILE_STATUS: CommonConstants.STATUS_RUNNING,
                                       CommonConstants.DATASET_ID: log_result[0]['dataset_id'],
                                       CommonConstants.BATCH_ID: batch_id,
                                       CommonConstants.CLUSTER_ID: 'NA',
                                       CommonConstants.WORKFLOW_ID: 'NA',
                                       'process_id': log_result[0]['process_id'],
                                       CommonConstants.PROCESS_NAME: PROCESS_NAME
                                       }
                    status_message = "Log info : " + str(process_info)
                    logger.info(status_message, extra=self.execution_context.get_context())
                    CommonUtils().create_process_log_entry(self.audit_db, process_info)
            else:
                process_info = {CommonConstants.FILE_NAME: log_result[0]['file_name'],
                                'file_id': file_id,
                                CommonConstants.FILE_STATUS: CommonConstants.STATUS_RUNNING,
                                CommonConstants.DATASET_ID: log_result[0]['dataset_id'],
                                CommonConstants.BATCH_ID: batch_id,
                                CommonConstants.CLUSTER_ID: 'NA',
                                CommonConstants.WORKFLOW_ID: 'NA',
                                'process_id': log_result[0]['process_id'],
                                CommonConstants.PROCESS_NAME: PROCESS_NAME
                                }
                status_message = "Log info : " + str(process_info)
                logger.info(status_message, extra=self.execution_context.get_context())
                CommonUtils().create_process_log_entry(self.audit_db, process_info)
        else:
            log_info_query_str = "select distinct process_id,frequency,data_date from {audit_db}.{log_step_dtl} where cycle_id={cycle_id}"
            log_info_query = log_info_query_str.format(audit_db=self.audit_db,
                                                       log_step_dtl=CommonConstants.LOG_STEP_DTL,
                                                       cycle_id=cycle_id)
            status_message = "Log query : " + str(log_info_query)
            logger.info(status_message, extra=self.execution_context.get_context())
            log_result = MySQLConnectionManager().execute_query_mysql(log_info_query)
            status_message = "Logs retrived : " + str(log_result)
            logger.info(status_message, extra=self.execution_context.get_context())
            process_id = log_result[0]["process_id"]
            frequency = log_result[0]["frequency"]
            step_name = PROCESS_NAME
            data_date = log_result[0]["data_date"]
            CommonUtils().create_step_audit_entry(process_id, frequency, step_name, data_date, cycle_id)

        status_message = "Completed create_logs function "
        logger.info(status_message, extra=self.execution_context.get_context())

    def update_logs(self, file_id, batch_id,cycle_id,status,message):
        status_message = "Starting update_logs function "
        logger.info(status_message, extra=self.execution_context.get_context())
        if file_id is not None or batch_id is not None:
            log_info = {}
            if file_id is None:
                file_ids_result = self.get_file_ids_by_batch(batch_id)
                for i in range(0,len(file_ids_result)):
                    file_id_retrived = file_ids_result[i]['file_id']
                    log_info = {CommonConstants.BATCH_ID: batch_id,
                                CommonConstants.PROCESS_NAME: PROCESS_NAME,
                    'message': message,
                    CommonConstants.PROCESS_STATUS: status,
                    CommonConstants.FILE_ID: file_id_retrived}
                    status_message = "Log info : " + str(log_info)
                    logger.info(status_message, extra=self.execution_context.get_context())
                    CommonUtils().update_process_status(self.audit_db, log_info)
            else:
                log_info = {CommonConstants.BATCH_ID: batch_id,
                            CommonConstants.PROCESS_NAME: PROCESS_NAME,
                            'message': message,
                            CommonConstants.PROCESS_STATUS: status,
                            CommonConstants.FILE_ID: file_id}
                status_message = "Log info : " + str(log_info)
                logger.info(status_message, extra=self.execution_context.get_context())
                CommonUtils().update_process_status(self.audit_db, log_info)
        else:
            log_info_query_str = "select distinct process_id,frequency,data_date from {audit_db}.{log_step_dtl} where cycle_id={cycle_id}"
            log_info_query = log_info_query_str.format(audit_db=self.audit_db,
                                                       log_step_dtl=CommonConstants.LOG_STEP_DTL,
                                                       cycle_id=cycle_id)
            status_message = "Log query : " + str(log_info_query)
            logger.info(status_message, extra=self.execution_context.get_context())
            log_result = MySQLConnectionManager().execute_query_mysql(log_info_query)
            status_message = "Logs retrived : " + str(log_result)
            logger.info(status_message, extra=self.execution_context.get_context())
            process_id = log_result[0]["process_id"]
            frequency = log_result[0]["frequency"]
            step_name = PROCESS_NAME
            data_date = log_result[0]["data_date"]
            status = status
            CommonUtils().update_step_audit_entry(process_id, frequency, step_name, data_date, cycle_id, status)
        status_message = "Completed update_logs function "
        logger.info(status_message, extra=self.execution_context.get_context())

    def get_file_ids_by_batch (self,batch_id):
        file_query_str = "select distinct(file_id) from {audit_db}.{process_log_table} where batch_id={batch_id}"
        file_query = file_query_str.format(audit_db=self.audit_db,
                                            process_log_table=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                             batch_id=batch_id)
        status_message = "File query : " + str(file_query)
        logger.info(status_message, extra=self.execution_context.get_context())
        file_result = MySQLConnectionManager().execute_query_mysql(file_query)
        status_message = "Files retrived : " + str(file_result)
        logger.info(status_message, extra=self.execution_context.get_context())
        return file_result

    def get_batch_id_by_file (self,file_id):
        batch = 0
        batch_query_str = "select distinct(batch_id) from {audit_db}.{process_log_table} where file_id={file_id}"
        batch_query = batch_query_str.format(audit_db=self.audit_db,
                                            process_log_table=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                            file_id=file_id)
        status_message = "Batch query : " + str(batch_query)
        logger.info(status_message, extra=self.execution_context.get_context())
        batch_result = MySQLConnectionManager().execute_query_mysql(batch_query)
        status_message = "Batch retrived : " + str(batch_result)
        logger.info(status_message, extra=self.execution_context.get_context())
        batch = batch_result[0]['batch_id']
        return batch

    def main(self, input_dict):
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for File withdraw utility"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.file_withdraw(input_dict)
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function for File withdraw utility"
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


def check_mandatory_parameters_exists(input_dict):
    """
    Purpose   :   This method is used to check if all the necessary parameters exist in the passed input dictionary .
    Input     :   Input dictionary containing all the necessary arguments.
    Output    :   Returns True if all the variables exist in the required format
    """
    try:
        flag = True

        if (input_dict["file_id"] is None and input_dict['batch_id'] is None) and (input_dict["cycle_id"] is None):
            has_incompatible_arg = True
            if has_incompatible_arg:
                status_message = "All inputs cannot be None"
                logger.error(status_message + str(traceback.format_exc()))
                raise ValueError("All inputs cannot be None")
        return flag

    except Exception as exception:
        logger.error(str(traceback.format_exc()), extra=self.execution_context.get_context())
        raise exception


def get_commandline_arguments():
    """
    Purpose   :   This method is used to get all the commandline arguments .
    Input     :   Console Input
    Output    :   Dictionary of all the command line arguments.
    """
    parser = argparse.ArgumentParser(prog=MODULE_NAME, usage='%(prog)s [command line parameters]',
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=textwrap.dedent('''
    command line parameters :

    -f, --file_id : File Id

    OR

    -b, --batch_id : Batch Id

    OR

   -f, --file_id : File Id
   -b, --batch_id :Batch Id

   OR

   -c, --cycle_id : Cycle Id


    command : python FileWithdrawUtility.py -f <file_ids>
              python FileWithdrawUtility.py -b <batch_ids>
              python FileWithdrawUtility.py -f <file_ids> -b <batch_ids>
              python FileWithdrawUtility.py -c <cycle_ids>


    The list of command line parameters valid for %(prog)s module.'''),
                                     epilog="The module will exit now!!")

    # List of Mandatory Parameters
    parser.add_argument('-f', '--file_id', required=False,
                        help='Specify comma separated file_ids file_id name using -f/--file_id')

    parser.add_argument('-b', '--batch_id', required=False,
                        help='Specify comma separated batch_ids using -b/--batch_id')

    parser.add_argument('-c', '--cycle_id', required=False,
                        help='Specify comma separated cycle_ids using -c/--cycle_id')

    cmd_arguments = vars(parser.parse_args())
    return cmd_arguments


if __name__ == '__main__':
    try:
        commandline_arguments = get_commandline_arguments()
        if check_mandatory_parameters_exists(commandline_arguments):
            file_withdraw_obj = FileWithdrawUtility()
            result_dict = file_withdraw_obj.main(input_dict=commandline_arguments)
        else:
            logger.error(str(traceback.format_exc()))
            raise Exception

        STATUS_MSG = "\nCompleted execution for File Withdraw Utility with status " + json.dumps(result_dict) + "\n"
        sys.stdout.write(STATUS_MSG)
    except Exception as exception:
        message = "Error occurred in main Error : %s\nTraceback : %s" % (str(exception), str(traceback.format_exc()))
        raise exception
