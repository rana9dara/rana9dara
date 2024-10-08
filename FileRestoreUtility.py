#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

import os
import traceback
import json
import argparse
import textwrap
import sys
import CommonConstants as CommonConstants
from LogSetup import logger
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager

sys.path.insert(0, os.getcwd())
# all module level constants are defined here
MODULE_NAME = "FileRestoreUtility"
PROCESS_NAME = "Restore"

"""
Module Name         :   FileRestoreUtility
Purpose             :   This module will read parameters provided and restore files/batches updating the log_batch_dtl,log_file_smry,log_file_dtl
Input Parameters    :   Parameters: batch_ids, file_ids
Output Value        :   Return Dictionary with status(Failed/Success)
Last changed on     :   14th Feburary 2019
Last changed by     :   Himanshu Khatri
Reason for change   :   FileRestoreUtility development
Pending Development :
"""


class FileRestoreUtility(object):


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

    def file_restore(self, input_dict):
        """
        Purpose   :   This method validates input parameters and restore files/batches updating the log_batch_dtl,log_file_smry,log_file_dtl
        Input     :   Input dictionary
        Output    :   Returns dictionary with status(Failed/Success)
        """

        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_RUNNING}
        try:
            status_message = "Executing file_restore function"
            logger.info(status_message, extra=self.execution_context.get_context())

            case = -1

            file_id_list=[]
            batch_id_list=[]


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
                    status_message = "WARNING : RESTORATION USING FILE ID IS NOT RECOMMENDED!"
                    logger.info(status_message, extra=self.execution_context.get_context())
                else:
                    raise Exception("Only one of batch_id or file_id should be provided")
            elif batch_ids is not None and cycle_ids is None:
                if file_ids is None:
                    case = 1
            else:
                if cycle_ids is not None and  (file_ids is None and batch_ids is None):
                    case = 2
                else:
                    raise Exception("Only one of batch_id/file_id or cycle_id should be provided")

            status_message = "For the inputs provided Case value is : " + str(case)
            logger.info(status_message, extra=self.execution_context.get_context())

            if case == -1:
                status_message = "Input Parameters do not match any criteria to process file restoration"
                logger.error(status_message, extra=self.execution_context.get_context())
                result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED}
                return result_dictionary
            else:
                failed_flag = False
                if case == 0:
                    # Only file_ids are given
                    logger.info("only file ids provided as inputs", extra=self.execution_context.get_context())
                    for file_id in file_id_list:
                        if not self.check_withrawn_successfull(file_id):
                            logger.debug("No file is withdrawn for this file id , So skipping Restoring", extra=self.execution_context.get_context())
                        else:
                            batch = self.get_batch_id_by_file(file_id)
                            failed_flag = self.call_restore_task(file_id,batch)

                if case == 1:
                    #only batch_ids provided as inputs
                    logger.info("only batch ids provided as inputs", extra=self.execution_context.get_context())
                    for batch_id in batch_id_list:
                        file_list = self.get_file_ids_by_batch(batch_id)
                        for file in file_list:
                            file_id = file['file_id']
                            batch = batch_id
                            if not self.check_withrawn_successfull(file_id):
                                logger.debug("No file is withdrawn for this file id , So skipping Restoring.", extra=self.execution_context.get_context())
                            else:
                                failed_flag = self.call_restore_task(file_id, batch)

                elif case == 2:
                    # only cycle id is provided as inputs

                    logger.info("only cycle ids provided as inputs", extra=self.execution_context.get_context())
                    for cycle_id in cycle_id_list:
                        check_cycle_withdraw_query_str = "select step_status from {audit_db}.{log_step_dtl} where cycle_id = {cycle_id} and step_status = '{step_status}' and lower(step_name) = '{withdraw_process_name}'"
                        check_cycle_withdraw_query = check_cycle_withdraw_query_str.format(audit_db=self.audit_db,
                                                                                           log_step_dtl=CommonConstants.LOG_STEP_DTL,
                                                                                           cycle_id=cycle_id,
                                                                                           step_status=CommonConstants.STATUS_SUCCEEDED,
                                                                                           withdraw_process_name=CommonConstants.WITHDRAW_PROCESS_NAME
                                                                                           )

                        status_message = "check status of Withrawn for this cycle id : " + str(check_cycle_withdraw_query)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        withdrawn_status = MySQLConnectionManager().execute_query_mysql(check_cycle_withdraw_query)
                        if not withdrawn_status:
                            logger.debug("No cycle has been withdrawn for this cycle id ", extra=self.execution_context.get_context())
                            continue
                        cycle_query_str = "update {audit_db}.{log_cycle_dtl}" \
                                          " set cycle_status='{cycle_status}'" \
                                          " where cycle_id = {cycle_id}"
                        cycle_query = cycle_query_str.format(audit_db=self.audit_db,
                                                             log_cycle_dtl=CommonConstants.LOG_CYCLE_DTL,
                                                             cycle_status=CommonConstants.STATUS_FAILED,
                                                             cycle_id=cycle_id
                                                             )
                        status_message = "update log_cycle dtl query : " + str(cycle_query)
                        logger.info(status_message, extra=self.execution_context.get_context())
                        MySQLConnectionManager().execute_query_mysql(cycle_query)


            status_message = "Completed file restore function"
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


    def check_withrawn_successfull(self,file_id):
        """
        Purpose   :   This method checks if the file/batch is withrawn successfull or not
        Input     :   file_id
        Output    :   Returns dictionary with status(Failed/Success)
        """

        withdrawn_success_flag = True
        file_query_str = "select file_process_status from {audit_db}.{process_log_table} " \
                         "where file_id = {file_id} and lower(file_process_name) = '{withdraw_process_name}' " \
                         "and file_process_status = '{withdraw_status}'"
        file_query = file_query_str.format(audit_db=self.audit_db,
                                           process_log_table=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                           file_id=file_id,
                                           withdraw_process_name=CommonConstants.WITHDRAW_PROCESS_NAME,
                                           withdraw_status = CommonConstants.STATUS_SUCCEEDED
                                           )
        status_message = "check file withdrawn in log_file dtl query : " + str(file_query)
        logger.info(status_message, extra=self.execution_context.get_context())
        query_result = MySQLConnectionManager().execute_query_mysql(file_query)

        if not query_result:
            withdrawn_success_flag = False

        return withdrawn_success_flag

    def call_restore_task(self,file_id, batch):

        """
        Purpose   :   This method updates the log_batch_dtl,log_file_smry,log_file_dtl
        Input     :   file_id,batch_id
        Output    :   Returns dictionary with status(Failed/Success)
        """
        failed_flag = False
        file_query_str = "update {audit_db}.{process_log_table} set file_process_status='{file_process_status}' where batch_id={batch_id} and file_id = {file_id} and lower(file_process_name) != '{withdraw_process_name}'"
        file_query = file_query_str.format(audit_db=self.audit_db,
                                           process_log_table=CommonConstants.PROCESS_LOG_TABLE_NAME,
                                           batch_id=batch,
                                           file_process_status=CommonConstants.STATUS_FAILED,
                                           file_id=file_id,
                                           withdraw_process_name=CommonConstants.WITHDRAW_PROCESS_NAME
                                           )
        status_message = "update log_file dtl query : " + str(file_query)
        logger.info(status_message, extra=self.execution_context.get_context())
        query_result = MySQLConnectionManager().execute_query_mysql(file_query)

        file_query_str = "update {audit_db}.{process_summary_table} set file_status='{file_status}' where file_id = {file_id}"
        file_query = file_query_str.format(audit_db=self.audit_db,
                                           process_summary_table=CommonConstants.FILE_AUDIT_TABLE,
                                           file_status=CommonConstants.STATUS_FAILED,
                                           file_id=file_id
                                           )
        status_message = "update log_file_smry query : " + str(file_query)
        logger.info(status_message, extra=self.execution_context.get_context())
        query_result = MySQLConnectionManager().execute_query_mysql(file_query)

        file_query_str = "update {audit_db}.{batch_details_table} set batch_status='{batch_status}' where batch_id = {batch_id}"
        file_query = file_query_str.format(audit_db=self.audit_db,
                                           batch_details_table=CommonConstants.BATCH_TABLE,
                                           batch_id=batch,
                                           batch_status=CommonConstants.STATUS_FAILED,
                                           )
        status_message = "update log_batch_dtl query : " + str(file_query)
        logger.info(status_message, extra=self.execution_context.get_context())
        query_result = MySQLConnectionManager().execute_query_mysql(file_query)
        return failed_flag

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
        if not file_result:
            status_message = "No files found for this particular batch id "
            logger.error(status_message, extra=self.execution_context.get_context())
            raise Exception
        # logger.info("File Ids ",str(file_result[0]['file_id']))
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
        if not batch_result:
            status_message = "No batches found for this particular file id "
            logger.error(status_message,extra=self.execution_context.get_context())
            raise Exception

        batch = batch_result[0]['batch_id']

        return batch

    def main(self, input_dict):
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for File restore utility"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.file_restore(input_dict)
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function for File Restore utility"
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


    def check_mandatory_parameters_exists(self,input_dict):
        """
        Purpose   :   This method is used to check if all the necessary parameters exist in the passed input dictionary .
        Input     :   Input dictionary containing all the necessary arguments.
        Output    :   Returns True if all the variables exist in the required format
        """
        try:
            flag = True
        # print(self.execution_context)
            if (input_dict["file_id"] is None and input_dict['batch_id'] is None) and input_dict["cycle_id"] is None :
                has_incompatible_arg = True
                if has_incompatible_arg:
                    status_message = '''All inputs cannot be None : command : python FileRestoreUtility.py -f <file_ids>
                                                                              python FileRestoreUtility.py -b <batch_ids>
                                                                              python FileRestoreUtility.py -c <cycle_ids>
                    '''
                    logger.error(status_message,extra=self.execution_context.get_context())
                    raise ValueError("All inputs cannot be None")
            return flag

        except Exception as exception:
            logger.error(str(traceback.format_exc()), extra= self.execution_context.get_context())
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
    -f, --file_id : File Id

    OR

    -b, --batch_id : Batch Id

    OR

    -c, --cycle_id : Cycle Id


    command : python FileRestoreUtility.py -f <file_ids>
              python FileRestoreUtility.py -b <batch_ids>
              python FileRestoreUtility.py -c <cycle_ids>


    The list of command line parameters valid for %(prog)s module.'''),
                                     epilog="The module will exit now!!")

    # List of Mandatory Parameters
    parser.add_argument('-f', '--file_id', required=False,
                        help='Specify comma separated file_ids file_id name using -f/--file_id')

    parser.add_argument('-b', '--batch_id', required=False,
                        help='Specify comma separated batch_ids using -b/--batch_id')

    parser.add_argument('-c', '--cycle_id', required=False,
                        help='Specify comma separated batch_ids using -c/--cycle_id')

    cmd_arguments = vars(parser.parse_args())
    return cmd_arguments


if __name__ == '__main__':
    try:
        commandline_arguments = get_commandline_arguments()
        file_restore_obj = FileRestoreUtility()
        if file_restore_obj.check_mandatory_parameters_exists(commandline_arguments):
            result_dict = file_restore_obj.main(input_dict=commandline_arguments)
        else:
            logger.error(str(traceback.format_exc()))
            raise Exception

        STATUS_MSG = "\nCompleted execution for File Restore Utility with status " + json.dumps(result_dict) + "\n"
        sys.stdout.write(STATUS_MSG)
    except Exception as exception:
        message = "Error occurred in main Error : %s\nTraceback : %s" % (str(exception), str(traceback.format_exc()))
        raise exception
