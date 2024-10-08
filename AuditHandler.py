#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   AuditHandler
#  Purpose             :
#  Input Parameters    :
#  Output Value        :
#  Pre-requisites      :
#  Last changed on     :   6th June 2018
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
from ParquetUtility import ParquetUtility
from ConfigUtility import JsonConfigUtility

# all module level constants are defined here
MODULE_NAME = "Audit Handler"
PROCESS_NAME = "Update audit status after completion of all steps"

USAGE_STRING = """
SYNOPSIS
    python AuditHandler.py <file_master_id> <batch_id>

    Where
        input parameters : file_master_id, batch_id

"""


class AuditHandler:
    # Default constructor
    def __init__(self, file_master_id, cluster_id, workflow_id, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"dataset_id": file_master_id})
        self.execution_context.set_context({"cluster_id": cluster_id})
        self.execution_context.set_context({"workflow_id": workflow_id})
        self.configuration = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # ########################################### execute_command ######################################################
    # Purpose            :   Executing the file audit update status
    # Input              :   data source, subject area
    # Output             :   NA
    # ##################################################################################################################
    def update_audit_status_to_success(self, batch_id=None, batch_id_list=None):
        status_message = ""
        curr_batch_id = batch_id
        log_info = {}
        audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        try:
            status_message = "Updating the audit status"
            logger.debug(status_message, extra=self.execution_context.get_context())

            # Input Validations
            if batch_id is None:
                raise Exception("Batch ID for current batch is invalid")
            if batch_id_list is None:
                raise Exception("Batch ID List is invalid")
            self.execution_context.set_context({"batch_id": batch_id})
            logger.debug(status_message, extra=self.execution_context.get_context())
            log_info['batch_id'] = curr_batch_id
            log_info[CommonConstants.PROCESS_STATUS] = CommonConstants.STATUS_SUCCEEDED
            log_info['status_id'] = CommonConstants.SUCCESS_ID
            CommonUtils().update_file_audit_status_by_batch_id(audit_db, log_info)
            CommonUtils().update_batch_status(audit_db, log_info)

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
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
                    " ERROR MESSAGE: " + str(e) + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            raise e



if __name__ == '__main__':
    file_master_id = sys.argv[1]
    batch_id = sys.argv[5]
    cluster_id = sys.argv[2]
    workflow_id = sys.argv[3]
    process_id = sys.argv[4]
    batch_id = str(batch_id).rstrip("\n\r")
    batch_id_list = sys.argv[6]
    audit_handler = AuditHandler(file_master_id, cluster_id, workflow_id)
    audit_handler.update_audit_status_to_success(batch_id, batch_id_list)
    status_msg = "Updated the audit status for batch : " + str(batch_id)

