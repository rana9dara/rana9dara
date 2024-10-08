#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

from datetime import datetime
import sys
import os
import json
import logging
import traceback
from LogSetup import logger
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager
from ExecutionContext import ExecutionContext


"""
 Module Name         :   Dag Generator
 Purpose             :   This Wrapper Will Generate Dag
 Input Parameters    :   process_id, dag_template_name
 Output Value        :
 Pre-requisites      :
 Last changed on     :   29 May, 2018
 Last changed by     :   Amal Kurup
 Reason for change   :   To pass dag template as an input argument to Generate DAG python module
"""

MODULE_NAME = "Generate_Dag"
PROCESS_NAME = 'Generate Dag From Template'

logger = logging.getLogger('Generate_dag')
hndlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hndlr.setFormatter(formatter)


class Generate_Dag(object):
    def __init__(self, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(os.path.join(
            CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
        print(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,CommonConstants.ENVIRONMENT_CONFIG_FILE))
        logger.info("System path: " + sys.path[0])
        self.audit_db = self.configuration.get_configuration([
           "EnvironmentParams", "mysql_db"])
        print(self.audit_db)
        print(self.configuration)

    def get_dataset_ids(self, process_id):
        """
        :param process_name: name of the process
        :return: data set id's list
        """
        dataset_id_list = []
        try:

            query = "Select " + CommonConstants.MYSQL_DATASET_ID + " from " + self.audit_db + "." +\
                    CommonConstants.EMR_PROCESS_WORKFLOW_MAP_TABLE + " where " + \
                    CommonConstants.EMR_PROCESS_WORKFLOW_COLUMN + "='" + str(process_id) + "' and "\
                    + CommonConstants.EMR_PROCESS_WORKFLOW_ACTIVE_COLUMN + "='" + \
                    str(CommonConstants.WORKFLOW_DATASET_ACTIVE_VALUE) + "'"
            dataset_ids = MySQLConnectionManager().execute_query_mysql(query, False)
            for id in dataset_ids:
                dataset_id_list.append(str(int(id[CommonConstants.MYSQL_DATASET_ID])))
            return dataset_id_list
        except Exception as exception:
            logger.error(str(exception) + str(traceback.format_exc()), extra=self.execution_context.get_context())
            raise exception

    def get_process_name(self, process_id):
        """
        :param process_name: name of the process
        :return: data set id's list
        """
        try:

            query = "Select " + "process_name" + " from " + self.audit_db + "." \
                    + CommonConstants.EMR_CLUSTER_CONFIGURATION_TABLE + " where " + \
                    CommonConstants.EMR_PROCESS_WORKFLOW_COLUMN + "='" + str(process_id) + "' and "\
                    + CommonConstants.EMR_PROCESS_WORKFLOW_ACTIVE_COLUMN + "='" + \
                    str(CommonConstants.WORKFLOW_DATASET_ACTIVE_VALUE) + "'"
            process_names = MySQLConnectionManager().execute_query_mysql(query, False)
            if process_names:
                return process_names[0]
            else:
                raise Exception(CommonConstants.PROCESS_NOT_FOUND)
        except Exception as exception:
            logger.error(str(exception) + str(traceback.format_exc()), extra=self.execution_context.get_context())
            raise exception

    def get_workflow_name(self, process_id):
        """
        :param process_name: name of the process
        :return: data set id's list
        """
        try:

            query = "Select " + "workflow_name" + " from " + self.audit_db + "." \
                    + CommonConstants.EMR_PROCESS_WORKFLOW_MAP_TABLE + " where " + \
                    CommonConstants.EMR_PROCESS_WORKFLOW_COLUMN + "='" + str(process_id) + "' and "\
                    + CommonConstants.EMR_PROCESS_WORKFLOW_ACTIVE_COLUMN + "='"\
                    + str(CommonConstants.WORKFLOW_DATASET_ACTIVE_VALUE) + "'"
            workflow_names = MySQLConnectionManager().execute_query_mysql(query, False)
            if workflow_names:
                return workflow_names[0]["workflow_name"]
            else:
                raise Exception(CommonConstants.WF_NOT_FOUND)
        except Exception as exception:
            logger.error(str(exception) + str(traceback.format_exc()), extra=self.execution_context.get_context())
            raise exception

    def replace_and_copy_template(self, value, process_id, dag_template_name):
        """
        :param value: data set list
        :return: Generates dag.py file
        """

        try:
            dagname = self.get_workflow_name(process_id) + '_' + str(process_id)
            python_file = os.path.join(CommonConstants.DAGS_FOLDER, dagname) + '.py'
            data_set = "sed -i 's|Dataset_Id_List|" + json.dumps(value) + "|g' " + python_file
            python_scripts_path = "sed -i 's|python_scripts_path|" + \
                                  CommonConstants.AIRFLOW_CODE_PATH + "|g' " + python_file
            dag_name = "sed -i 's|dagname|" + dagname + "|g' " + python_file
            p_id = "sed -i 's|p_id|" + process_id + "|g' " + python_file
            if os.path.exists(CommonConstants.AIRFLOW_CODE_PATH) is True:
                os.system("cd" + " " + CommonConstants.AIRFLOW_CODE_PATH)
                os.system("cp" + " " + str(dag_template_name) + " " + python_file)
                os.system(data_set)
                os.system(p_id)
                os.system(python_scripts_path)
                os.system(dag_name)
                logger.info("Created the dag file successfully")

        except Exception as exception:
            logger.error(str(exception) + str(traceback.format_exc()), extra=self.execution_context.get_context())
            raise exception


def usage():
    """
    :def: USAGE() Function
    :return: prints help statement
    """
    msg = """Usage: python GenerateDag.py <process_id> <dag_template_name> """
    print(msg)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
        raise Exception("Invalid arguments")
    process_id = sys.argv[1]
    dag_template_name = sys.argv[2]

    dag = Generate_Dag()
    dataset_list = dag.get_dataset_ids(process_id)
    dag.replace_and_copy_template(dataset_list, process_id, dag_template_name)
