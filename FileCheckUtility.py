#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'


# ####################################################Module Information################################################
#  Module Name         :   FileCheckUtility
#  Purpose             :   This module will perform the pre-configured steps before invoking FileCheckUtility.py.
#  Input Parameters    :   file_location
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   19th November 2017
#  Last changed by     :   Amal Kurup
#  Reason for change   :   File Check Utility development
# ######################################################################################################################

# Library and external modules declaration
import traceback
import os
import sys
import re
sys.path.insert(0, os.getcwd())
from ExecutionContext import ExecutionContext
from LogSetup import logger
from PySparkUtility import PySparkUtility
import CommonConstants as CommonConstants

# all module level constants are defined here
MODULE_NAME = "FileCheckUtility"


class FileCheckUtility:
    # Default constructor
    def __init__(self, parent_execution_context=None):
        if parent_execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = parent_execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def perform_header_check(self, file_complete_path=None, field_delimiter=None, column_name_list=None, file_format=None, file_name=None):
        status_message = ""
        spark_context = None
        try:
            if not column_name_list:
                raise Exception("Column metadata details are not provided")
            status_message = "Starting function to check the file schema"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())
            # logger.debug("Received column sequence "+ column_name_list ,  extra=self.execution_context.get_context())
            spark_context = PySparkUtility(self.execution_context).get_spark_context(MODULE_NAME,CommonConstants.HADOOP_CONF_PROPERTY_DICT)
            if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                file_df = spark_context.read.format("parquet").load(file_complete_path)

            elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                file_name = file_name.split("/")[-1]
                file_name_path = file_complete_path + "/" + file_name
                file_df = spark_context.read.format('com.crealytics.spark.excel').option('useHeader', 'true').load(
                    file_name_path)

            else:
                if CommonConstants.MULTILINE_READ_FLAG == "Y":
                    file_df = spark_context.read.format("csv").option("multiline", "true").option("header", "true"). \
                        option("delimiter", str(field_delimiter)).load(file_complete_path)

                else:
                    file_df = spark_context.read.format("csv").option("header", "true"). \
                        option("delimiter", str(field_delimiter)).load(file_complete_path)

            file_column_list = file_df.schema.names
            #  Compare schema column list with provided column list
            # added logic to handle case sensitivity
            file_column_list = [re.sub('[^a-zA-Z0-9]', '', x.lower()) for x in file_column_list]
            column_name_list = [re.sub('[^a-zA-Z0-9]', '', x.lower()) for x in column_name_list]

            if file_column_list == column_name_list:
                status = True
            else:
                status = False

            status_message = "Completed file header check"
            self.execution_context.set_context({"function_status": 'COMPLETED'})
            logger.debug(status_message, extra=self.execution_context.get_context())
            return status
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e

    def perform_field_count_check(self, file_complete_path=None, field_delimiter=None, field_count=None, file_format=None, file_name=None):
        status_message = ""
        spark_context = None
        try:
            if not field_count:
                raise Exception("Field count is not provided")
            status_message = "Starting function to check the field count"
            self.execution_context.set_context({"function_status": 'STARTED'})
            logger.debug(status_message, extra=self.execution_context.get_context())
            # logger.debug("Received column sequence "+ column_name_list ,  extra=self.execution_context.get_context())
            spark_context = PySparkUtility(self.execution_context).get_spark_context(MODULE_NAME,CommonConstants.HADOOP_CONF_PROPERTY_DICT)
            if (file_format is not None) and (file_format.lower() == CommonConstants.PARQUET):
                file_df = spark_context.read.format("parquet").load(file_complete_path)

            elif (file_format is not None) and (file_format.lower() == CommonConstants.EXCEL_FORMAT):
                file_name = file_name.split("/")[-1]
                file_name_path = str(file_complete_path) + "/" + str(file_name)
                file_df = spark_context.read.format('com.crealytics.spark.excel').option('useHeader', 'true').load(
                    file_name_path)

            else:
                if CommonConstants.MULTILINE_READ_FLAG == "Y":
                    file_df = spark_context.read.format("csv").option("multiline","true"). \
                        option("delimiter", str(field_delimiter)).load(file_complete_path)

                else:
                    file_df = spark_context.read.format("csv"). \
                        option("delimiter", str(field_delimiter)).load(file_complete_path)

            field_count_in_file = len(file_df.columns)
            # logger.debug("File Column list "+ file_column_list ,  extra=self.execution_context.get_context())

            #  Compare schema column list with provided column list
            if field_count_in_file == field_count:
                status = True
            else:
                status = False

            status_message = "Completed field count check"
            self.execution_context.set_context({"function_status": 'COMPLETED'})
            logger.debug(status_message, extra=self.execution_context.get_context())
            return status
        except Exception as e:
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise e
