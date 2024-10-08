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
from FileTransferUtility import FileTransferUtility
from CommonUtils import CommonUtils
from MySQLConnectionManager import MySQLConnectionManager

sys.path.insert(0, os.getcwd())
# all module level constants are defined here
MODULE_NAME = "ClusterToS3LoadHandler"
PROCESS_NAME = "Copy Files from Hive Tables/HDFS to S3"

"""
Module Name         :   ClusterToS3LoadHandler
Purpose             :   This module will read parameters provided and will trigger FileTransferutility
                        parsing and validating the required parameters
Input Parameters    :   Parameters: hive_table_name or dataset_id or hive_table_name with target_s3_location
                        or hdfs_location with target_s3_location
Output Value        :   Return Dictionary with status(Failed/Success)
Last changed on     :   24th May 2018
Last changed by     :   Pushpendra Singh
Reason for change   :   HDFS to S3 Copy Module development
Pending Development :   Kerberos,overiding s3 access & secret key
"""


class ClusterToS3LoadHandler(object):
    """
    This class is Wrapper for copying files/directory from S3 to Hdfs Location
    """

    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_name": PROCESS_NAME})
        self.env_configs = JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.cluster_mode = self.env_configs.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "cluster_mode"])
        self.audit_db = self.env_configs.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.hadoop_configs = self.env_configs.get_configuration([CommonConstants.HADOOP_OPTIONS_KEY])
        self.s3_distcp_configs = self.env_configs.get_configuration([CommonConstants.S3_DISTCP_OPTIONS_KEY])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def hdfs_to_s3_copy(self, input_dict):
        """
        Purpose   :   This method is used to call data copy utility and also validates input parameters
        Input     :   Input dictionary
        Output    :   Returns dictionary with status(Failed/Success)
        """

        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_RUNNING}
        try:
            status_message = "Executing hdfs_to_s3_copy function"
            logger.info(status_message, extra=self.execution_context.get_context())

            case = -1
            hive_table_name = input_dict["table_name"]
            dataset_id = input_dict["dataset_id"]
            s3_target_location = input_dict["s3_target_location"]
            hdfs_location = input_dict["hdfs_location"]

            if hive_table_name is not None:
                if s3_target_location is not None:
                    case = 2
                else:
                    case = 0
            elif dataset_id is not None:
                case = 1
            elif hdfs_location is not None:
                if s3_target_location is not None:
                    case = 3
                else:
                    case = -1
            else:
                case = -1

            status_message = "For the inputs provided Case value is" + str(case)
            logger.info(status_message, extra=self.execution_context.get_context())

            if case == -1:
                status_message = "Input Parameters does not match any criteria to transfer data"
                logger.error(status_message, extra=self.execution_context.get_context())
                result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED}
                return result_dictionary
            elif case == 0:
                status_message = "Fetching Inputs for case" + str(case)
                logger.info(status_message, extra=self.execution_context.get_context())
                # get table type and fetch the location
                query = "select dataset_type,s3_post_dqm_location,table_s3_path from {audit_db}.{ctl_dataset_information} where table_name ='{table_name}'".format(
                    audit_db=self.audit_db,
                    ctl_dataset_information=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME,
                    table_name=hive_table_name)
                result_set = MySQLConnectionManager().execute_query_mysql(query, False)

                if len(result_set) == 0:
                    raise Exception("Unable to Fetch Details For Case: " + str(case) + "for input" + str(input_dict))

                for result in result_set:
                    dataset_type = result['dataset_type']
                    s3_post_dqm_location = result['s3_post_dqm_location']
                    table_s3_path = result['table_s3_path']

                dataset_type = dataset_type if dataset_type is not "" or dataset_type != "" else None
                dataset_type = dataset_type.strip() if dataset_type is not None else None

                if dataset_type is None or dataset_type == "" or dataset_type == "Null" or dataset_type == "NULL":
                    raise Exception("Table Type Can Not Be Null or Empty")

                if str(dataset_type).lower() == CommonConstants.RAW_DATASET_TYPE:
                    target_location = str(s3_post_dqm_location)
                elif str(dataset_type).lower() == CommonConstants.PROCESSED_DATASET_TYPE:
                    target_location = str(table_s3_path)
                else:
                    raise Exception("Table Type Can Be Raw Or Processed")

                if target_location.startswith('s3://'):
                    target_location = target_location.replace('s3://', 's3a://')

                # get source from fetch_hive_table_location and target we fetched from dataset inforamtion table

                source_location = str(self.fetch_hive_table_location(hive_table_name))

                if not self.validate_source_target_locations(source_location=source_location,
                                                             target_location=target_location):
                    raise Exception("Falied In Validating Source & Target Location")

                source_location = "/" + source_location

            elif case == 1:
                status_message = "Fetching Inputs for case" + str(case)
                logger.info(status_message, extra=self.execution_context.get_context())
                query = "select dataset_type,s3_post_dqm_location,table_s3_path from {audit_db}.{ctl_dataset_information} where dataset_id ={dataset_id}".format(
                    audit_db=self.audit_db,
                    ctl_dataset_information=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME,
                    dataset_id=dataset_id)
                result_set = MySQLConnectionManager().execute_query_mysql(query, False)

                if len(result_set) == 0:
                    raise Exception("Unable to Fetch Details For Case: " + str(case) + "for input" + str(input_dict))

                for result in result_set:
                    dataset_type = result['dataset_type']
                    s3_post_dqm_location = result['s3_post_dqm_location']
                    table_s3_path = result['table_s3_path']

                dataset_type = dataset_type if dataset_type is not "" or dataset_type != "" else None
                dataset_type = dataset_type.strip() if dataset_type is not None else None

                if dataset_type is None or dataset_type == "" or dataset_type == "Null" or dataset_type == "NULL":
                    raise Exception("Table Type Can Not Be Null or Empty")

                if str(dataset_type).lower() == CommonConstants.RAW_DATASET_TYPE:
                    target_location = str(s3_post_dqm_location)
                elif str(dataset_type).lower() == CommonConstants.PROCESSED_DATASET_TYPE:
                    target_location = str(table_s3_path)
                else:
                    raise Exception("Table Type Can Be Raw Or Processed")

                if target_location.startswith('s3://'):
                    target_location = target_location.replace('s3://', 's3a://')

                # get source from fetch_hive_table_location and target we fetched from dataset inforamtion table

                source_location = str(self.fetch_hive_table_location(hive_table_name))

                if not self.validate_source_target_locations(source_location=source_location,
                                                             target_location=target_location):
                    raise Exception("Falied In Validating Source & Target Location")

                source_location = "/" + source_location

            elif case == 2:
                # fetch HDFS table location from hive_table name & s3 path is given

                source_location = str(self.fetch_hive_table_location(hive_table_name))
                target_location = str(s3_target_location)

                if target_location.startswith('s3://'):
                    target_location = target_location.replace('s3://', 's3a://')

                if not self.validate_source_target_locations(source_location=source_location,
                                                             target_location=target_location):
                    raise Exception("Falied In Validating Source & Target Location")


            elif case == 3:

                source_location = str(hdfs_location)
                target_location = str(s3_target_location)

                if target_location.startswith('s3://'):
                    target_location = target_location.replace('s3://', 's3a://')

                if not self.validate_source_target_locations(source_location=source_location,
                                                             target_location=target_location):
                    raise Exception("Falied In Validating Source & Target Location")

            """
            # Will add this feature in next relaese

            status_message = "Fetching Kerberos Details From ENV Config File"
            logger.debug(status_message, extra=self.execution_context.get_context())
            self.kerberos_enabled = self.env_configs.get_configuration(
                [CommonConstants.ENVIRONMENT_CONFIG_FILE, "kerberos_enabled"])
            self.kerberos_principal = self.env_configs.get_configuration(
                [CommonConstants.ENVIRONMENT_CONFIG_FILE, "kerberos_principal"])
            self.keytab_path = self.env_configs.get_configuration(
                [CommonConstants.ENVIRONMENT_CONFIG_FILE, "keytab_path"])


            if self.kerberos_enabled == "Y" or self.kerberos_enabled == "y":
                if self.keytab_path is None or self.keytab_path is "":
                    raise Exception("Keytab Location Is Not Provided")
                if self.kerberos_principal is None or self.kerberos_principal is "":
                    raise Exception("Principal Is Not Provided")
                command = "kinit" + " " + self.kerberos_principal + " " + "-k -t" + " " + self.keytab_path
                execution_status = CommonUtils().execute_shell_command(command)
                if execution_status != True:
                    raise Exception("Failed To Kinit with provided kerberos details")
            else:
                self.keytab_path = None
                self.kerberos_principal = None
            """

            status_message = "Calling Utility With Following Inputs" + "Source Location:" + str(source_location)
            logger.info(status_message, extra=self.execution_context.get_context())

            FileTransferUtility().transfer_files(source_location=source_location, target_location=target_location,
                                                 hadoop_configs=self.hadoop_configs,
                                                 s3_distcp_configs=self.s3_distcp_configs,
                                                 cluster_mode=self.cluster_mode)

            result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
            return result_dictionary
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    def validate_source_target_locations(self, source_location=None, target_location=None):
        """
        Purpose   :   This method is validate source and target location
        Input     :   source and target location(both are mandatory)
        Output    :   Returns True/False
        """
        try:
            status_message = "Executing validate_source_target_locations function"
            logger.debug(status_message, extra=self.execution_context.get_context())
            flag = True

            if source_location is None or target_location is None:
                status_message = "Source or Target Lcoation Can Not Be None"
                logger.debug(status_message, extra=self.execution_context.get_context())
                flag = False

            if source_location.startswith('s3a://') or source_location.startswith('s3://'):
                status_message = "Source Location Can Not Be S3 Path"
                logger.debug(status_message, extra=self.execution_context.get_context())
                flag = False

            if not target_location.startswith('s3a://'):
                status_message = "Target Location Should Start With S3a://"
                logger.debug(status_message, extra=self.execution_context.get_context())
                flag = False

            return flag
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

    def fetch_hive_table_location(self, table_name=None):
        """
        Purpose   :   This method is used to fetch the hive table location
        Input     :   hive table name  (Mandatory)
        Output    :   Returns hive table location
        """
        conn = None
        try:
            status_message = "Executing fetch_hive_table_location function for table_name : " + str(table_name)
            logger.debug(status_message, extra=self.execution_context.get_context())
            if table_name is None or table_name == "":
                raise Exception("Table Name Can Not be None")

            if "." not in table_name:
                table_name = "default." + str(table_name)

            result_set = CommonUtils().execute_hive_query("DESCRIBE FORMATTED " + table_name)
            for result in result_set:
                if (str(list(result)[0]).strip()) == "Location:":
                    location = (str(str(list(result)[1]).strip().split(":", -1)[2].split("/", 1)[1]).strip()).rstrip(
                        '/')
            return location

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

    def main(self, input_dict):
        result_dictionary = None
        status_message = ""
        try:
            status_message = "Starting the main function for Hdfs To S3 Copy"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary = self.hdfs_to_s3_copy(input_dict)
            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function for Hdfs to S3 Copy"
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

        if input_dict["table_name"]:

            has_incompatible_arg = False

            if input_dict["dataset_id"] is not None or input_dict["hdfs_location"] is not None:
                has_incompatible_arg = True

            if has_incompatible_arg:
                raise ValueError("table_name cannot be used with dataset_id/hdfs_location")

        if input_dict["dataset_id"]:

            has_incompatible_arg = False

            if input_dict["table_name"] is not None or input_dict["hdfs_location"] is not None or input_dict[
                "s3_target_location"] is not None:
                has_incompatible_arg = True

            if has_incompatible_arg:
                raise ValueError("dataset_id cannot be used with table_name/s3_target_location/hdfs_location")

        if input_dict["hdfs_location"]:
            has_incompatible_arg = False

            if input_dict["dataset_id"] is not None or input_dict["table_name"] is not None:
                has_incompatible_arg = True

            if has_incompatible_arg:
                raise ValueError("hdfs_location cannot be used with table_name/dataset_id")

            if not input_dict["s3_target_location"]:
                raise ValueError("The S3 target location does not exist in the specified parameters. " + \
                                 "Kindly make sure you provide the s3 target location with hdfs location.")

        return flag

    except Exception as exception:
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

    -ht, --table_name : Hive Table Name

    OR

    -d, --dataset_id : Dataset Id

    OR

    -ht, --table_name : Hive Table Name
    -t, --s3_target_location : Specify target s3 path

    OR

    -s, --hdfs_location : HDFS Location
    -t, --s3_target_location : Specify target s3 path

    command : python3 ClusterToS3LoadHandler.py -ht <hive_table_name>
              python3 ClusterToS3LoadHandler.py -d <dataset_id>
              python3 ClusterToS3LoadHandler.py -ht <hive_table_name> -t <s3_target_location>
              python3 ClusterToS3LoadHandler.py -s <hdfs_location> -t <s3_target_location>


    The list of command line parameters valid for %(prog)s module.'''),
                                     epilog="The module will exit now!!")

    # List of Mandatory Parameters either provide source,target or provide json_path
    parser.add_argument('-ht', '--table_name', required=False,
                        help='Specify the hive table name using -ht/--table_name')

    parser.add_argument('-d', '--dataset_id', required=False,
                        help='Specify the dataset id using -d/--dataset_id')

    parser.add_argument('-s', '--hdfs_location', required=False,
                        help='Specify the hdfs_location name using -s/--hdfs_location')

    parser.add_argument('-t', '--s3_target_location', required=False,
                        help="Specify the target s3 location using -t/--s3_target_location")

    cmd_arguments = vars(parser.parse_args())
    return cmd_arguments


if __name__ == '__main__':
    try:
        commandline_arguments = get_commandline_arguments()
        if check_mandatory_parameters_exists(commandline_arguments):
            hdfs_s3_load_handler = ClusterToS3LoadHandler()
            result_dict = hdfs_s3_load_handler.main(input_dict=commandline_arguments)
        else:
            raise Exception

        STATUS_MSG = "\nCompleted execution for S3 to HDFS Utility with status " + json.dumps(result_dict) + "\n"
        sys.stdout.write(STATUS_MSG)
    except Exception as exception:
        message = "Error occurred in main Error : %s\nTraceback : %s" % (str(exception), str(traceback.format_exc()))
        raise exception
