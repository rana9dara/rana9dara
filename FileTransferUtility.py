"""
Module Name         :   FileTransferUtility
Purpose             :   This module create the hadoop distcp command based on the inputs
                        provided and triggers the command
Input Parameters    :   Required Parameters: source_location, target_location
Output Value        :   Return Dictionary with status(True/False)
Pre-requisites      :   Provide Mandatory Parameters either provide source & target location
                        other parameters are optional
Last changed on     :   22nd Jan 2019
Last changed by     :   Nishant Nijaguna
Reason for change   :   Python2 to 3
"""
#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

import traceback
import sys
from LogSetup import logger
from ExecutionContext import ExecutionContext
from CommonUtils import CommonUtils
import CommonConstants as CommonConstants

MODULE_NAME = "FileTransferUtility"


class FileTransferUtility(object):
    """
        This class Utility for copying files/directory from S3 to Hdfs Location or vice versa
    """

    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def transfer_files(self, source_location=None, target_location=None, data_movement_type=None,
                       aws_access_key=None, aws_secret_key=None, hadoop_configs=None,
                       s3_distcp_configs=None, cluster_mode=None, source_pattern=None):
        """
        Purpose   :   This method is used to copy files from one(s3/hdfs) location to other(s3/hdfs)
                      location.
        Input     :   1.source location (Parent location containing all the files/directories to be
                      copied
                      2.target location (Where files are to be copied)
                      3.data_movement_type : override/append
                      4.aws_access_key : aws access key
                      5.aws_secret_key: aws secret key
                      6.hadoop_configs: other hadoop level configurations refer json -
                        hadoop_options_config.json
                      7.cluster_mode: Provide EMR/Cloudera to process copy using s3-disct-cp or
                        hadoop distcp
        Output    :   Returns the execution status for the hadoop_dist ot s3-dist-cp command
        """

        method_name = ""
        try:
            method_name = sys._getframe().f_code.co_name
            status_message = "Started Function %s to copy files from %s to %s." % \
                             (str(method_name), str(source_location), str(target_location))
            logger.info(status_message, extra=self.execution_context.get_context())

            if source_location is None or target_location is None:
                raise Exception("Source or Target Location Can Not be None")

            if cluster_mode is None:
                cluster_mode = "EMR"

            if cluster_mode != "EMR":

                if aws_access_key is not None and aws_secret_key is not None:
                    command = "hadoop distcp -Dfs.s3a.awsAccessKeyId='" + aws_access_key + \
                              "' -Dfs.s3a.secret.key='" + aws_secret_key + "' "
                    command_to_be_displayed = "hadoop distcp -Dfs.s3a.awsAccessKeyId='" + \
                                              "*" * 20 + "' -Dfs.s3a.secret.key='" + "*" * 30 + "' "
                else:
                    command = "hadoop distcp"
                    command_to_be_displayed = "hadoop distcp"

                if data_movement_type is None:
                    distcp_options_command = "-overwrite"
                elif data_movement_type == "overwrite":
                    distcp_options_command = "-overwrite"
                elif data_movement_type == "append":
                    #append works only with update
                    distcp_options_command = "-update -append"

                if hadoop_configs:

                    for key in hadoop_configs:
                        if key.startswith('-D'):
                            hadoop_configs[key[2:]] = hadoop_configs.pop(key)

                    access_key_id = "fs.s3a.awsAccessKeyId"
                    secret_key_id = "fs.s3a.secret.key"
                    for k in list(hadoop_configs.keys()):
                        if k == access_key_id or k == secret_key_id:
                            del hadoop_configs[k]

                    query_part_list = ["-D" + key + "=" + hadoop_configs[key]
                                       if hadoop_configs[key] else "" for key in hadoop_configs]

                    key_value_options_command = " ".join(query_part_list)
                else:
                    key_value_options_command = ""

                command = command + " " + distcp_options_command + " " + source_location + " "\
                          + target_location+ " " + key_value_options_command
                command_to_be_displayed = \
                    command_to_be_displayed + " " + key_value_options_command + " " +\
                    distcp_options_command + " " + source_location + " " + target_location

                status_message = "Command to be executed : " + str(command_to_be_displayed)
                logger.debug(status_message, extra=self.execution_context.get_context())
                execution_status = CommonUtils().execute_restricted_shell_command(command)

            else:
                command = CommonConstants.S3_DIST_CP_BIN_PATH+ "s3-dist-cp"

                if source_pattern is not None:
                    source_pattern_option = " --srcPattern="+str(source_pattern)
                else:
                    source_pattern_option = ""

                if s3_distcp_configs:

                    for key in list(s3_distcp_configs.keys()):
                        if key == "srcPattern":
                            del s3_distcp_configs[key]

                    for key in s3_distcp_configs:
                        query_part_list = ["--" + key + "=" + s3_distcp_configs[key]
                                           if s3_distcp_configs[key] else "" for key
                                           in s3_distcp_configs]
                        key_value_options_command = " ".join(query_part_list)
                else:
                    key_value_options_command = ""

                command = command + " --src=" + source_location + " --dest=" + target_location+\
                          " " + key_value_options_command + source_pattern_option
                command_to_be_displayed = command

                status_message = "Command to be executed : " + str(command_to_be_displayed)
                logger.debug(status_message, extra=self.execution_context.get_context())
                execution_status = CommonUtils().execute_restricted_shell_command(command)

            return execution_status
        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(error, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception


if __name__ == '__main__':
    try:
        source_location = sys.argv[1]
        target_location = sys.argv[2]
        if source_location is not None and target_location is not None:
            obj = FileTransferUtility()
            obj.transfer_files(source_location, target_location)
        else:
            raise Exception
    except Exception as e:
        raise Exception(str(e))
