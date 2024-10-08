"""Module for terminating long running clusters."""
#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'
# ####################################################Module Information#############################
#   Module Name         :   AutoTerminateWrapper
#   Purpose             :   Module for terminating long running EMR clusters.
#   Input Parameters    :   idle_timeout (optional), cluster_id_list (optional), aws_region (optional),
#                           config_file_path (optional)
#   Output Value        :   NA
#   Execution Steps     :   1. Trigger this module by passing any one of these inputs -
#                                a. idle_timeout, cluster_id_list, aws_region
#                                b. config_file_path
#                                     config_file structure -
#                                         {
#                                           "Include": [
#                                             {
#                                               "ZS_Project_Code": "9902ZS3747"
#                                             },
#                                                 {
#                                               "ZS_Project_Code": "9902ZS374"
#                                             }
#                                           ],
#                                           "Exclude": {
#                                             "ClusterId": ["j-FITDPVBZO1U3", "j-2Q7PB1HB9W7DX"],
#                                             "Tags": []
#                                           },
#                                           "aws_region": "us-east-1",
#                                           "IdleTimeOut": 60
#                                         }
#                                     Description -
#                                       'Include' -> All clusters will these tags will be considered for termination
#                                       'Exclude' -> All clusters with the mentioned ids or tags will be excluded from
#                                                    termination process
#   Predecessor module  :
#   Successor module    :       NA
#   Pre-requisites      :   The code should be deployed along with all other CommonComponents code modules
#   Last changed on     :   01st Jan 2019
#   Last changed by     :   Shashwat Shukla
#   Reason for change   :   Remove hard coded values from code
#
# ###################################################################################################

import sys
import os
import boto3
import time
import json
import requests
import pandas as pd
import datetime
import traceback
import argparse
import copy
import CommonConstants as CommonConstants
from LogSetup import logger
from ExecutionContext import ExecutionContext
from MySQLConnectionManager import MySQLConnectionManager
from Terminate_EMR_Cluster import Terminate_EMR
from CommonUtils import CommonUtils
from ConfigUtility import JsonConfigUtility
from NotificationUtility import NotificationUtility


# constants
STATUS_KEY = "status"
RESULT_KEY = "result"
ERROR_KEY = "error"
SUCCESS_STATUS = {STATUS_KEY: "SUCCESS", RESULT_KEY: "", ERROR_KEY: ""}
FAILED_STATUS = {STATUS_KEY: "FAILED", RESULT_KEY: "", ERROR_KEY: ""}
DEFAULT_IDLE_THRESHOLD = 60
CLUSTER_TERMINATION_WARNING_WINDOW = 15
INSTANCE_GROUP_TYPES_LIST = ["MASTER"]
INVALID_CLUSTER_RESPONSE = "Invalid cluster ID - {cluster_id}"
MODULE_NAME = "Terminate EMR"

TERMINATION_WARNING_TEMPLATE = """
<p>Hi Team,</p>
<p>The following clusters will be terminated in <strong><span style="color: #ff0000;"> $$termination_window$$ minute(s)</span></strong>.</p>
<table style="border-color: black; height: 30px; margin-left: auto; margin-right: auto;" width="497" border="1">
<tbody>
<tr>
<th style="width: 228px; text-align: center;">cluster_id</th>
<th style="width: 254.4px; text-align: center;">cluster_name</th>
</tr>
$$cluster_details$$
</tbody>
</table>
<p>If any of the clusters need to be stopped from termination, make an entry for the cluster_id in the config file available at <em>$$config_file_path$$</em></p>
<p>Thanks,</p>
<p>CC Team.</p>
"""

ORPHAN_CLUSTER_TERMINATION_NOTIFICATION = """
<p>Hi Team,</p>
<p>The following orphan clusters have been terminated. Please try to avoid launching cluster from AWS Console.</p>
<table style="border-color: black; height: 30px; margin-left: auto; margin-right: auto;" width="497" border="1">
<tbody>
<tr>
<th style="width: 228px; text-align: center;">cluster_id</th>
<th style="width: 254.4px; text-align: center;">cluster_name</th>
</tr>
$$cluster_details$$
</tbody>
</table>
<p>Thanks,</p>
<p>CC Team.</p>
"""

LONG_RUNNING_CLUSTERS_NOTIFICATION = """
<p>Hi Team,</p>
<p>The following long running clusters could not be terminated. Please terminate the clusters manually to avoid extra costs.</p>
<table style="border-color: black; height: 30px; margin-left: auto; margin-right: auto;" width="497" border="1">
<tbody>
<tr>
<th style="width: 228px; text-align: center;">cluster_id</th>
<th style="width: 254.4px; text-align: center;">cluster_name</th>
</tr>
$$cluster_details$$
</tbody>
</table>
<p>&nbsp;</p>
<p>Thanks,</p>
<p>CC Team.</p>
"""

CLUSTER_INFO_HTML_TEMPLATE = """<tr><td style="text-align:center">$$cluster_id$$</td><td style="text-align:center">$$cluster_name$$</td></tr>"""


class AutoTerminate(object):

    def __init__(self):

        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(
            os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                         CommonConstants.ENVIRONMENT_CONFIG_FILE))
        self.audit_db = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.email_subject = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations", "auto_terminate", "subject"])
        self.ses_sender = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations", "auto_terminate", "ses_sender"])
        self.ses_recipient_list = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations",
             "auto_terminate", "ses_recipient_list"])

    def terminate_running_cluster(self, cluster_id, aws_region):
        # terminate the EMR
        try:
            client = boto3.client("emr", region_name=aws_region)
            # turn off termination protection
            client.set_termination_protection(JobFlowIds=[cluster_id], TerminationProtected=False)
            # terminate the EMR
            try:
                response = client.terminate_job_flows(JobFlowIds=[cluster_id])
            except Exception as e:
                status_message = "Failed to terminate the EMR"
                logger.debug(status_message, extra=self.execution_context.get_context())
                return False

            status_message = "In Check EMR Status for EMR ID: " + cluster_id
            logger.info(status_message, extra=self.execution_context.get_context())

            query_string = "select cluster_id from {audit_db_name}.{log_cluster_dtl} " \
                           "where cluster_status !='TERMINATED' and cluster_id='{cluster_id}'"
            execute_query = query_string.format(audit_db_name=self.audit_db,
                                                log_cluster_dtl=
                                                CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
                                                cluster_id=str(cluster_id))
            # check if there are any running emrs
            running_emr = MySQLConnectionManager().execute_query_mysql(execute_query, False)
            # To check Running EMRs
            if len(running_emr) == 0:
                status_message = "No update required in log table"
                logger.info(status_message, extra=self.execution_context.get_context())
            else:
                terminate_emr_cluster = Terminate_EMR()
                terminate_emr_cluster.terminate_cluster(cluster_id, aws_region)
                # update emr termination status in log tables
                try:
                    CommonUtils().update_emr_termination_status(cluster_id, self.audit_db)
                except Exception as e:
                    status_message = "*********Failed to update the EMR termination status in log table***********"
                    logger.debug(status_message, extra=self.execution_context.get_context())
            return True
        except Exception as e:
            return False

    def is_cluster_idle(self, cluster_id, aws_region, idle_threshold):
        status_message = "Received request for cluster id: {id}".format(id=cluster_id)
        logger.debug(status_message, extra=self.execution_context.get_context())
        # get IP from cluster ID
        client = boto3.client("emr", region_name=aws_region)
        try:
            response = client.list_instances(
                ClusterId=cluster_id,
                InstanceGroupTypes=INSTANCE_GROUP_TYPES_LIST,
                InstanceStates=['RUNNING']
            )
        except Exception as e:
            print((INVALID_CLUSTER_RESPONSE.format(cluster_id=cluster_id)))
            return False
        if "Instances" not in response or len(response["Instances"]) == 0 or "PrivateIpAddress" not in \
                response["Instances"][0]:
            error = "Failed to fetch cluster IP for the cluster ID - {cluster_id}. Please check if it is running". \
                format(cluster_id=cluster_id)
            return False

        # get cluster ip
        cluster_ip = str(response["Instances"][0]["PrivateIpAddress"])

        cluster_apps_url = "http://{cluster_ip}:8088/ws/v1/cluster/apps".format(cluster_ip=cluster_ip)
        response = requests.request("GET", cluster_apps_url)
        # print response.json()

        if not response.ok:
            status_message = "Failed to fetch details from resource manager!"
            logger.debug(status_message, extra=self.execution_context.get_context())
            return False

        if response.json()["apps"] is None:
            status_message = "No jobs have been executed on this EMR, using cluster launch as the last recorded job..."
            logger.debug(status_message, extra=self.execution_context.get_context())
            cluster_info_url = "http://{cluster_ip}:8088/ws/v1/cluster/info".format(cluster_ip=cluster_ip)
            cluster_info_response = requests.request("GET", cluster_info_url)
            # print cluster_info_response.json()

            if not cluster_info_response.ok:
                status_message = "Failed to fetch details from resource manager!"
                logger.debug(status_message, extra=self.execution_context.get_context())
                return False
            latest_job_end_ts = int(cluster_info_response.json()["clusterInfo"]["startedOn"])

        else:

            app_details_df = pd.read_json(json.dumps(response.json()["apps"]["app"]))

            app_details_columns_df = app_details_df[['id', 'state', 'startedTime', 'finishedTime']]

            # check for jobs in accepted and running state
            running_apps_df = app_details_columns_df[
                (app_details_df["state"] == "ACCEPTED") | (app_details_df["state"] == "RUNNING")]

            # if running jobs are found, skip emr termination
            if len(running_apps_df) > 0:
                status_message = "EMR is currently running jobs. Not terminating!"
                logger.debug(status_message, extra=self.execution_context.get_context())
                return False

            # sort in desc on finishedTime
            app_details_columns_df = app_details_columns_df.sort_values(["finishedTime"], ascending=[0])

            # convert to list
            app_details_columns_df_values = app_details_columns_df[["finishedTime"]].values.flatten()
            app_details_columns_values_list = app_details_columns_df_values.tolist()

            # use latest value for timestamp
            latest_job_end_ts = app_details_columns_values_list[0]

        # remove milliseconds from time stamp
        latest_job_end_ts = latest_job_end_ts / 1000

        # current time
        current_time = int(time.time())

        # terminate EMR if it has been idle for too long
        if int(current_time - latest_job_end_ts) > int(idle_threshold * 60):
            status_message = "Idle time: {minutes} minute(s).\nIdle threshold: {threshold} minute(s).\nTerminating EMR!:{cluster_id}".format(
                minutes=str(int(current_time - latest_job_end_ts) / 60), threshold=idle_threshold,
                cluster_id=cluster_id)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return True

        else:
            status_message = "Idle time: {minutes} minute(s).\nIdle threshold:{threshold} minute(s).\nNot terminating EMR!".format(
                minutes=str(int(current_time - latest_job_end_ts) / 60), threshold=idle_threshold)
            logger.debug(status_message, extra=self.execution_context.get_context())
            return False

    def validate_clusters(self, cluster_detail_dict, config_file):
        valid_cluster_dict = {}
        check = True
        # creating the list of clusters and appending it to cluster_id_list after comparing to the tags in configfile
        for cluster_id in list(cluster_detail_dict.keys()):
            cluster_tags = cluster_detail_dict[cluster_id]['cluster_tags']
            try:
                # invalid config file
                if type(config_file) == dict:
                    # cluster id present in exclude list
                    if 'Exclude' in config_file and 'ClusterId' in config_file['Exclude'] and \
                                    cluster_id in config_file['Exclude']['ClusterId']:
                        continue
                    # checking 'include' tags
                    if 'Include' in config_file and len(config_file['Include']) != 0:
                        for include_tags in config_file['Include']:
                            key = []
                            value = []
                            for key_tag_include, value_tag_include in list(include_tags.items()):
                                ##############################################
                                key.append(key_tag_include)
                                value.append(value_tag_include)
                                for cluster_tags_include in cluster_tags:
                                    # check if cluster tag matches with tags in 'include' list
                                    condition_flag = True
                                    for key_item, value_item in zip(key, value):
                                        if condition_flag and (cluster_tags_include['Key'] == key_item) and \
                                                (cluster_tags_include['Value'] == value_item):
                                            continue
                                        else:
                                            condition_flag = False
                                    if condition_flag:
                                        valid_cluster_dict[cluster_id] = copy.deepcopy(cluster_detail_dict[cluster_id])
                                        ######################################
                    if 'Exclude' in config_file and len(config_file['Exclude']['Tags']) != 0:
                        flag = 'Terminate'
                        for exclude_tags in config_file['Exclude']['Tags']:
                            for key_tag_exclude, value_tag_exclude in list(exclude_tags.items()):
                                key = key_tag_exclude
                                value = value_tag_exclude
                                for cluster_tags_exclude in cluster_tags:
                                    if cluster_tags_exclude['Key'] == key and cluster_tags_exclude['Value'] == value:
                                        flag = 'Not_Terminate'
                                        break
                                if flag == 'Not_Terminate':
                                    status_message = "EXCLUDED: {cluster_id}".format(cluster_id=cluster_id)
                                    break
                            if flag == 'Not_Terminate':
                                # remove cluster from dict if tags match with 'exclude' tags
                                try:
                                    del valid_cluster_dict[cluster_id]
                                except Exception as e:
                                    pass
                                break
                            else:
                                valid_cluster_dict[cluster_id] = copy.deepcopy(cluster_detail_dict[cluster_id])
                else:
                    check = False
            except:
                status_message = "Failed while validating cluster tags"
                pass
        return check, valid_cluster_dict

    def get_cluster_list(self, config_file):
        client = boto3.client('emr')

        # get details of all clusters that is in the respective ClusterStates
        response_cluster_details = client.list_clusters(ClusterStates=['RUNNING', 'WAITING'])
        logger.info("List of Cluster Details : " + str(response_cluster_details),
                    extra=self.execution_context.get_context())
        # setting the idle time out
        if type(config_file['IdleTimeOut']) == int:
            if 'IdleTimeOut' in config_file and config_file['IdleTimeOut'] != '':
                idle_time = config_file['IdleTimeOut']
            else:
                idle_time = 60
        else:
            idle_time = 60
            status_message = "+++++++++Idle Time given is not an integer: Using Default TimeOut Value of 60min+++++++++++"
            logger.debug(status_message, extra=self.execution_context.get_context())
        # get cluster tags
        cluster_details_dict = {}
        for cluster_detail in response_cluster_details['Clusters']:
            cluster_id = cluster_detail['Id']
            try:
                response = client.describe_cluster(ClusterId=cluster_id)
                cluster_detail_item = {"cluster_id": cluster_id, "cluster_tags": response['Cluster']['Tags']}
                cluster_details_dict[cluster_id] = response['Cluster']['Tags']
                cluster_details_dict[cluster_id] = {"cluster_tags": response['Cluster']['Tags'], "cluster_name": cluster_detail['Name']}
                # cluster_details_list.append(cluster_detail_item)
            except Exception as e:
                status_message = "No access to describe this cluster ID: {cluster_id}".format(cluster_id=cluster_id)
                logger.debug(status_message, extra=self.execution_context.get_context())
        # print cluster_details_dict
        get_cluster_dict_status, valid_cluster_details_dict = self.validate_clusters(
            cluster_details_dict, config_file)
        aws_region = config_file['aws_region']
        status = "SUCCESS"
        error = ""
        if not get_cluster_dict_status:
            error = "The Configuration file is Invalid"
            status = "FAILED"

        data = {"status": status, "result": {"cluster_details_dict": valid_cluster_details_dict, "Idle_Time_Out": idle_time,
                                             "aws_region": aws_region}, "error": error}

        status_message = "Returning cluster details - {cluster_details}".format(cluster_details=data)
        logger.debug(status_message, extra=self.execution_context.get_context())

        return data

    def send_email_notification(self, email_body=None, replacement_parameters=None, ses_region=None,
                                cluster_type=None):
        status_message = "****************Sending notification****************"
        logger.debug(status_message, extra=self.execution_context.get_context())

        if cluster_type == "orphan":
            email_subject = self.configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations", "orphan_terminate", "subject"])
            ses_sender = self.configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations", "orphan_terminate", "ses_sender"])
            ses_recipient_list = self.configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations",
                 "orphan_terminate", "ses_recipient_list"])
            for param in list(replacement_parameters.keys()):
                email_body = email_body.replace(param, str(replacement_parameters[param]))

            notification_status = NotificationUtility().send_notification(ses_region, email_subject,
                                                                          ses_sender, ses_recipient_list,
                                                                          email_body, "", "")
        else:
            for param in list(replacement_parameters.keys()):
                email_body = email_body.replace(param, str(replacement_parameters[param]))

            notification_status = NotificationUtility().send_notification(ses_region, self.email_subject,
                                                                          self.ses_sender, self.ses_recipient_list,
                                                                          email_body, "", "")
        if notification_status[STATUS_KEY] == "FAILED":
            raise Exception(
                "Failed to send notification. ERROR - {error}".format(error=notification_status[ERROR_KEY]))
        status_message = "****************Sent notification****************"
        logger.debug(status_message, extra=self.execution_context.get_context())

    def get_cluster_details_table_html(self, cluster_dict):
        cluster_info_html = ""
        for cluster_id in list(cluster_dict.keys()):
            cluster_info_html = cluster_info_html + CLUSTER_INFO_HTML_TEMPLATE.replace(
                "$$cluster_id$$", cluster_id).replace("$$cluster_name$$", cluster_dict[cluster_id]["cluster_name"])
        return cluster_info_html

    def is_cluster_orphan(self, cluster_details_dict):
        try:
            cluster_id_tuple = tuple(list(cluster_details_dict.keys()))
            if len(cluster_id_tuple) == 1:
                cluster_id_tuple_str = "('" + str(cluster_id_tuple[0]) + "')"
            else:
                cluster_id_tuple_str = str(tuple(cluster_id_tuple))

            # list all active emr entries in log_cluster_dtl
            get_active_cluster_query = "SELECT cluster_id from {database}.log_cluster_dtl" \
                                       " where cluster_status <> '{cluster_state}' " \
                                       "and cluster_id in" \
                                       " {cluster_ids_tuple}".format(
                database=self.audit_db,
                cluster_state=CommonConstants.CLUSTER_EMR_TERMINATE_STATUS,
                cluster_ids_tuple=cluster_id_tuple_str)
            logger.info("Generated Query : " + get_active_cluster_query,
                        extra=self.execution_context.get_context())

            query_results = \
                MySQLConnectionManager().execute_query_mysql(get_active_cluster_query, False)
            active_cluster_tuple = tuple([cluster["cluster_id"]
                                          for cluster in query_results if "cluster_id" in cluster])

            orphan_emr_list = []
            for cluster_id in list(cluster_details_dict.keys()):
                if cluster_id not in active_cluster_tuple:
                    orphan_emr_list.append(cluster_id)

            logger.info("Final orphan cluster list is " + str(orphan_emr_list),
                        extra=self.execution_context.get_context())

            return orphan_emr_list

        except Exception as err:
            status_message = "No access to describe this cluster ID"
            logger.debug(status_message, extra=self.execution_context.get_context())
            # self.execution_context.set_context({"traceback": str(traceback.format_exc())})
            # logger.error(str(err),
            #              extra=self.execution_context.get_context())
            # self.execution_context.set_context({"traceback": ""})
            # raise err


    def trigger_auto_termination(self, config_file_path=None, cluster_details_dict=None, aws_region=None,
                                 idle_timeout=None):

        # loading and reading the configuration file
        try:
            config_file = json.loads(str(open(config_file_path, 'r').read()))
        except Exception as e:
            raise Exception("Config file must be in a valid JSON format!")

        # get details if input has not been given
        if cluster_details_dict is None or len(cluster_details_dict) == 0:
            json_output = AutoTerminate().get_cluster_list(config_file)
            cluster_details_dict = json_output['result']['cluster_details_dict']
            idle_timeout = json_output['result']['Idle_Time_Out']
            aws_region = json_output['result']['aws_region']

        orphan_cluster_dict = {}
        if config_file['orphan_flag'] == 'Y':
            orphan_emr_list = AutoTerminate().is_cluster_orphan(cluster_details_dict)
            if len(orphan_emr_list) > 0:
                for orphan_emr in orphan_emr_list:
                    logger.info("Terminating Cluster : " + str(orphan_emr),
                                extra=self.execution_context.get_context())
                    orphan_cluster_dict[orphan_emr] = cluster_details_dict[orphan_emr]
                    cluster_info_html = self.get_cluster_details_table_html(orphan_cluster_dict)
                    replacement_dict = {"$$cluster_details$$": cluster_info_html}
                    Terminate_EMR().terminate_cluster(orphan_emr, aws_region)
                AutoTerminate().send_email_notification(email_body = ORPHAN_CLUSTER_TERMINATION_NOTIFICATION,
                                                        replacement_parameters = replacement_dict,
                                                        ses_region = aws_region,
                                                        cluster_type = "orphan")

        if len(cluster_details_dict) > 0:
            idle_cluster_dict = {}

            for cluster_id in list(cluster_details_dict.keys()):
                if AutoTerminate().is_cluster_idle(cluster_id, aws_region, idle_timeout):
                    idle_cluster_dict[cluster_id] = copy.deepcopy(cluster_details_dict[cluster_id])

            if len(idle_cluster_dict) > 0:
                # print idle_cluster_dict
                cluster_info_html = self.get_cluster_details_table_html(idle_cluster_dict)
                # prepare replacement parameters
                replacement_dict = {"$$termination_window$$": CLUSTER_TERMINATION_WARNING_WINDOW,
                                    "$$config_file_path$$": config_file_path, "$$cluster_details$$": cluster_info_html}
                AutoTerminate().send_email_notification(email_body = TERMINATION_WARNING_TEMPLATE, replacement_parameters = replacement_dict, ses_region = aws_region)
                status_message = "Waiting for {interval} minute(s) before EMR termination".format(
                    interval=str(CLUSTER_TERMINATION_WARNING_WINDOW))
                time.sleep(CLUSTER_TERMINATION_WARNING_WINDOW * 60)
                # refresh the config file
                try:
                    config_file = json.loads(str(open(config_file_path, 'r').read()))
                except Exception as e:
                    raise Exception("Config file must be in a valid JSON format!")
                cluster_validation_status, clusters_to_terminate_dict = AutoTerminate().validate_clusters(
                    idle_cluster_dict, config_file)
                status_message = "****************Terminating following clusters****************"
                print(clusters_to_terminate_dict)
                long_running_clusters_dict = {}
                if cluster_validation_status:
                    for cluster_id in list(clusters_to_terminate_dict.keys()):
                        termination_status = self.terminate_running_cluster(cluster_id, aws_region)
                        if not termination_status:
                            long_running_clusters_dict[cluster_id] = copy.deepcopy(clusters_to_terminate_dict[cluster_id])
                if len(long_running_clusters_dict) > 0:
                    cluster_info_html = self.get_cluster_details_table_html(long_running_clusters_dict)
                    replacement_dict = {"$$cluster_details$$": cluster_info_html}
                    self.send_email_notification(email_body=LONG_RUNNING_CLUSTERS_NOTIFICATION, replacement_parameters = replacement_dict, ses_region = aws_region)
                else:
                    status_message = "All long running idle clusters were terminated successfully"
                    logger.info(status_message, extra=self.execution_context.get_context())

            else:
                status_message = "No idle clusters were detected!"
                logger.info(status_message, extra=self.execution_context.get_context())

        else:
            status_message = "None of the running clusters have been configured for automatic termination. Ending process."
            logger.info(status_message, extra=self.execution_context.get_context())



if __name__ == "__main__":

    try:
        # default value
        aws_region = CommonConstants.AWS_REGION
        parser = argparse.ArgumentParser(description="Wrapper to check all active EMRs and terminate the. \
         long running EMRs")
        parser.add_argument("--idle_timeout", help="Time (in minutes) after which an idle EMR should be. \
         terminated", required=False)
        parser.add_argument("--cluster_id_list", help="The list of clusters that has to be terminated. \
        (Separated with commas and no spaces))", required=False)
        parser.add_argument("--aws_region", help="Specifies the aws region name", required=False)
        parser.add_argument("--config_file_path", help="Specifies the path for the config file, which will be used if "
                                                       "none of the other inputs have been provided", required=False)

        args = vars(parser.parse_args())
        flag = 0
        # default value
        config_file_path = ""
        if args['config_file_path'] is not None and args['config_file_path'] != "":
            config_file_path = args['config_file_path']
            flag = 1
        elif args['idle_timeout'] is None and args['cluster_id_list'] is None and args['aws_region'] is None:
            flag = 1
        elif args['idle_timeout'] is None or args['cluster_id_list'] is None or args['aws_region'] is None:
            status_message = "Not sufficient data"
            print(status_message)
            sys.exit(0)
        if flag == 0:
            try:
                idle_timeout = int(args['idle_timeout'])
            except:
                status_message = "Idle TimeOut should be an integer"
                print(status_message)
                sys.exit(0)

            if type(args['cluster_id_list']) != str or type(args['aws_region']) != str:
                status_message = "Invalid input data type"
                print(status_message)
                sys.exit(0)

            idle_timeout = int(args['idle_timeout'])
            aws_region = args['aws_region']
            cluster_details_dict = args['cluster_id_list'].split(',')
            print("Starting automatic cluster termination module with command line arguments")
            AutoTerminate().trigger_auto_termination(cluster_details_dict=cluster_details_dict,
                                                     aws_region=aws_region, idle_timeout=idle_timeout)

        elif config_file_path is None or config_file_path == "":
            raise Exception("Config file path not provided")
        else:
            print(("Starting automatic cluster termination module using configurations from JSON {0}".format(
                config_file_path)))
            AutoTerminate().trigger_auto_termination(config_file_path=config_file_path)

    except Exception as e:
        print((traceback.format_exc(e)))
        raise
