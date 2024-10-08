#!/usr/bin/python
# -*- coding: utf-8 -*-

import boto3
import traceback
import time
import base64
import ast
import psycopg2
import json
import sys, os
import glob
from datetime import datetime
import getopt
import subprocess
service_directory_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1,service_directory_path)
code_dir_path = os.path.abspath(os.path.join(service_directory_path, "../"))
sys.path.insert(1, code_dir_path)
from CommonUtils import CommonUtils
from ExecutionContext import ExecutionContext
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from DILogSetup import get_logger
logging = get_logger()

APPLICATION_CONFIG_FILE = "application_variables.json"
ENVIRONMENT_CONFIG_FILE = "environment_variable.json"
DEFAULT_POSTGRESQL_PORT = 5432
DEFAULT_AUTOCOMMIT_ENABLED = True
# TODO remove hardcoded value of dir of app config
config_path_dir = "config"
STATUS_KEY = "status"
RESULT_KEY = "result"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCESS"
ERROR_KEY = "error"
WAITING_STATE="WAITING"
TERMINATED_STATE="TERMINATED"
ZS_Project_Name = ""
TAGS_KEY = "Tags"
KEY_IN_TAGS = "Key"
APPLICATION_CONFIG_FILE = "application_config.json"
VALUE_IN_TAGS = "Value"
PROJECT_CODE = "ZS_Project_Code"
CLUSTER_STOP_TIME_KEY = "cluster_stop_time"
MASTER_NODE_DNS_KEY = "master_node_dns"
CLUSTER_NAME_KEY = "cluster_name"
CLUSTER_CREATE_REQUEST_TIME_KEY = "cluster_create_request_time"
PROCESS_ID_KEY = 'process_id'
CLUSTER_ID_KEY = 'cluster_id'
CLUSTER_STATUS_KEY = 'cluster_status'
MODULE_NAME = "EMRManager"
PROCESS_NAME =  "launch_cluster"
CLUSTER_START_TIME_KEY = 'cluster_start_time'
CLUSTER_TERMINATE_USER_KEY = 'cluster_terminate_user'
CLUSTER_LAUNCH_USER_KEY = 'cluster_launch_user'
DEFAULT_PROCESS_ID = "1"
DEFAULT_MASTER_NODE_DNS = ""
SLEEP_TIME = 180

class EmrManagerUtility:
    ######################################################Module Information############################
#   Module Name         :   EMR_Manager_Utility
#   Purpose             :   Launch and Terminate EMR
#   Input Parameters    :   NA
#   Output              :   NA
#   Execution Steps     :   Instantiate object of this class and call the class functions
#   Predecessor module  :   This module is  a generic module
#   Successor module    :   NA
#   Last changed on     :   13th Feb 2019
#   Last changed by     :   Rakhi Darshi
#   Reason for change   :   Initial Development
####################################################################################################


    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.execution_context.set_context({"process_name": PROCESS_NAME})
        self.configuration = json.load(open(APPLICATION_CONFIG_FILE))
        self.audit_db = self.configuration["adapter_details"]["generic_config"][ "mysql_db"]
        pass

    def get_user(self):
        error_message=""
        try:
            command = 'who am i | cut -d " " -f 1'
            user_output_command = subprocess.Popen(command, stdout=subprocess.PIPE,
                                                   stderr=subprocess.PIPE, shell=True)
            command_output, command_error = user_output_command.communicate()
            user_name = command_output.strip()
            logging.debug(user_name)
            return{STATUS_KEY:SUCCESS_KEY,RESULT_KEY:user_name}
        except:
            error_message=str(traceback.format_exc())
            return {STATUS_KEY:FAILED_KEY,ERROR_KEY:error_message}


    def validate(self, event):
        """
            Purpose             :   This module performs below operation:
                                      a. Checks if project code is present in the payload passed
            Input               :   payload
            Output              :   Return status :SUCCESS/FAILED ,error :ERROR THROWN
            """
        logging.info("Starting function validate")
        validate_msg=""
        return {STATUS_KEY:SUCCESS_KEY}
        if type(event)== dict:
            logging.debug("event is dict")
            if TAGS_KEY in event:
                logging.debug("Tags is present in dict event")
                for keys in event[TAGS_KEY]:
                    if(keys.get(KEY_IN_TAGS, "")==PROJECT_CODE):
                        logging.debug("ZS_Project_Code key is present")
                        if not (keys[VALUE_IN_TAGS]==""):
                            if (len(keys[VALUE_IN_TAGS])==10):
                                logging.debug("length of cluster id is 10")
                                return {STATUS_KEY:SUCCESS_KEY}
                            else:
                                validate_msg="Enter a valid ZS PROJECT CODE"
                                logging.debug(validate_msg)
                        else:
                            validate_msg="no project code mentioned"
                            logging.debug(validate_msg)
                    else:
                        validate_msg="Key ZS_Project_Code is not present"
                        logging.debug(validate_msg)
            else:
                validate_msg="Key 'Tags' is not present in payload"
                logging.debug(validate_msg)
        else:
            validate_msg="payload passed is not a dictionary"
            logging.debug(validate_msg)

        logging.info("Completing function validate")
        return {STATUS_KEY:FAILED_KEY,ERROR_KEY:validate_msg}

    def get_status(self,cluster_id,region_name):
        error_message=""
        try:
            retries = 1
            retry = True

            status_message=""
            emr_client = boto3.client(
                "emr", region_name=region_name
            )
            logging.info("started polling")
            status = emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']['Status']['State']
            logging.info("Completed polling")
            return {STATUS_KEY:SUCCESS_KEY,RESULT_KEY:status}
        except:
            error_message=str(traceback.format_exc())
            return {STATUS_KEY:FAILED_KEY,ERROR_KEY:error_message}


    def launch_emr_cluster(self,event,region_name, process_id=None):
        """
            Purpose             :   This module performs below operation:
                                      a. Launches EMR cluster
            Input               :   payload
            Output              :   Return status :SUCCESS/FAILED ,result : cluster_id , error :ERROR THROWN
            """
        status_message=""
        previous_status=""
        cluster_id = ""
        try:
            validation_status=self.validate(event)
            if(validation_status[STATUS_KEY]==SUCCESS_KEY):
                logging.info("launching emr")
                client = boto3.client('emr', region_name=region_name)
                result = client.run_job_flow(
                Name = event['Name'],
                LogUri = event['LogUri'],
                ReleaseLabel = event['AmiVersion'],
                Instances = event['Instances'],
                Configurations=[
                                                            {
                                                                "Classification": "spark-env",
                                                                "Configurations": [
                                                                    {
                                                                        "Classification": "export",
                                                                        "Properties": {
                                                                            "PYSPARK_PYTHON": "/usr/bin/python3"
                                                                        }
                                                                    }
                                                                ]
                                                            }

                                                        ],
                BootstrapActions = event['BootstrapActions'],
                Applications = event['Applications'],
                VisibleToAllUsers = event['VisibleToAllUsers'],
                JobFlowRole = event['JobFlowRole'],
                ServiceRole = event['ServiceRole'],
                Tags = event['Tags'],
                )
                logging.debug(str(result))

                if('JobFlowId' in result):
                    job_id = result['JobFlowId']
                    result = {
                        "cluster_id": job_id
                    }
                else:
                    raise Exception("'JobFlowId' not in result'")
                cluster_id = result[CLUSTER_ID_KEY]
                if process_id is None:
                    process_id = DEFAULT_PROCESS_ID

                launch_user=self.get_user()[RESULT_KEY]
                launch_user =launch_user.decode('utf-8')
                logging.debug(launch_user)
                status_message = "Obtained cluster id=" + str(result["cluster_id"])
                logging.debug(status_message)
                current_status = self.get_status(result['cluster_id'],region_name)[RESULT_KEY]
                current_ts = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
                logging.debug("__________________________________________________________________________________________")
                logging.debug(result['cluster_id'],event['Name'],process_id,current_status,launch_user,current_ts)
                insert_query = "insert into log_cluster_dtl(" + CLUSTER_ID_KEY + "," + CLUSTER_NAME_KEY + "," + \
                               PROCESS_ID_KEY + "," + CLUSTER_STATUS_KEY + "," + CLUSTER_LAUNCH_USER_KEY + "," + \
                               CLUSTER_CREATE_REQUEST_TIME_KEY + ")" + "values(" + "'" + \
                               result['cluster_id'] + "'" + "," + "'" + event[
                                   'Name'] + "'" + "," + "'" + process_id + "'" + "," + "'" + current_status +\
                               "'" + "," + "'" + launch_user + "'" + "," + "'" + current_ts + "'" + ")"

                logging.info("Query to insert in the log table: %s", str(insert_query))
                response = MySQLConnectionManager().execute_query_mysql(insert_query, False)
                logging.info("Query output: %s", str(response))
                logging.debug(str(response))
                # if response[STATUS_KEY] == FAILED_KEY:
                #     raise Exception("Failed to execute query. ERROR - " + str(response[ERROR_KEY]))
                retries = 1
                retry = True
                while retry is True and retries < 4:
                    while True:
                        try:
                            current_ts = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
                            current_status = self.get_status(result['cluster_id'],region_name)[RESULT_KEY]
                            logging.debug(current_status)
                            update_query="update log_cluster_dtl set "+ CLUSTER_STATUS_KEY+"="+"'"+current_status+"'"+\
                                         "where cluster_id="+"'"+result['cluster_id']+"'"
                            logging.debug(str(update_query))
                            if(previous_status!=current_status):
                                if current_status == WAITING_STATE:
                                    master_ip=CommonUtils().get_emr_ip_list(cluster_id=result['cluster_id'],
                                                                                region=region_name)[RESULT_KEY][0]
                                    logging.debug(master_ip)
                                    update_query = "update log_cluster_dtl set " + CLUSTER_STATUS_KEY + "=" +"'"+\
                                                   current_status + "'" +","+CLUSTER_START_TIME_KEY+"="+"'"+current_ts+"'"+","+\
                                                   MASTER_NODE_DNS_KEY+"="+"'"+master_ip+"'"+ " where cluster_id=" + "'"+\
                                                   result['cluster_id']+"'"
                                    logging.debug(str(update_query))

                                logging.info("Query to insert in the log table: %s", str(update_query))
                                response = MySQLConnectionManager().execute_query_mysql(update_query, False)
                                logging.info("Query output: %s", str(response))
                                logging.debug(str(response))
                                # if response[STATUS_KEY] == FAILED_KEY:
                                #     raise Exception("Failed to execute query. ERROR - " + str(response[ERROR_KEY]))
                                previous_status=current_status

                            status_message = "current cluster status:" + current_status + " for cluster_id:" + result[
                                "cluster_id"]
                            logging.debug(status_message)
                            time.sleep(SLEEP_TIME)
                            if current_status in [WAITING_STATE]:
                                retry = False
                                break
                            if current_status in [TERMINATED_STATE]:
                                raise Exception("cluster terminated")
                        except:
                            error_message = str(traceback.format_exc())
                            if ("ThrottlingException" in error_message):
                                if retries >= 4:
                                    logging.info("Number of attempts: %s has reached the configured limit",
                                                 str(retries))
                                    raise Exception(error_message)
                                else:
                                    retry = True
                                    logging.info("Retrying API call. No of attempts so far: %s", str(retries))
                                    # exponential backoff in sleep time
                                    time.sleep((2 ^ retries) * SLEEP_TIME)
                                    retries += 1
                            else:
                                raise Exception(error_message)

                return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: result["cluster_id"]}
        except:
            # check if cluster was launched
            logging.debug(cluster_id)
            logging.debug(region_name)
            print("*******************")
            if cluster_id != "":
                self.terminate_cluster(str(cluster_id), region_name)
            error_message = status_message , ": " , str(traceback.format_exc())
            logging.error(error_message)
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def terminate_cluster(self, cluster_id, region_name):
        """
            Purpose             :   This module performs below operation:
                                      a. Terminates EMR cluster
            Input               :   cluster id,region name
            Output              :   Return status :SUCCESS/FAILED ,result : cluster_id , error :ERROR THROWN
            """
        status_message=""
        status=""
        try:
            logging.info("terminate cluster started")
            print(cluster_id)
            print(region_name)
            logging.debug(cluster_id)
            logging.debug(region_name)
            logging.debug(type(cluster_id))
            logging.debug(type(region_name))
            print((cluster_id!=""))


            if ((cluster_id!="") and (type(region_name)==str)) :
                print("****************")
                if ((region_name!="") and (type(region_name)==str)):

                    logging.debug("inside nested if")
                    client = boto3.client(
                        "emr",
                        region_name=region_name
                    )

                    print(("Terminating cluster for cluster id"+cluster_id))
                    print(("Terminating cluster for region name"+region_name))
                    time.sleep(10)
                    client.set_termination_protection(JobFlowIds=[cluster_id], TerminationProtected=False)
                    response = client.terminate_job_flows(JobFlowIds=[cluster_id])
                    print(response)
                    logging.debug(response)

                    terminate_user=self.get_user()[RESULT_KEY]
                    logging.info(type(terminate_user))
                    current_ts = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
                    update_query = "update log_cluster_dtl set " + CLUSTER_STATUS_KEY + "=" + "'" + TERMINATED_STATE + "'" \
                                   + "," + CLUSTER_STOP_TIME_KEY + "=" + "'" + current_ts + "'" +"," \
                                   + CLUSTER_TERMINATE_USER_KEY + "=" + "'" + terminate_user.decode('utf-8') \
                                   + "'" + " where cluster_id=" +"'"+cluster_id+"'"

                    logging.info("Query to insert in the log table: %s", str(update_query))
                    response = MySQLConnectionManager().execute_query_mysql(update_query, False)
                    logging.info("Query output: %s", str(response))


                    logging.debug(response)
                    return {STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def create_app_config(self, application_config_dir):
        """
        Purpose     :   To extract application config details
        Input       :   application_config_file = contains all application specific details, env_config_file = contains
                                all environment specific details.
        Output      :   Return status SUCCESS/FAILED
        """
        try:
            status_message = "Calling create_app_config function"
            logging.info(status_message)
            application_config_path = os.path.join(application_config_dir, APPLICATION_CONFIG_FILE)
            environment_config_path = os.path.join(application_config_dir, ENVIRONMENT_CONFIG_FILE)
            app_conf_path= ("application_config_path=" + application_config_path)
            env_conf_path=("environment_config_path=" + environment_config_path)
            logging.debug(app_conf_path)
            logging.debug(env_conf_path)
            application_config = json.load(open(application_config_path))
            logging.debug(application_config)
            # environment_config = json.loads(dbutils.fs.head(environment_config_path))
            environment_config = json.load(open(environment_config_path))
            application_config_str = json.dumps(application_config)
            logging.debug(application_config_str)
            for key in environment_config:
                application_config_str = application_config_str.replace("$$" + key, environment_config[key])

            self.application_config = ast.literal_eval(application_config_str)
            status_message = "Application configuration after replacing environment variable" + json.dumps(
                self.application_config)
            logging.debug(status_message)
            status_message = "Completing create_app_config function"
            logging.debug(status_message)
            return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: self.application_config}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.debug (error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}


