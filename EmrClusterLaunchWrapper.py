#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Module for EMR Launch Cluster Wrapper
"""
__AUTHOR__ = 'ZS Associates'

"""
   Module Name         :   EmrClusterLaunchWrapper
   Purpose             :   Wrapper to EMR Launcher utility.
                           Reads cluster configuration from table AUDIT_INFORMATION.
                           ctl_cluster_config for defined process.
   Input Parameters    :   Process-id, Action-type, Workflow-id
   Output              :   NA
   Execution Steps     :   python EmrClusterLaunchWrapper.py
   Predecessor module  :   NA
   Successor module    :   NA
   Pre-requisites      :   MySQL server should be up and running on the configured MySQL server
   Last changed on     :   17 April 2018
   Last changed by     :   Madhusudhan Naidu
   Reason for change   :   Removed lambda invoking functions
"""

# Library and external modules declaration

import sys
import getopt
import os
import traceback
import subprocess
import time
import logging
import CommonConstants as CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
from CommonUtils import CommonUtils
from Launch_EMR_Utility import Create_EMR
from Poll_EMR_Utility import Emr_State
from Get_EMR_Metadata import Get_EMR_Metadata
from Get_EMR_Host_IP import Get_EMR_Host_IP
from ConfigUtility import JsonConfigUtility
from ExecutionContext import ExecutionContext

# Define all module level constants here
MODULE_NAME = "EmrClusterLaunchWrapper"

logger = logging.getLogger(__name__)
hndlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hndlr.setFormatter(formatter)


class EmrClusterLaunchWrapper(object):
    """
    :type : class contains all the functions related to MySQLConnectionManager
    """

    def __init__(self):
        self.emr_cluster_metadata = None
        self.configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                            CommonConstants.
                                                            ENVIRONMENT_CONFIG_FILE))
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY
                                                              , "mysql_db"])
        self.bucket_name = self.configuration.get_configuration([CommonConstants.
                                                                 ENVIRONMENT_PARAMS_KEY,
                                                                 "s3_bucket_name"])

    def extractClusterConfiguration(self, process_id):
        """
        :type : Function to retrieve Cluster Configuration Details for given process-id
        :return:  cluster configs
        """

        query = "SELECT * FROM " + self.audit_db + "." + CommonConstants\
            .EMR_CLUSTER_CONFIGURATION_TABLE + " WHERE process_id = " + str(process_id) + ";"
        logger.info("Generated Query to extract Cluster config: %s", str(query))
        cluster_configs = MySQLConnectionManager().execute_query_mysql(query, False)
        return cluster_configs

    def launchEmr(self, user_name, process_id):
        """
        :type : Function to Launch EMR Cluster as per Cluster Configuration defined for Process-id
        :return: Launches a Cluster
        """
        logger.info("Launching EMR cluster")
        logger.info("Reading cluster configuration from AUDIT_INFORMATION.EMR_CLUSTER_CONFIGURATION"
                    " table.")
        cluster_configs = self.extractClusterConfiguration(process_id)
        logger.info("Cluster Configuration Provided: %s", str(cluster_configs[0]))
        self.cluster_configs = cluster_configs[0]
        create_emr = Create_EMR(self.cluster_configs, self.bucket_name)
        logger.info("Checking if cluster execution is enabled for the process : %s",
                    str(process_id))
        if self.cluster_configs['active_flag'].lower() == 'y':
            try:
                logger.info("Cluster configuration is active")
                jobid = create_emr.launch_emr_cluster()
                cluster_id = jobid
                self.cluster_id = cluster_id
                status_message = "Cluster Launched, Cluster ID: " + str(self.cluster_id)
                logger.info(status_message)
                query = "Insert into {audit_db}.{emr_cluster_details_table} (cluster_id," \
                        "cluster_status,process_id," \
                        "cluster_name,cluster_launch_user)values ('{cluster_id}','{status}'," \
                        "{process_id}," \
                        "'{cluster_name}','{cluster_launch_user}')".format(
                            audit_db=self.audit_db,
                            emr_cluster_details_table=CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
                            cluster_id=self.cluster_id,
                            status=CommonConstants.EMR_STARTING, process_id=process_id,
                            cluster_name=self.cluster_configs['cluster_name'],
                            cluster_launch_user=user_name)
                logger.info("Query to insert in the log table: %s", str(query))
                response = MySQLConnectionManager().execute_query_mysql(query, False)
                logger.info("Query output: %s", str(response))
                return response
            except Exception as e:
                status_message = "Error Occurred when invoking EMR cluster. Exiting"
                logger.error(status_message + str(traceback.format_exc()))
                raise e
        else:
            status_message = "Cluster Instantiation is disabled for Process ID: " + str(process_id)
            logger.info(status_message)
            raise Exception(status_message)

    def pollClusterStatus(self, cluster_id=None):
        """
        :type: Function to Poll status of EMR Cluster
        :param cluster_id: cluster id (job id)
        :return: cluster state
        """

        emr_state = Emr_State()
        logger.info("Starting EMR Cluster status check. Input cluster ID: %s", str(cluster_id))
        if cluster_id is None or cluster_id == '':
            logger.info("Cluster ID not provided, hence cluster must be launched in this execution."
                        " Cluster ID retrieved: %s", str(self.cluster_id))
            if self.cluster_id is None or self.cluster_id == '':
                status_message = "Unable to find Cluster ID"
                raise Exception(status_message)
            else:
                cluster_id_str = self.cluster_id
        else:
            cluster_id_str = cluster_id
        try:
            status_check_emr = emr_state.get_emr_cluster_state(jobid=cluster_id_str,
                                                               region_name=self.cluster_configs[
                                                                   "region_name"])
            status_message = "Status of EMR cluster: " + str(status_check_emr)
            logger.info(status_message)
            return status_check_emr
        except Exception as e:
            status_message = "Error in polling the EMR status: " + str(e.message)
            logger.error(status_message + str(traceback.format_exc()))
            raise e


    def extractEmrClusterDetails(self, cluster_id=None):
        """
        :type: Function to fetch EMR Cluster metadata from AWS
        :param cluster_id: cluster id (job id)
        :return: EMR meta data
        """
        logger.info("Extracting Cluster Metadata from EMR")
        if cluster_id is None or cluster_id == '':
            logger.info("Cluster Id not provided, checking cluster id in object of class "
                        "EmrClusterLaunchWrapper")
            if self.cluster_id is None or self.cluster_id == '':
                status_message = "Cluster Id not available in class EmrClusterLaunchWrapper, " \
                                 "exiting"
                raise Exception(status_message)
            else:
                logger.info(
                    "Setting EmrClusterLaunchWrapper object cluster_id as cluster_id to poll for"
                    " status check")
        else:
            logger.info("Setting cluster_id provided as cluster_id to poll for status check")

        try:
            emr_state = Get_EMR_Metadata()
            emr_cluster_metadata = emr_state.get_emr_cluster_state(jobid=self.cluster_id,
                                                                   region_name=self.cluster_configs
                                                                   ["region_name"])
            self.emr_cluster_metadata = emr_cluster_metadata
            status_message = "Cluster Metadata Retrieved: " +  str(self.emr_cluster_metadata)
            logger.info(status_message)
        except Exception as e:
            status_message = "Error in extract EMR Cluster Details"
            logger.error(status_message + str(traceback.format_exc()))
            raise e

    def populateEmrClusterDetailsTable(self, process_id):

        """
        :def: Function to insert EMR Cluster metadata into EMR_CLUSTER_DETAILS table
        :return:
        """
        try:
            logger.info("Inserting EMR metadata into table EMR_CLUSTER_DETAILS")
            request_sent_time = self.emr_cluster_metadata['CreationDateTime']
            cluster_ready_time = self.emr_cluster_metadata['ReadyDateTime']
            master_node_dns = self.emr_cluster_metadata['DNS']
            query = "Update {audit_db}.{emr_cluster_details_table} set cluster_status='" \
                    "{cluster_status}'," \
                    "master_node_dns='{master_node_dns}',cluster_create_request_time=" \
                    "'{cluster_create_request_time}'," \
                    "cluster_start_time='{cluster_start_time}',cluster_stop_time=" \
                    "{cluster_stop_time} where " \
                    "cluster_id='{cluster_id}'".format(audit_db=self.audit_db,
                                                       process_id=process_id,
                                                       emr_cluster_details_table=CommonConstants.
                                                       EMR_CLUSTER_DETAILS_TABLE,
                                                       cluster_status=CommonConstants.EMR_WAITING,
                                                       master_node_dns=master_node_dns,
                                                       cluster_create_request_time=
                                                       request_sent_time,
                                                       cluster_start_time=cluster_ready_time,
                                                       cluster_stop_time='NULL',
                                                       cluster_id=self.cluster_id)
            logger.info("Query to update EMR details: " + str(query))
            response = MySQLConnectionManager().execute_query_mysql(query, False)
            logger.info("Query output: " + str(response))
            status_message = "Response of Insert Query: " + str(response)
            logger.info(status_message)
        except Exception as e:
            status_message = "Error while populating EMR the details"
            logger.error(status_message + str(traceback.format_exc()))
            raise e

    def populateClusterIPAddress(self):
        """
        Method for populating Cluster IP Address
        :return:
        """
        try:
            logger.info("Fetching the Host IP's")
            get_ip = Get_EMR_Host_IP()
            ips = get_ip.get_ips(self.cluster_id, self.cluster_configs["region_name"])
            for ip in ips:
                query = "Insert into {audit_db}.{emr_cluster_details_table} (cluster_id," \
                        "host_ip_address) values " \
                        "('{cluster_id}','{host_ip_address}')".format(audit_db=self.audit_db,
                                                                      emr_cluster_details_table=
                                                                      CommonConstants.
                                                                      EMR_CLUSTER_HOST_DETAILS,
                                                                      cluster_id=self.cluster_id,
                                                                      host_ip_address=ip)
                response = MySQLConnectionManager().execute_query_mysql(query, False)
                status_message = "Response of Insert Query: " + str(response)
                logger.info(status_message)
        except Exception as e:
            logger.error(str(traceback.format_exc()))
            raise e

    def main(self, process_id, action_type_str, cluster_id_str=None):
        """
        Main function
        :param process_id:
        :param action_type_str:
        :param cluster_id_str:
        :return:
        """
        try:
            # main() function
            configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                           CommonConstants.ENVIRONMENT_CONFIG_FILE))
            audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                        "mysql_db"])

            user_name = CommonUtils().get_user_name()
            logger.info('Retrieved user name: %s', str(user_name))

            # Getting current status of EMR
            process_id = str(process_id)

            query = "select cluster_id from {audit_db}.{emr_cluster_details_table} where " \
                    "process_id = {process_id_q}" \
                    " and cluster_status = '{cluster_status}';".format(audit_db=audit_db,
                                                                       emr_cluster_details_table=
                                                                       CommonConstants.
                                                                       EMR_CLUSTER_DETAILS_TABLE,
                                                                       process_id_q=process_id,
                                                                       cluster_status=
                                                                       CommonConstants.EMR_WAITING)
            logger.info("Query to fetch current status of EMR: %s", str(query))
            existing_cluster_id = MySQLConnectionManager().execute_query_mysql(query)
            logger.info("Result of query: %s", str(existing_cluster_id))
            if not existing_cluster_id:
                # Creating Object of class EmrClusterLaunchWrapper
                logger.info("No existing cluster found hence proceeding with next steps")
                if action_type_str.lower() == 'start':
                    logger.info('Action type is: %s hence launching EMR', str(action_type_str))
                    self.launchEmr(user_name, process_id)
                    # Poll status of EMR until it is launched i.e. status changes to WAITING.
                    logger.info("Launched EMR, polling for completion of launch process")
                    status_check_emr = 'STARTING'
                    flag = 0
                    while status_check_emr != "TERMINATING" and status_check_emr != "SHUTTING_DOWN"\
                            and status_check_emr != "FAILED" and status_check_emr != "WAITING":
                        status_check_emr = self.pollClusterStatus()
                        time.sleep(60)
                        if status_check_emr == "BOOTSTRAPPING" and flag == 0:
                            logger.info("Updating status of EMR since its in Bootstrapping state")
                            query = "Update {audit_db}.{emr_cluster_details_table} set " \
                                    "cluster_status='{status}' " \
                                    "where cluster_id='{cluster_id}'".\
                                format(audit_db=audit_db,
                                       emr_cluster_details_table=
                                       CommonConstants.EMR_CLUSTER_DETAILS_TABLE,
                                       cluster_id=self.cluster_id,
                                       status=CommonConstants.EMR_BOOTSTRAPPING)
                            logger.info("Query to update status: %s", str(query))
                            response = MySQLConnectionManager().execute_query_mysql(query, False)
                            logger.info("Query output: %s", str(response))
                            flag = 1
                        if status_check_emr == "WAITING":
                            logger.info("EMR Launched successfully, cluster id: %s",
                                        str(self.cluster_id))
                            self.extractEmrClusterDetails()
                            self.populateEmrClusterDetailsTable(process_id)
                            self.populateClusterIPAddress()
                            break
                    if status_check_emr == "SHUTTING_DOWN" or status_check_emr == "FAILED" or \
                            status_check_emr == "TERMINATING":
                        logger.info("EMR Launched Failed, cluster id: %s", str(cluster_id_str))
                        query = "Update {audit_db}.{emr_cluster_details_table} set " \
                                "cluster_status='{status}' where cluster_id='{cluster_id}'".\
                            format(audit_db=audit_db,
                                   emr_cluster_details_table=CommonConstants.
                                   EMR_CLUSTER_DETAILS_TABLE,
                                   cluster_id=cluster_id_str,
                                   status=CommonConstants.EMR_TERMINATED)
                        response = MySQLConnectionManager().execute_query_mysql(query, False)
                        raise Exception("EMR Launched Failed, cluster id: %s", str(cluster_id_str))
                elif action_type_str.lower() == 'poll':
                    self.pollClusterStatus(cluster_id_str)
                # elif action_type_str.lower() == 'stop':
                #     logger.info("Terminating Cluster")
                #
                else:
                    logger.error("Invalid Action_Type provided")
            else:
                logger.info("Cluster already running. cluster_id = %s", str(existing_cluster_id))
        except Exception as e:
            logger.error(str(traceback.format_exc()))
            raise e


def usage():
    """
    :def: USAGE() Function
    :return: prints help statement
    """
    msg = """Usage: python EmrClusterLaunchWrapper.py -p <process_id> -a <action_type> -c cluster_id
process_id to be picked up from table AUDIT_INFORMATION.EMR_CLUSTER_CONFIGURATION table.
action_type could be any of start, stop, poll. specifies the action to be performed for EMR
cluster_id will be used for 'poll' and 'stop'  actions. For 'start' action it will be ignored.
                """
    logger.debug(msg)


if __name__ == "__main__":

    try:
        # Declaring empty variables
        process_id = ''
        action_type = ''
        cluster_id = ''
        try:
            opts, args = getopt.getopt(sys.argv[1:], "hp:a:c:")
            # Parsing Command Line Arguments
            for opt, arg in opts:
                if opt == '-h':
                    usage()
                elif opt == "-p":
                    process_id = arg
                elif opt == "-a":
                    action_type = arg
                elif opt == "-c":
                    cluster_id = arg
            obj = EmrClusterLaunchWrapper()
            obj.main(process_id, action_type, cluster_id)
        except getopt.GetoptError:
            msg = "Invalid arguments"
            usage()
            raise Exception(msg)
        except Exception as e:
            raise Exception('Arguments are incorrect ' + str(e))
    except Exception as e:
        logger.error(str(traceback.format_exc()))
        raise e
