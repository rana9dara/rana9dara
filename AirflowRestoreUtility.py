#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse
import json
import sys
import traceback
import os
import MySQLdb
import boto3
import awscli.clidriver
from LogSetup import logger
from MoveCopy import MoveCopy
import AirflowUtilityConstants
from AirflowCleanupUtility import AirflowCleanupUtility
from ExecutionContext import ExecutionContext
from MySQLConnectionManager import MySQLConnectionManager

__author__ = 'ZS Associates'
"""
Module Name         : Airflow restore Utility
Purpose             :
Input               : dag name, full backup flag, mysql host, database name, username, password,
                        cleanup flag, s3 base path and airflow home directory
Output              : Backup the airlfow DAG, logs and sql data to S3
Pre-requisites      : Airflow DAG name and python file name should be same.
Last changed on     : 25th May 2018
Last changed by     : krishna.medikeri@zs.com
Reason for change   : Initial development
"""

# Library and external modules declaration

# Module level constants
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_KEY = "status"
ERROR_KEY = "error"
RESULT_KEY = "result"

SUBDAG_PREFIX = "subdags_"
EMPTY = ''
DEFAULT_MYSQL_PORT = 3306
DEFAULT_SQL_FILENAME = "data.sql"

# The usage sting to be displayed to the user for the utility
USAGE_STRING = """
SYNOPSIS
    python AirflowRestoreUtility.py -f/--conf_file_path <conf_file_path> -c/--conf <conf>

    Where
        conf_file_path - Absolute path of the file containing JSON configuration
        conf - JSON configuration string

        Note: Either 'conf_file_path' or 'conf' should be provided.

"""

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_NAME = "AirflowRestoreUtility"
EXECUTION_CONTEXT = ExecutionContext()
EXECUTION_CONTEXT.set_context({"current_module": MODULE_NAME})


class AirflowRestoreUtility(object):
    """
    Class contains all the functions related to Airflow DAG backup
    """

    def __init__(self, date, restore_file_pattern, host, username, password, database_name, s3_path, airflow_home,
                 port=DEFAULT_MYSQL_PORT):
        self.move_copy_object = MoveCopy()
        self.restore_file_pattern = restore_file_pattern
        self.host = host
        self.username = username
        self.password = password
        self.database_name = database_name
        self.s3_path = s3_path
        self.airflow_home = airflow_home
        self.port = port
        self.dag_names = []
        self.date = date

    def create_and_get_connection(self, host, username, password, port, database_name):
        """
        Purpose   :   Creates a MySQL connection object to be used for executing queries
        Input     :   MySQL host, username, password, optional port (Default is 3306), optional database name
                      (Default is empty)
        Output    :   MySQL connection object
        """
        try:
            # Check for mandatory parameters
            if host is None or username is None or password is None:
                raise Exception("Please provide host, username and password for creating connection")

            # Create a connection to MySQL database
            connection = MySQLdb.connect(user=username, passwd=password, host=host, port=port, db=database_name)
            logger.debug(
                "MySQL connection created for Host: %s, Port: %s, Username: %s, Password: *******, Database: %s",
                host, str(port), username, database_name, extra=EXECUTION_CONTEXT.get_context())

            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: connection}

        except Exception as ex:
            traceback.print_exc()
            logger.error("Error while creating database connection. ERROR - %s", str(traceback.format_exc()),
                         extra=EXECUTION_CONTEXT.get_context())
            return {STATUS_KEY: STATUS_FAILED, ERROR_KEY: str(ex)}

    def close_connection(self, conn):
        """
        Purpose   :   Closes the MySQL connection
        Input     :   MySQL connection object
        Output    :   None
        """
        try:
            conn.close()
            logger.debug("MySQL connection closed", extra=EXECUTION_CONTEXT.get_context())
            return {STATUS_KEY: STATUS_SUCCESS}
        except Exception as ex:
            logger.error("Error while closing database connection. ERROR - %s", str(traceback.format_exc()),
                         extra=EXECUTION_CONTEXT.get_context())
            return {STATUS_KEY: STATUS_FAILED, ERROR_KEY: str(ex)}

    def s3_to_local(self, command):
        try:
            cli_execution_status = awscli.clidriver.create_clidriver().main(command.split())
            if cli_execution_status != 0:
                raise Exception("Return status = " + str(cli_execution_status))
        except Exception as s3_error:
            logger.error("Error while executing command - %s\nError: %s", command, str(s3_error),
                         extra=EXECUTION_CONTEXT.get_context())
            raise s3_error

    def fetch_dag_names(self):
        """
        Purpose   :   Fetches DAG names from s3
        Input     :   Na
        Output    :   Na
        """
        try:
            bucket = self.s3_path.split("/")[2]
            prefix = self.s3_path.split(bucket)[1].strip("/") + "/" + self.date + "/"
            client = boto3.client('s3')
            result = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
            if result.get('CommonPrefixes') is not None:
                for objects in result.get('CommonPrefixes'):
                    if self.restore_file_pattern is not None:
                        if objects.get('Prefix').endswith(self.restore_file_pattern + "/"):
                            self.dag_names.append(str(objects.get('Prefix')).split("/")[-2])
                    else:
                        self.dag_names.append(objects.get('Prefix').split("/")[-2])
                logger.debug("List of DAGs to restore - %s", str(self.dag_names), extra=EXECUTION_CONTEXT.get_context())
        except Exception as ex:
            logger.error("Error while fetching list of DAGs from s3.\n%s", str(ex),
                         extra=EXECUTION_CONTEXT.get_context())
            raise ex

        return self.dag_names

    def archive_dag(self, name):
        """
        Purpose   :   Archives latest backups
        Input     :   dag name
        Output    :   Na
        """
        latest_backup_path = self.s3_path + "/" + self.date + "/" +  name
        archive_path = self.s3_path + "/archive/" + self.date + "/" + name

        job_output = self.move_copy_object.s3_move_copy(action="move", source_path_type="s3",
                                                        source_path=latest_backup_path,
                                                        target_path=archive_path,
                                                        sse_flag=True)

        if job_output[STATUS_KEY] == STATUS_FAILED:
            raise Exception("Error while archiving DAGs to s3.\n" + "Error: " + job_output[ERROR_KEY])

    def restore(self, backup_dag_name):
        """
        Purpose   :   takes backup of dag_name and stores in S3
        Input     :   dag name and airflow_table_config
        Output    :   backup status
        """
        try:
            logger.debug("Processing DAG - %s", backup_dag_name, extra=EXECUTION_CONTEXT.get_context())
            s3_source_path = self.s3_path + "/" + self.date + "/" + backup_dag_name

            # Restore sql data, logs and dag file
            if self.restore_file_pattern:
                self.restore_logs(s3_source_path, backup_dag_name.replace("_" + self.restore_file_pattern, ''))
            else:
                self.restore_logs(s3_source_path, backup_dag_name)
            self.restore_sql_data(s3_source_path)
            self.restore_dagfile(s3_source_path)

            # Archive the DAG
            self.archive_dag(backup_dag_name)

            # Return status and query result
            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: EMPTY}
        except Exception as ex:
            logger.error("Error while executing query. ERROR - %s", str(traceback.format_exc()),
                         extra=EXECUTION_CONTEXT.get_context())
            return {STATUS_KEY: STATUS_FAILED, ERROR_KEY: str(ex)}

    def get_subdag_path(self, name):
        """
        Purpose   :   Create subdag path from dag_name
        Input     :   dag name
        Output    :   subdag path
        """
        subdag_name = SUBDAG_PREFIX + name
        subdag_path = self.airflow_home + "/dags/" + subdag_name
        if os.path.isdir(subdag_path):
            return subdag_name

        return None

    def restore_dagfile(self, s3_dag_path):
        """
        Purpose   :   Restores dag_name from s3 to history server
        Input     :   dag name and target_path
        Output    :   backup status
        """
        try:
            # copy DAG from s3 to local
            dags_path = s3_dag_path + "/dags"
            hs_dags_path = self.airflow_home + "/dags"
            command = "s3 cp {} {} --recursive".format(dags_path, hs_dags_path)
            logger.debug("restore_dagfile - %s", command, extra=EXECUTION_CONTEXT.get_context())
            self.s3_to_local(command)

            # Return status and query result
            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: EMPTY}

        except Exception as ex:
            logger.error("Error while restoring DAG. ERROR - %s", str(traceback.format_exc()),
                         extra=EXECUTION_CONTEXT.get_context())
            raise ex

    def restore_logs(self, s3_logs_path, backup_dag_name):
        """
        Purpose   :   takes backup of dag_name and stores in S3
        Input     :   dag name and target_path
        Output    :   backup status
        """
        try:
            # copy logs from s3 to local
            logs_path = s3_logs_path + "/logs/"
            hs_logs_path = self.airflow_home + "/logs/" + backup_dag_name
            command = "s3 cp {} {} --recursive".format(logs_path, hs_logs_path)
            logger.debug("restore_logs() - %s", command, extra=EXECUTION_CONTEXT.get_context())
            self.s3_to_local(command)

            # Return status and query result
            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: EMPTY}

        except Exception as ex:
            logger.error("Error while restoring logs. ERROR - %s", str(traceback.format_exc()),
                         extra=EXECUTION_CONTEXT.get_context())
            raise ex

    def restore_sql_data(self, s3_data_path):
        """
        Purpose   :   takes backup of dag_name and stores in S3
        Input     :   dag name, airflow_table_config, base_s3_path
        Output    :   backup status
        """
        conn_created = False
        conn = None
        try:
            output = self.create_and_get_connection(self.host, self.username, self.password, self.port,
                                                    self.database_name)
            if output[STATUS_KEY] == STATUS_FAILED:
                raise Exception("Error while creating sql connection.\n" + "Error: " + output[ERROR_KEY])

            conn = output[RESULT_KEY]
            conn_created = True

            # copy data.sql file from s3 to local
            if "server_logs" not in s3_data_path:
                s3_path = s3_data_path + "/data/data.sql"
                local_path = os.getcwd()
                # local_path = self.airflow_home
                command = "s3 cp {} {}".format(s3_path, local_path)
                logger.debug("restore_sql_data - %s", command, extra=EXECUTION_CONTEXT.get_context())
                self.s3_to_local(command)

                cursor = conn.cursor()
                with open(DEFAULT_SQL_FILENAME, 'r') as files:
                    for i, query in enumerate(files):
                        # executing query
                        logger.debug("Executing query - %s", query, extra=EXECUTION_CONTEXT.get_context())
                        cursor.execute(query)
                        logger.debug("Query executed successfully", extra=EXECUTION_CONTEXT.get_context())

                        if i % 500 == 0:
                            cursor.close()
                            conn.commit()
                            cursor = conn.cursor()

                cursor.close()
                conn.commit()

            # Return status and query result
            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: EMPTY}

        except Exception as ex:
            logger.error("Error while executing query. ERROR - %s", str(traceback.format_exc()),
                         extra=EXECUTION_CONTEXT.get_context())
            raise ex
        finally:
            if conn_created and conn:
                self.close_connection(conn)


# Print the usage for the Database Utility
def usage(status=1):
    sys.stdout.write(USAGE_STRING)
    sys.exit(status)


if __name__ == '__main__':
    # Setup basic logging info

    CONF_FILE_PATH = None
    CONF = None
    ARGS = None
    try:
        PARSER = argparse.ArgumentParser(description="Python utility to move/copy log files from source to destination")
        PARSER.add_argument("-f", "--conf_file_path", help="Config file path.")
        PARSER.add_argument("-c", "--conf", help="Config json input.")
        PARSER.add_argument("-b", "--backup_date", required=True, help="Date of Backup.")
        PARSER.add_argument("-d", "--docker_flag", choices=['Yes', 'No'], required=True, help="Is it Docker? Yes/No")
        PARSER.add_argument("-n", "--restore_file_run_id", help="Run ID of the back up file.")
        # parser.add_argument("--help", help="Help")
        ARGS = PARSER.parse_args()

    except Exception as error:
        sys.stderr.write(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nERROR: " + str(error)}))
        usage(1)

    logger.debug("Parse the input arguments...", extra=EXECUTION_CONTEXT.get_context())
    try:
        RUN_ID = ""
        DOCKER_FLAG = ""
        if ARGS.conf_file_path is not None:
            CONF_FILE_PATH = ARGS.conf_file_path
        elif ARGS.conf is not None:
            CONF = ARGS.conf
        if ARGS.backup_date is not None:
            BACKUP_DATE = ARGS.backup_date
        else:
            raise Exception("Backup Date cannot be empty or None.")
        if ARGS.docker_flag is not None:
            DOCKER_FLAG = ARGS.docker_flag
        if DOCKER_FLAG == "Yes" and ARGS.restore_file_run_id is not None:
            RESTORE_DAG_PATTERN = ARGS.restore_file_run_id
        elif DOCKER_FLAG == "Yes" and ARGS.restore_file_run_id is None:
            raise Exception("The backup_file_name cannot be empty while the docker_flag is yes")
        else:
            RESTORE_DAG_PATTERN = None

        # Check for all the mandatory arguments
        if CONF_FILE_PATH is None and CONF is None:
            sys.stderr.write(json.dumps({
                STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nERROR: Either JSON configuration file path "
                                                      "or JSON configuration string should be provided\n"}))
            usage(1)

        # Parse the configuration
        if CONF_FILE_PATH:
            with open(CONF_FILE_PATH) as conf_file:
                CONFS = json.load(conf_file)
        else:
            CONFS = json.loads(CONF)

        logger.debug("Successfully parsed configuration...", extra=EXECUTION_CONTEXT.get_context())
        # validate the configuration
        ENVIRONMENT = CONFS.get("environment", None)
        TAG_NAME = CONFS.get("tag_name", None)
        DAG_NAME = CONFS.get("dag_name", None)
        FULL_RESTORE = CONFS.get("full_restore", None)
        CLEANUP = CONFS.get("cleanup", None)
        S3_BASE_PATH = CONFS.get("s3_base_path", None)
        AIRFLOW_HOME_DIR = CONFS.get("airflow_home_dir", None)
        DB_SECRET_NAME_S3_REGION = CONFS.get("s3_region", None)

        MYSQL_CONF = CONFS["mysql_connection_details"]
        DB_HOST = MYSQL_CONF.get("host", None)
        DB_NAME = MYSQL_CONF.get("db_name", None)
        DB_USERNAME = MYSQL_CONF.get("username", None)
        DB_PASSWORD_SECRET_NAME = MYSQL_CONF.get("password", None)
        DB_PORT = MYSQL_CONF.get("port", DEFAULT_MYSQL_PORT)

        secret_password = MySQLConnectionManager().get_secret(DB_PASSWORD_SECRET_NAME, DB_SECRET_NAME_S3_REGION)
        DB_PASSWORD = secret_password['password']

        if DOCKER_FLAG == "Yes":
            S3_BASE_PATH = S3_BASE_PATH + "/container_logs"
        elif DOCKER_FLAG == "No":
            S3_BASE_PATH = S3_BASE_PATH + "/backup"
        logger.debug("Instantiate the AirflowRestoreUtility", extra=EXECUTION_CONTEXT.get_context())
        AIRFLOW_UTILITY = AirflowRestoreUtility(date=BACKUP_DATE,
                                                restore_file_pattern=RESTORE_DAG_PATTERN,
                                                host=DB_HOST,
                                                username=DB_USERNAME,
                                                password=DB_PASSWORD,
                                                database_name=DB_NAME,
                                                s3_path=S3_BASE_PATH,
                                                airflow_home=AIRFLOW_HOME_DIR,
                                                port=DB_PORT)

        logger.debug("Instantiate the AirflowCleanupUtility", extra=EXECUTION_CONTEXT.get_context())
        CLEANUP_UTILITY = AirflowCleanupUtility(host=DB_HOST,
                                                username=DB_USERNAME,
                                                password=DB_PASSWORD,
                                                database_name=DB_NAME,
                                                airflow_home=AIRFLOW_HOME_DIR,
                                                port=DB_PORT)

        # create list of DAG names to process
        DAG_NAMES = []
        if FULL_RESTORE:
            DAG_NAMES = AIRFLOW_UTILITY.fetch_dag_names()
        elif DAG_NAME:
            DAG_NAMES.append(DAG_NAME)
        # Process the DAGs
        for DAG_NAME in DAG_NAMES:
            logger.debug('Processing DAG - %s', DAG_NAME, extra=EXECUTION_CONTEXT.get_context())
            logger.info("CLEANING AIRFLOW: ", extra=EXECUTION_CONTEXT.get_context())
            if AIRFLOW_UTILITY.restore_file_pattern is None:
                AIRFLOW_UTILITY.restore_file_pattern = ""
            if AIRFLOW_UTILITY.restore_file_pattern:
                exec_status = CLEANUP_UTILITY.cleanup(name=DAG_NAME.replace("_" + AIRFLOW_UTILITY.restore_file_pattern, ''),
                                                      airflow_table_config=AirflowUtilityConstants.airflow_table_config)
            else:
                exec_status = CLEANUP_UTILITY.cleanup(name=DAG_NAME,
                                                  airflow_table_config=AirflowUtilityConstants.airflow_table_config)
            if exec_status[STATUS_KEY] == STATUS_FAILED:
                sys.stderr.write(json.dumps(exec_status))
                sys.exit(1)
            logger.info("CLEANING DONE: ", extra=EXECUTION_CONTEXT.get_context())
            logger.info("RESTORING AIRFLOW: ", extra=EXECUTION_CONTEXT.get_context())
            exec_status = AIRFLOW_UTILITY.restore(backup_dag_name=DAG_NAME)
            if exec_status[STATUS_KEY] == STATUS_FAILED:
                sys.stderr.write(json.dumps(exec_status))
                sys.exit(1)
            logger.info("RESTORING DONE: ", extra=EXECUTION_CONTEXT.get_context())
            logger.debug("Airflow restore completed successfully...", extra=EXECUTION_CONTEXT.get_context())

    except Exception as err:
        traceback.print_exc(err)
        logger.error(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nError during airflow restore process",
                                 "ERROR": str(err)}), extra=EXECUTION_CONTEXT.get_context())
        sys.exit(1)
