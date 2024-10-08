#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import sys
import getopt
import traceback
import os
import glob
import shutil
import MySQLdb
from LogSetup import logger
from ExecutionContext import ExecutionContext
from MoveCopy import MoveCopy
import AirflowUtilityConstants

__author__ = 'ZS Associates'
"""
Module Name         : Airflow Cleanup Utility
Purpose             :
Input               : dag name, mysql host, database name, username, password and airflow home directory
Output              : Cleanup the airlfow DAG, logs and sql data
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
MODULE_NAME = "AirflowCleanupUtility"
EXECUTION_CONTEXT = ExecutionContext()
EXECUTION_CONTEXT.set_context({"current_module": MODULE_NAME})
# The usage sting to be displayed to the user for the utility
USAGE_STRING = """
SYNOPSIS
    python AirflowCleanupUitlity.py -f/--conf_file_path <conf_file_path> -c/--conf <conf>

    Where
        conf_file_path - Absolute path of the file containing JSON configuration
        conf - JSON configuration string

        Note: Either 'conf_file_path' or 'conf' should be provided.

"""

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


class AirflowCleanupUtility(object):
    """
    Class contains all the functions related to Airflow DAG cleanup
    """

    def __init__(self, host, username, password, database_name, airflow_home,
                 port=DEFAULT_MYSQL_PORT):
        self.move_copy_object = MoveCopy()
        self.host = host
        self.username = username
        self.password = password
        self.database_name = database_name
        self.airflow_home = airflow_home
        self.port = port
        self.dag_names = []

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
            logger.debug("MySQL connection created for Host: %s, Port: %s, Username: %s" +
                         ", Password: *******, Database: %s", host, str(port), username, database_name,
                         extra=EXECUTION_CONTEXT.get_context())

            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: connection}

        except Exception as ex:
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

    def cleanup(self, name, airflow_table_config):
        """
        Purpose   :   Cleanup DAG file, logs and sql data
        Input     :   dag name and airflow_table_config
        Output    :   cleanup status
        """
        try:
            # Cleanup sql data, logs and dag file
            self.cleanup_dagfile(name)
            self.cleanup_sql_data(name, airflow_table_config)
            self.cleanup_logs(name)

            # Return status and query result
            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: EMPTY}
        except Exception as ex:
            logger.error("Error during DAG cleanup. ERROR - %s", str(traceback.format_exc()),
                         extra=EXECUTION_CONTEXT.get_context())
            return {STATUS_KEY: STATUS_FAILED, ERROR_KEY: str(ex)}

    def get_subdag_path(self, name):
        """
        Purpose   :   fetches subdag path from dag_name
        Input     :   dag name
        Output    :   subdag path
        """
        subdag_name = SUBDAG_PREFIX + name
        subdag_path = self.airflow_home + "/dags/" + subdag_name
        if os.path.isdir(subdag_path):
            return subdag_name

        return None

    def cleanup_dagfile(self, dag_name):
        """
        Purpose   :   Cleanup DAG file
        Input     :   dag name
        Output    :   Na
        """
        try:
            # remove existing DAG from local
            dag_path = self.airflow_home + "/dags/" + dag_name + ".py"
            dag_path_pyc = self.airflow_home + "/dags/" + dag_name + ".pyc"
            logger.debug("cleanup_dagfile() - DELETE %s", dag_path, extra=EXECUTION_CONTEXT.get_context())
            if os.path.exists(dag_path):
                os.remove(dag_path)
            if os.path.exists(dag_path_pyc):
                os.remove(dag_path_pyc)

            # remove subdags from local
            if self.get_subdag_path(dag_name):
                subdags_path = self.airflow_home + "/dags/" + self.get_subdag_path(dag_name)
                logger.debug("cleanup_dagfile() - DELETE %s", subdags_path, extra=EXECUTION_CONTEXT.get_context())
                shutil.rmtree(subdags_path)
        except Exception as ex:
            logger.error("Error while deleting DAG file. ERROR - %s", str(traceback.format_exc()),
                         extra=EXECUTION_CONTEXT.get_context())
            raise ex

    def cleanup_logs(self, dag_name):
        """
        Purpose   :   Cleanup logs from local
        Input     :   dag name
        Output    :   Na
        """
        try:
            # remove existing logs from local
            logs_path = self.airflow_home + "/logs/" + dag_name
            logger.debug("cleanup_logs() - DELETE %s", logs_path, extra=EXECUTION_CONTEXT.get_context())
            # remove logs for subdags from local
            for path in glob.glob(logs_path + ".*"):
                if not os.path.isfile(path):
                    shutil.rmtree(path)

            if os.path.exists(logs_path):
                shutil.rmtree(logs_path)

            webserver_scheduler_log_path = self.airflow_home + "/logs"
            for file in os.listdir(webserver_scheduler_log_path):
                if file == "scheduler.logs":
                    os.remove(webserver_scheduler_log_path + "/scheduler.logs")
                if file == "webserver.logs":
                    os.remove(webserver_scheduler_log_path + "/webserver.logs")


        except Exception as ex:
            logger.error("Error while deleting logs. ERROR - %s", str(traceback.format_exc()),
                         extra=EXECUTION_CONTEXT.get_context())
            raise ex

    def cleanup_sql_data(self, dag_name, airflow_table_config):
        """
        Purpose   :   Removes DAG sql data from local
        Input     :   dag name and airflow table config
        Output    :   Na
        """
        conn_created = False
        conn = None
        try:
            logger.debug("*************** Cleaning MySQL *****************", extra=EXECUTION_CONTEXT.get_context())
            output = self.create_and_get_connection(self.host, self.username, self.password, self.port,
                                                    self.database_name)
            if output[STATUS_KEY] == STATUS_FAILED:
                raise Exception("Error while creating sql connection.\n" + "Error: " + output[ERROR_KEY])

            conn = output[RESULT_KEY]
            conn_created = True

            # delete the sql data from history server
            for table_conf in airflow_table_config:
                # constructing query
                table_name = table_conf["table_name"]
                query = "DELETE FROM %s.%s WHERE dag_id='%s' OR dag_id like '%s" % (
                    self.database_name, table_name, dag_name, dag_name) + ".%'"
                # executing query
                logger.debug("Executing query - %s", query, extra=EXECUTION_CONTEXT.get_context())
                cursor = conn.cursor()
                cursor.execute(query)
                logger.debug("Query executed successfully", extra=EXECUTION_CONTEXT.get_context())
                cursor.close()

            conn.commit()
        except Exception as ex:
            logger.error("Error while removing sql data. ERROR - %s", str(traceback.format_exc()))
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
    OPTS = None
    try:
        OPTS, ARGS = getopt.getopt(
            sys.argv[1:], "f:c:",
            ["conf_file_,path=", "conf="
             "help"])
    except Exception as error:
        sys.stderr.write(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nERROR: " + str(error)}))
        usage(1)

    logger.debug("Parse the input arguments...", extra=EXECUTION_CONTEXT.get_context())
    for option, ARG in OPTS:
        if option in ("-h", "--help"):
            usage(1)
        elif option in ("-f", "--conf_file_path"):
            CONF_FILE_PATH = ARG
        elif option in ("-c", "--conf"):
            CONF = ARG

    # Check for all the mandatory arguments
    if CONF_FILE_PATH is None and CONF is None:
        sys.stderr.write(json.dumps({
            STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nERROR: Either JSON configuration file path "
                                                  "or JSON configuration string should be provided\n"}))
        usage(1)

    try:
        # Parse the configuration
        if CONF_FILE_PATH:
            with open(CONF_FILE_PATH) as conf_file:
                CONF = json.load(conf_file)
        else:
            CONF = json.loads(CONF)

        logger.debug("Successfully parsed configuration...", extra=EXECUTION_CONTEXT.get_context())

        # validate the configuration
        ENVIRONMENT = CONF.get("environment", None)
        TAG_NAME = CONF.get("tag_name", None)
        DAG_NAME = CONF.get("dag_name", None)
        CLEANUP_ALL = CONF.get("full_cleanup", None)
        AIRFLOW_HOME_DIR = CONF.get("airflow_home_dir", None)

        MYSQL_CONF = CONF["mysql_connection_details"]
        DB_HOST = MYSQL_CONF.get("host", None)
        DB_NAME = MYSQL_CONF.get("db_name", None)
        DB_USERNAME = MYSQL_CONF.get("username", None)
        DB_PASSWORD = MYSQL_CONF.get("password", None)
        DB_PORT = MYSQL_CONF.get("port", DEFAULT_MYSQL_PORT)

        logger.debug("Instantiate and call the AirflowRestoreUtility", extra=EXECUTION_CONTEXT.get_context())
        AIRFLOW_UTILITY = AirflowCleanupUtility(host=DB_HOST,
                                                username=DB_USERNAME,
                                                password=DB_PASSWORD,
                                                database_name=DB_NAME,
                                                airflow_home=AIRFLOW_HOME_DIR,
                                                port=DB_PORT)

        # create list of DAG names to process
        DAG_NAMES = []
        if CLEANUP_ALL == "True":
            for dag_filepath in glob.glob(AIRFLOW_HOME_DIR + "/dags/*.py"):
                DAG_NAME = dag_filepath.rsplit("/", 1)[1].rsplit(".", 1)[0]
                DAG_NAMES.append(DAG_NAME)
        else:
            DAG_NAMES.append(DAG_NAME)

        logger.debug("Processing DAGs - %s", str(DAG_NAMES), extra=EXECUTION_CONTEXT.get_context())

        # Process the DAGs
        for DAG_NAME in DAG_NAMES:
            logger.debug('Processing DAG - %s', DAG_NAME, extra=EXECUTION_CONTEXT.get_context())

            exec_status = AIRFLOW_UTILITY.cleanup(name=DAG_NAME,
                                                  airflow_table_config=AirflowUtilityConstants.airflow_table_config)

            if exec_status[STATUS_KEY] == STATUS_FAILED:
                sys.stderr.write(json.dumps(exec_status))
                sys.exit(1)

    except Exception as error:
        logger.error(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nError during airflow restore process",
                                 "ERROR": str(error)}), extra=EXECUTION_CONTEXT.get_context())
        sys.exit(1)
