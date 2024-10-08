import argparse
import json
import sys
import traceback
import os
import glob
from datetime import datetime
import MySQLdb
from ExecutionContext import ExecutionContext
from LogSetup import logger
from MoveCopy import MoveCopy
import AirflowUtilityConstants
from AirflowCleanupUtility import AirflowCleanupUtility
from MySQLConnectionManager import MySQLConnectionManager

__author__ = 'ZS Associates'
"""
Module Name         : Airflow Backup Utility
Purpose             :
Input               : dag name, full backup flag, mysql host, database name, username, password,
                        cleanup flag, s3 base path and airflow home directory
Output              : Backup the airlfow DAG, logs and sql data to S3
Pre-requisites      : Airflow DAG name and python file name should be same.
Last changed on     : 25th May 2018
Last changed by     : arunsingh.kushwaha@zs.com
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
    python AirflowBackupUitlity.py -f/--conf_file_path <conf_file_path> -c/--conf <conf> -r/--run_id <run_id> 
    -d/--docker_flag <flag>

    Where
        conf_file_path - Absolute path of the file containing JSON configuration
        conf - JSON configuration string
        run_id - Run ID
        docker_flag - Yes if the call is made from a container.
                      No if the call is made from EC2.

        Note: Either 'conf_file_path' or 'conf' should be provided.

"""

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MODULE_NAME = "AirflowBackupUtility"
EXECUTION_CONTEXT = ExecutionContext()
EXECUTION_CONTEXT.set_context({"current_module": MODULE_NAME})


class AirflowBackupUtility(object):
    """
    Class contains all the functions related to Airflow DAG backup
    """

    def __init__(self, host, username, password, database_name, s3_path, airflow_home,
                 port=DEFAULT_MYSQL_PORT):
        """
        Purpose: Fetches all the necessary credentials from environment_params.
        :param host: MYSQL host
        :param username: MYSQL username
        :param password: MYSQL password
        :param database_name: MYSQL DB name
        :param s3_path: Path in S3 used to back up.
        :param airflow_home: THe path where the airflow logs are present.
        :param port: Port
        """

        self.log_files = ["webserver.logs", "scheduler.logs"]
        self.move_copy_object = MoveCopy()
        self.host = host
        self.username = username
        self.password = password
        self.database_name = database_name
        self.s3_path = s3_path
        self.airflow_home = airflow_home
        self.port = port

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

    def get_subdag_path(self, dags_name):
        """
        Purpose   :   Fetctes subdag path from dag_name
        Input     :   dag name
        Output    :   subdag path
        """
        subdag_name = SUBDAG_PREFIX + dags_name
        subdag_path = self.airflow_home + "/dags/" + subdag_name
        if os.path.isdir(subdag_path):
            return subdag_name

        return None

    def backup(self, name, env_flag, unique_id, airflow_table_config):
        """
        Purpose   :   takes backup of DAG and stores in S3
        Input     :   dag name and cleanup flag
        Output    :   backup status
        """
        try:
            if env_flag == "Yes":
                base_s3_path = self.s3_path + "/container_logs/"
            elif env_flag == "No":
                base_s3_path = self.s3_path + "/backup/"
            base_s3_path = base_s3_path + datetime.strftime(datetime.today(), "%d-%m-%Y")
            if env_flag == "Yes":
                base_s3_path = base_s3_path + "/" + name + "_" + unique_id
            else:
                base_s3_path = base_s3_path + "/" + name
            # Backup dag file, logs and sql data
            self.backup_dagfile(name, base_s3_path + "/dags/")
            self.backup_logs(name, base_s3_path + "/logs/")
            self.backup_sql_data(name, airflow_table_config, base_s3_path + "/data/")
            if env_flag == "Yes":
                self.backup_server_logs(base_s3_path + "/server_logs/")
            # Return status and query result
            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: EMPTY}
        except Exception as ex:
            logger.error("Error during DAG backup. ERROR - %s", str(traceback.format_exc()),
                         extra=EXECUTION_CONTEXT.get_context())
            return {STATUS_KEY: STATUS_FAILED, ERROR_KEY: str(ex)}

    def backup_dagfile(self, name, target_path):
        """
        Purpose   :   takes backup of dag_name and stores in S3
        Input     :   dag name and cleanup flag
        Output    :   backup status
        """
        try:
            dag_path = self.airflow_home + "/dags/" + name + ".py"

            job_output = self.move_copy_object.s3_move_copy(action="copy", source_path_type="moveit",
                                                            source_path=dag_path,
                                                            target_path=target_path,
                                                            sse_flag=True)

            if job_output[STATUS_KEY] == STATUS_FAILED:
                raise Exception("Error while copying dag to s3.\n" + "Error: " + job_output[ERROR_KEY])

            # backup subdags if exists
            if self.get_subdag_path(name):
                sub_dag_name = self.get_subdag_path(name)
                dag_path = self.airflow_home + "/dags/" + sub_dag_name
                target_path = target_path + sub_dag_name
                job_output = self.move_copy_object.s3_move_copy(action="copy", source_path_type="moveit",
                                                                source_path=dag_path,
                                                                target_path=target_path,
                                                                sse_flag=True)

                if job_output[STATUS_KEY] == STATUS_FAILED:
                    raise Exception("Error while copying dag to s3.\n" + "Error: " + job_output[ERROR_KEY])

        except Exception as ex:
            logger.error("Error while DAG backup. ERROR - %s", str(traceback.format_exc()),
                         extra=EXECUTION_CONTEXT.get_context())
            raise ex

    def backup_logs(self, name, target_path):
        """
        Purpose   :   takes backup of dag_name and stores in S3
        Input     :   dag name and cleanup flag
        Output    :   backup status
        """
        try:
            logs_path = self.airflow_home + "/logs/" + name + "/"
            if os.path.exists(logs_path):
                target_path_ = target_path
                job_output = self.move_copy_object.s3_move_copy(action="copy", source_path_type="moveit",
                                                                source_path=logs_path,
                                                                target_path=target_path_,
                                                                sse_flag=True)

                if job_output[STATUS_KEY] == STATUS_FAILED:
                    raise Exception("Error while copying logs to s3.\n" + "Error: " + job_output[ERROR_KEY])

            # backup subdag logs if exists
            if self.get_subdag_path(name):
                for logs_path_ in glob.glob(logs_path + ".*"):
                    target_path_ = target_path + logs_path_.split("/")[-1]
                    logger.debug(logs_path_, extra=EXECUTION_CONTEXT.get_context())
                    logger.debug(target_path_, extra=EXECUTION_CONTEXT.get_context())
                    job_output = self.move_copy_object.s3_move_copy(action="copy", source_path_type="moveit",
                                                                    source_path=logs_path_,
                                                                    target_path=target_path_,
                                                                    sse_flag=True)
                    if job_output[STATUS_KEY] == STATUS_FAILED:
                        raise Exception("Error while copying logs to s3.\n" + "Error: " + job_output[ERROR_KEY])
        except Exception as ex:
            logger.error("Error while logs backup . ERROR - %s", str(traceback.format_exc()),
                         extra=EXECUTION_CONTEXT.get_context())
            raise ex

    def backup_sql_data(self, name, airflow_table_config, target_path):
        """
        Purpose   :   takes backup of dag metadata and stores in S3
        Input     :   dag name , airflow_table_config and target_path
        Output    :   backup status
        """
        conn_created = False
        conn = None
        try:
            output = self.create_and_get_connection(self.host, self.username, self.password, self.port,
                                                    self.database_name)
            if output[STATUS_KEY] == STATUS_FAILED:
                raise Exception("Error while creating sql connection.\n" + "Error: " + output[ERROR_KEY])

            if output[STATUS_KEY] == STATUS_SUCCESS:
                conn = output[RESULT_KEY]
                conn_created = True
            else:
                return output

            with open(DEFAULT_SQL_FILENAME, 'w') as files:
                for table_conf in airflow_table_config:
                    # constructing query
                    columns = ["`" + x + "`" for x in table_conf["columns"]]
                    columns = ", ".join(columns)
                    table_name = table_conf["table_name"]
                    query = "SELECT %s FROM %s.%s WHERE dag_id='%s' OR dag_id like '%s" % (
                        columns, DB_NAME, table_name, name, name) + ".%'"

                    # executing query
                    logger.debug("Executing query - %s", query, extra=EXECUTION_CONTEXT.get_context())
                    cursor = conn.cursor()
                    cursor.execute(query)
                    logger.debug("Query executed successfully", extra=EXECUTION_CONTEXT.get_context())

                    formatter = table_conf["formatter"]
                    insert_statement = "INSERT INTO {}.{} ({}) VALUES ({});"

                    for row in cursor.fetchall():
                        formatted_row = formatter % row
                        formatted_row = formatted_row.replace("'None'", 'NULL')
                        formatted_row = formatted_row.replace('"None"', 'NULL')
                        formatted_row = formatted_row.replace("None", 'NULL')
                        logger.debug("%s", insert_statement.format(DB_NAME, table_name, columns, formatted_row),
                                     extra=EXECUTION_CONTEXT.get_context())
                        files.write(insert_statement.format(DB_NAME, table_name, columns, formatted_row) + "\n")

                    cursor.close()

            job_output = self.move_copy_object.s3_move_copy(action="copy", source_path_type="moveit",
                                                            source_path=DEFAULT_SQL_FILENAME,
                                                            target_path=target_path,
                                                            sse_flag=True)

            if job_output[STATUS_KEY] == STATUS_FAILED:
                raise Exception("Error while copying sql file to s3.\n" + "Error: " + job_output[ERROR_KEY])

        except Exception as ex:
            logger.error("Error while executing query. ERROR - %s", str(traceback.format_exc()),
                         extra=EXECUTION_CONTEXT.get_context())
            raise ex
        finally:
            if conn_created and conn:
                self.close_connection(conn)

    def backup_server_logs(self, target_path):
        """
        Purpose: TO back up and clean the webserver and scheduler logs.
        :param target_path: The S3 path to save to.
        :return:
        """
        for item in self.log_files:
            if os.path.isfile(self.airflow_home + "/logs/" + item):
                logger.debug("SERVER_LOGS SOURCE: %s", self.airflow_home + "/logs/" + item,
                             extra=EXECUTION_CONTEXT.get_context())
                logger.debug("SERVER_LOGS TARGET: %s", target_path, extra=EXECUTION_CONTEXT.get_context())
                job_output = self.move_copy_object.s3_move_copy(action="copy", source_path_type="moveit",
                                                                source_path=self.airflow_home + "/logs/" + item,
                                                                target_path=target_path + item,
                                                                sse_flag=True)
                if job_output[STATUS_KEY] == STATUS_FAILED:
                    raise Exception("Error while copying logs to s3.\n" + "Error: " + job_output[ERROR_KEY])
        if CLEANUP:
            for clean_log_file in AIRFLOW_UTILITY.log_files:
                logger.debug("PATH: %s/logs/%s", str(AIRFLOW_UTILITY.airflow_home), clean_log_file,
                             extra=EXECUTION_CONTEXT.get_context())
                open(AIRFLOW_UTILITY.airflow_home + "/logs/" + clean_log_file, "w").close()


# Print the usage for the Database Utility
def usage(status=1):
    """
    Purpose: Display the help page on the command line.
    :param status: 1
    :return:
    """
    sys.stdout.write(USAGE_STRING)
    sys.exit(status)


if __name__ == '__main__':
    # Setup basic logging info

    CONF_FILE_PATH = None
    CONF = None
    try:
        PARSER = argparse.ArgumentParser(description="Python utility to move/copy log files from source to destination")
        PARSER.add_argument("-f", "--conf_file_path", help="Config file path.")
        PARSER.add_argument("-c", "--conf", help="Config json input.")
        PARSER.add_argument("-r", "--run_id", required=False, help="Run ID.")
        PARSER.add_argument("-d", "--docker_flag", choices=['Yes', 'No'], required=True, help="Is it Docker? Yes/No")
        # parser.add_argument("--help", help="Help")
        ARGS = PARSER.parse_args()

    except Exception as error:
        sys.stderr.write(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nERROR: " + str(error)}))
        usage(1)

    logger.debug("Parse the input arguments...", extra=EXECUTION_CONTEXT.get_context())

    RUN_ID = ""
    DOCKER_FLAG = ""
    if ARGS.conf_file_path is not None:
        CONF_FILE_PATH = ARGS.conf_file_path
    elif ARGS.conf is not None:
        CONF = ARGS.conf
    if ARGS.run_id is not None:
        RUN_ID = ARGS.run_id
    if ARGS.docker_flag is not None:
        DOCKER_FLAG = ARGS.docker_flag

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
        FULL_BACKUP = CONF.get("full_backup", None)
        CLEANUP = CONF.get("cleanup", None)
        S3_BASE_PATH = CONF.get("s3_base_path", None)
        AIRFLOW_HOME_DIR = CONF.get("airflow_home_dir", None)
        DB_SECRET_NAME_S3_REGION = CONF.get("s3_region", None)

        MYSQL_CONF = CONF["mysql_connection_details"]

        DB_HOST = MYSQL_CONF.get("host", None)
        DB_NAME = MYSQL_CONF.get("db_name", None)
        DB_USERNAME = MYSQL_CONF.get("username", None)
        DB_PASSWORD_SECRET_NAME = MYSQL_CONF.get("password", None)
        DB_PORT = MYSQL_CONF.get("port", DEFAULT_MYSQL_PORT)

        secret_password = MySQLConnectionManager().get_secret(DB_PASSWORD_SECRET_NAME, DB_SECRET_NAME_S3_REGION)
        DB_PASSWORD = secret_password['password']

        if DAG_NAME is None and FULL_BACKUP is not True:
            raise Exception("Full backup must be True when dag_name is not provided...")

        logger.info("Instantiate and call the AirflowBackupUtility", extra=EXECUTION_CONTEXT.get_context())
        AIRFLOW_UTILITY = AirflowBackupUtility(host=DB_HOST,
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
        if FULL_BACKUP:
            for dag_filepath in glob.glob(AIRFLOW_HOME_DIR + "/dags/*.py"):
                DAG_NAME = dag_filepath.rsplit("/", 1)[1].rsplit(".", 1)[0]
                # if "_" in dag_name:
                DAG_NAMES.append(DAG_NAME)
        else:
            DAG_NAMES.append(DAG_NAME)

        if DOCKER_FLAG == "No":
            AIRFLOW_UTILITY.backup_server_logs(target_path=AIRFLOW_UTILITY.s3_path +
                                               "/backup/" +
                                               datetime.strftime(datetime.today(), "%d-%m-%Y") +
                                               "/server_logs/")

        # Process the DAGs
        for DAG_NAME in DAG_NAMES:
            logger.debug('Processing DAG - %s', DAG_NAME, extra=EXECUTION_CONTEXT.get_context())
            logger.info("BACKING UP", extra=EXECUTION_CONTEXT.get_context())
            exec_status = AIRFLOW_UTILITY.backup(name=DAG_NAME, env_flag=DOCKER_FLAG, unique_id=RUN_ID,
                                                 airflow_table_config=AirflowUtilityConstants.airflow_table_config)

            if exec_status[STATUS_KEY] == STATUS_FAILED:
                sys.stderr.write(json.dumps(exec_status))
                sys.exit(1)
            logger.info("BACK-UP DONE.", extra=EXECUTION_CONTEXT.get_context())
            logger.info("CLEANING UP", extra=EXECUTION_CONTEXT.get_context())
            if CLEANUP:
                exec_status = CLEANUP_UTILITY.cleanup(name=DAG_NAME,
                                                      airflow_table_config=AirflowUtilityConstants.airflow_table_config)
                if exec_status[STATUS_KEY] == STATUS_FAILED:
                    sys.stderr.write(json.dumps(exec_status))
                    sys.exit(1)
            logger.info("CLEAN-UP DONE.", extra=EXECUTION_CONTEXT.get_context())

    except Exception as error:
        traceback.print_exc()
        logger.error(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nError during airflow backup process",
                                 "ERROR": str(error)}), extra=EXECUTION_CONTEXT.get_context())
        sys.exit(1)

