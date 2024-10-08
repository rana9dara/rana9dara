

#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'
"""
Doc_Type            : Tech Products
Tech Description    : This utility is used for triggering and monitoring Airflow dags.
Pre_requisites      : Needs to be triggered from the node where Airflow is installed for and Airflow database (MySQL)
                      is accessible
Inputs              : Dag id, Optional Airflow database configuration, Run id required for getting dag status
Outputs             : Database query execution status and result. Output will be in the following format -
                      {
                          "status": "SUCCESS/FAILED",
                          "result": "RunId/ Dag status",
                          "error": "<Error if status FAILED>"
                      }
Example             : For triggering dag:
                      Input Dictionary -
                      {
                          "operation": "trigger",
                          "dag_id": "tutorial",
                          "sync": true,
                          "airflow_db_conf": {"db_host":"localhost","db_port":3306,"db_username":"root",
                          "db_password":"secret","db_name":"airflow"}
                      }

                      For getting dag status:
                      Input Dictionary -
                      {
                          "operation": "get_status",
                          "dag_id": "tutorial",
                          "run_id": "trigger__2017-09-21T00:31:40.480649",
                          "airflow_db_conf":{"db_host":"localhost","db_port":3306,"db_username":"root",
                          "password":"db_secret","db_name":"airflow"}
                      }
                      Command to Execute - python DagTriggerUtility.py --conf_file_path <input_dictionary_path>
Config_file         : None
"""

# Library and external modules declaration
import MySQLdb
import subprocess
import time
import traceback
import sys, os
import json
import getopt
from datetime import datetime
from LogSetup import logger
from ExecutionContext import ExecutionContext
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
import SecretsManagerUtility

MODULE_NAME = "DagTriggerUtility"

# The usage sting to be displayed to the user for the utility
USAGE_STRING = """
SYNOPSIS
    python DagTriggerUtility.py -f/--conf_file_path <conf_file_path> -c/--conf <conf>

    Where
        conf_file_path - Absolute path of the file containing JSON configuration
        conf - JSON configuration string

        Note: Either 'conf_file_path' or 'conf' should be provided.

"""


class DagTriggerUtility():
    """
    Class contains all the functions related to Dag Trigger utility
    """

    def __init__(self):
        """ Initialize all class level variables """
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(
            os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))

    def _execute_query(self, airflow_db_conf, query):
        """
        Purpose   :   Creates a Airflow database (MySQL) connection object and executes queries
        Input     :   Airflow database configuration (A dictionary containing - host, port, username, password &
                      database)
        Output    :   query output
        """
        try:
            # Query to fetch the dag state from Airflow database
            is_retry = True
            retry_count = 0
            cursor = None
            while is_retry and retry_count < CommonConstants.RETRY_COUNT:
                try:
                    host = airflow_db_conf["db_host"]
                    port = int(airflow_db_conf["db_port"])
                    username = airflow_db_conf["db_username"]
                    password = airflow_db_conf["db_password"]
                    # secret_name = airflow_db_conf["db_password"]
                    # password = SecretsManagerUtility.get_secret(secret_name,
                    # self.configuration.get_configuration(
                    #    [CommonConstants.ENVIRONMENT_PARAMS_KEY,
                    #     "secrets_manager_region"]))["password"]
                    database_name = airflow_db_conf["db_name"]
                    # Creates a MySQL connection
                    logger.info(host, port, username, database_name,password)
                    connection = MySQLdb.connect(host=host, port=port, user=username, passwd=password, db=database_name)
                    cursor = connection.cursor()
                    cursor.execute(query)
                    is_retry = False
                    logger.info("username: " + username,extra=self.execution_context.get_context())
                    logger.info("password: " + password,extra=self.execution_context.get_context())
                except:
                    retry_count += 1
                    logger.error("Error while connecting to the airflow database. Retrying - " + str(retry_count),
                                 extra=self.execution_context.get_context())
                    if retry_count == CommonConstants.RETRY_COUNT:
                        raise
            return cursor
        except:
            logger.error("Error while executing query",
                         extra=self.execution_context.get_context())
            raise

    def _get_dag_task_states(self, dag_id, run_id, airflow_db_config):
        """
        Purpose   :   Fetches the state of a dag from Airflow database
        Input     :   Dag id, Run id of the dag, Airflow database configuration (A dictionary containing -
                      host, port, username, password & database)
        Output    :   The dag state - running, success or failed
        """
        connection = None
        try:
            # Query to fetch the dag state from Airflow database
            query = " select distinct(task_instance.state) from " \
                    "task_instance left outer join dag_run " \
                    "on task_instance.dag_id=dag_run.dag_id where task_instance.execution_date = dag_run.execution_date " \
                    "and task_instance.dag_id  = '" + dag_id + "' and dag_run.run_id = '" + run_id + "'"
            logger.info("Running Query: " + query,
                        extra=self.execution_context.get_context())
            cursor = self._execute_query(airflow_db_conf=airflow_db_config, query=query)
            # If dag status is found return the status else raise exception
            if cursor.description is not None:
                if cursor.rowcount > 0:
                    for row in cursor:
                        status = str(row[0]).strip()
                        logger.debug("Status for dag " + dag_id + " and run id " + run_id + " - " + status,
                                     extra=self.execution_context.get_context())
                        if status.lower() == "upstream_failed" or status.lower() == "failed":
                            raise Exception("Dag with dag id: " + dag_id + " and run id: " + run_id +
                                            " is failed for one or more task")
                else:
                    raise Exception("Dag with dag id: " + dag_id + " and run id: " + run_id +
                                    " is failed for one or more task")
            else:
                raise Exception("Dag state not found for dag " + dag_id + " and run id " + run_id)
            cursor.close()
            return "success"

        except Exception as exp:
            logger.error("Error while getting dag state" + str(exp), extra=self.execution_context.get_context())
            raise exp

        finally:
            if connection:
                connection.close()

    def _get_dag_state(self, dag_id, run_id, airflow_db_config):
        """
        Purpose   :   Fetches the state of a dag from Airflow database
        Input     :   Dag id, Run id of the dag, Airflow database configuration (A dictionary containing -
                      host, port, username, password & database)
        Output    :   The dag state - running, success or failed
        """
        connection = None
        try:
            # Query to fetch the dag state from Airflow database
            query = "select state from dag_run where dag_id = '" + dag_id + "' and run_id = '" + run_id + "'"
            cursor = self._execute_query(airflow_db_conf=airflow_db_config, query=query)
            # If dag status is found return the status else raise exception
            if cursor.rowcount == 1 and cursor.description is not None:
                for row in cursor:
                    status = row[0].strip()
                    logger.debug("Status for dag " + dag_id + " and run id " + run_id + " - " + status,
                                 extra=self.execution_context.get_context())
                    if status.lower() != "failed":
                        return status
                    else:
                        raise Exception("Dag with dag id: " + dag_id + " and run id: " + run_id + " is failed")
            else:
                raise Exception("Dag state not found for dag " + dag_id + " and run id " + run_id)
            cursor.close()

        except:
            logger.error("Error while getting dag state", extra=self.execution_context.get_context())
            raise

        finally:
            if connection:
                connection.close()

    def _trigger_dag(self, dag_id):
        """
        Purpose   :   Triggers a Airflow dag using airflow cli
        Input     :   Dag id
        Output    :   The run id of the dag
        """
        try:
            is_retry = True
            retry_count = 0
            run_id = None
            output = None
            while is_retry and retry_count < CommonConstants.RETRY_COUNT:
                try:
                    # Create a run id for the dag
                    run_id = "trigger__{0}".format(datetime.now()).replace(" ", "T")
                    command = "airflow trigger_dag -r " + run_id + " " + dag_id
                    logger.debug("Triggering dag run command " + command, extra=self.execution_context.get_context())
                    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
                    output = process.communicate()[0]
                    if process.returncode == 0:
                        print("Command " + command + " executed successfully")
                    else:
                        raise Exception("Error while triggering command " + command + ". Command output - " +
                                        str(output))
                    is_retry = False
                except Exception as ex:
                    retry_count += 1
                    logger.error("Unable to trigger the DAG. Retrying - " + str(retry_count) + "Error output - " +
                                 str(output), extra=self.execution_context.get_context())
                    if retry_count == CommonConstants.RETRY_COUNT:
                        raise ex
            return run_id
        except:
            logger.error("Error while triggering dag id " + dag_id, extra=self.execution_context.get_context())
            raise

    def _is_dag_exists(self, dag_id, airflow_db_config):
        """
        Purpose   :   Check whether a dag entry exists in Airflow db
        Input     :   Dag id, Airflow database configuration (A dictionary containing -
                      host, port, username, password & database)
        Output    :   The run id of the dag
        """
        connection = None
        try:
            # Query to fetch the dag entry
            query = "select dag_id from dag where dag_id = '" + dag_id + "'"
            cursor = self._execute_query(airflow_db_conf=airflow_db_config, query=query)
            # If dag entry exists return true else return false
            if cursor.rowcount > 0 and cursor.description is not None:
                status = True
            else:
                status = False
            cursor.close()
            return status

        except:
            logger.error("Error while getting dag state", extra=self.execution_context.get_context())
            raise

        finally:
            if connection:
                connection.close()


    def get_dag_state(self, dag_id, run_id, airflow_db_config):
        """
        Purpose   :   Fetches the state of a dag from Airflow database
        Input     :   Dag name, Run id of the dag, Airflow database configuration (A dictionary containing -
                      host, port, username, password & database)
        Output    :   The dag state - running, success or failed
        """
        try:
            # Check for mandatory parameters
            if dag_id is None or run_id is None or airflow_db_config is None:
                raise Exception("Mandatory Dag Id or Run Id or Airflow db configuration not provided")
            status = self._get_dag_state(dag_id=dag_id, run_id=run_id, airflow_db_config=airflow_db_config)
            return {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCESS, CommonConstants.RESULT_KEY: status}
        except Exception as ex:
            logger.error("Error while getting dag state for dag " + dag_id +
                         " and run " + run_id + ". ERROR - " + str(traceback.format_exc()),
                         extra=self.execution_context.get_context())
            return {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED, CommonConstants.ERROR_KEY: str(ex)}


    def execute_dag(self, dag_id, airflow_db_config=None, sync=False,
                    dag_state_poll_interval=CommonConstants.DAG_STATE_POLL_INTERVAL,
                    dag_state_max_poll_timeout=CommonConstants.DAG_STATE_MAX_POLL_TIMEOUT, **kwargs):
        """
        Purpose   :   Executes a Airflow dag and polls for the status if synchronous calling is enabled.
                      If Airflow database config not passed synchronous calling and dag entry check will be skipped.
        Input     :   Dag id, Optional Airflow database configuration (A dictionary containing -
                      host, port, username, password & database), Synchronous calling enabled flag,
                      Dag state poll interval, Dag state poll max timeout
        Output    :   Dag execution status and run id if generated
        """
        try:
            # Check for mandatory parameters
            if dag_id is None:
                raise Exception("Mandatory Dag Id not provided")
            # If airflow database config is provided check whether the dag entry exists
            if airflow_db_config:
                dag_exists = self._is_dag_exists(dag_id=dag_id, airflow_db_config=airflow_db_config)
                logger.info(dag_exists, extra=self.execution_context.get_context())
                count = 0
                # Wait for the dag entry to be available for maximum DAG_ENTRY_MAX_POLL_TIMEOUT seconds
                while not dag_exists:
                    if count <= CommonConstants.DAG_ENTRY_MAX_POLL_TIMEOUT:
                        logger.info("Dag entry not found for dag " + dag_id + " in dag table. Waiting for "
                                    + str(CommonConstants.DAG_ENTRY_POLL_INTERVAL)
                                    + " seconds before checking again ...",
                                    extra=self.execution_context.get_context())
                        time.sleep(CommonConstants.DAG_ENTRY_POLL_INTERVAL)
                        count += CommonConstants.DAG_ENTRY_POLL_INTERVAL
                        dag_exists = self._is_dag_exists(dag_id=dag_id, airflow_db_config=airflow_db_config)
                    else:
                        raise Exception("Dag entry not found even after waiting for " +
                                        str(CommonConstants.DAG_ENTRY_MAX_POLL_TIMEOUT) + " seconds")

                logger.info("Dag entry found for dag " + dag_id + " in dag table",
                            extra=self.execution_context.get_context())

            else:
                logger.error("Cannot fetch dag entry for " + dag_id
                             + " as database config not provided. Disabling synchronous dag state polling",
                             extra=self.execution_context.get_context())
                # If airflow database config not provided disable synchronous dag state polling
                sync = False

            logger.info("Triggering dag " + dag_id, extra=self.execution_context.get_context())
            run_id = self._trigger_dag(dag_id=dag_id)
            logger.info("Dag " + dag_id + " triggered with run id " + run_id,
                        extra=self.execution_context.get_context())

            # If synchronous dag state polling is enabled check dag status
            if sync:
                logger.debug("Polling for dag execution status ...", extra=self.execution_context.get_context())
                status = self._get_dag_state(dag_id=dag_id, run_id=run_id, airflow_db_config=airflow_db_config)
                count = 0
                # Poll the dag status while the dag is running for maximum dag_state_max_poll_timeout seconds
                while status.lower() == CommonConstants.RUNNING_STATE.lower():
                    time.sleep(dag_state_poll_interval)
                    count += dag_state_poll_interval
                    if count <= dag_state_max_poll_timeout:
                        status = self._get_dag_state(dag_id=dag_id, run_id=run_id, airflow_db_config=airflow_db_config)
                        print("status:", status, count, dag_id, run_id, airflow_db_config)
                    else:
                        raise Exception("Maximum time out " + str(dag_state_max_poll_timeout) +
                                        " seconds reached for polling the dag")
                status = self._get_dag_task_states(dag_id=dag_id, run_id=run_id, airflow_db_config=airflow_db_config)
                print("task state:", status)
                if status.lower() == CommonConstants.STATUS_SUCCESS.lower():
                    print("Dag " + dag_id + " and run id " + run_id + " executed successfully")
                else:
                    raise Exception("Dag " + dag_id + " and run_id " + run_id +
                                    " execution failed with status - " + status)

            # Return the dag execution status along with the run id
            return {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCESS, CommonConstants.RESULT_KEY: run_id}

        except Exception as ex:
            logger.error("Error while executing dag " + dag_id + ". ERROR - " + str(traceback.format_exc()),
                         extra=self.execution_context.get_context())
            raise Exception(
                {CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED, CommonConstants.ERROR_KEY: str(ex)})


    # Print the usage for the Dag Trigger Utility
    def usage(status=1):
        sys.stdout.write(USAGE_STRING)
        sys.exit(status)


if __name__ == '__main__':
    conf_file_path = None
    conf = None
    opts = None
    try:
        opts, args = getopt.getopt(
            sys.argv[1:], "f:c:h",
            ["conf_file_path=", "conf="
                                "help"])
    except Exception as e:
        sys.stderr.write(json.dumps({CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                     CommonConstants.ERROR_KEY: "ERROR: " + str(e)}) + "\n")
        usage(1)

    # Parse the input arguments
    for option, arg in opts:
        if option in ("-h", "--help"):
            usage(1)
        elif option in ("-f", "--conf_file_path"):
            conf_file_path = arg
        elif option in ("-c", "--conf"):
            conf = arg

    # Check for all the mandatory arguments
    if conf_file_path is None and conf is None:
        sys.stderr.write(json.dumps({
            CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED, CommonConstants.ERROR_KEY: "ERROR: Either JSON "
                                                                                                  "configuration file "
                                                                                                  "path "
                                                                                                  "or JSON "
                                                                                                  "configuration "
                                                                                                  "string should be "
                                                                                                  "provided"}) + "\n")
        usage(1)

    dagid = None
    airflow_db_conf = None
    synch = False
    runid = None
    operation = None

    try:
        # Parse the configuration
        if conf_file_path:
            with open(conf_file_path) as conf_file:
                trigger_conf = json.load(conf_file)
        else:
            trigger_conf = json.loads(conf)

        if "dag_id" in trigger_conf:
            dagid = trigger_conf["dag_id"]
        if "airflow_db_conf" in trigger_conf:
            airflow_db_conf = trigger_conf["airflow_db_conf"]
        if "sync" in trigger_conf:
            synch = trigger_conf["sync"]
        if "run_id" in trigger_conf:
            runid = trigger_conf["run_id"]
        if "operation" in trigger_conf:
            operation = trigger_conf["operation"]

    except Exception as e:
        sys.stderr.write(json.dumps({CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                     CommonConstants.ERROR_KEY: "Error while parsing configuration."
                                                                " ERROR: " + str(e)}) + "\n")
        sys.exit(1)

    # Check whether the operation is trigger or get_status
    if not (operation and operation in ["trigger", "get_status"]):
        sys.stderr.write(json.dumps({CommonConstants.STATUS_KEY: CommonConstants.STATUS_FAILED,
                                     CommonConstants.ERROR_KEY: "Mandatory operation types "
                                                                "- trigger/get_status not provided"}) + "\n")
        sys.exit(1)

    trigger_utility = DagTriggerUtility()
    if operation == "trigger":
        # Execute trigger
        exec_status = trigger_utility.execute_dag(dag_id=dagid, airflow_db_config=airflow_db_conf, sync=synch)
    else:
        # Execute get status
        exec_status = trigger_utility.get_dag_state(dag_id=dagid, run_id=runid, airflow_db_config=airflow_db_conf)

    if exec_status[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_SUCCESS:
        sys.stdout.write(json.dumps(exec_status) + "\n")
        sys.exit(0)
    else:
        sys.stderr.write(json.dumps(exec_status) + "\n")
        sys.exit(1)

