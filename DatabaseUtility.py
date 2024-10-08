#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'
"""
Module Name         : DatabaseUtility
Purpose             : Executes a query on PostgreSQL database
Input               : Query to execute, optional database connection object, database host, port, username, password,
                      optional database name to connect and auto commit flag
Output              : The result of the query
Pre-requisites      :
Last changed on     : 19th July 2017
Last changed by     : rdas@amgen.com
Reason for change   : Initial development
"""

# Library and external modules declaration
#import psycopg2
import logging
import json
import sys, os
import glob
import datetime
import getopt
import traceback
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Module level constants
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_KEY = "status"
ERROR_KEY = "error"
RESULT_KEY = "result"

EMPTY = ""
DEFAULT_POSTGRESQL_PORT = 5432
DEFAULT_AUTOCOMMIT_ENABLED = True

# The usage sting to be displayed to the user for the utility
USAGE_STRING = """
SYNOPSIS
    python DatabaseUtility.py -f/--conf_file_path <conf_file_path> -c/--conf <conf>

    Where
        conf_file_path - Absolute path of the file containing JSON configuration
        conf - JSON configuration string

        Note: Either 'conf_file_path' or 'conf' should be provided.

"""

class DatabaseUtility():
    """
    Class contains all the functions related to Database utility
    """

    def __init__(self):
        # Initialize all class level variables
        pass

    def create_and_get_connection(self, host, username, password, port=DEFAULT_POSTGRESQL_PORT, database_name=EMPTY,
                                  auto_commit=DEFAULT_AUTOCOMMIT_ENABLED):
        try:
            # Check for mandatory parameters
            if host is None or username is None or password is None:
                raise Exception("Please provide host, username and password for creating connection")

            if database_name is None:
                logging.warning("Database name is not provided...")

            # Create a connection to PostgreSQL database
            connection = psycopg2.connect(host=host, port=port, user=username, password=password, dbname=database_name)
            logging.debug("PostgreSQL connection created for Host: " + host + ", Port: " + str(port) + ", Username: " +
                          username + ", Password: *******, Database: " + database_name)
            if auto_commit:
                connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                logging.debug("Autocommit enabled for connection")

            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: connection}

        except Exception as ex:
            logging.error("Error while creating database connection. ERROR - " + str(traceback.format_exc()))
            return {STATUS_KEY: STATUS_FAILED, ERROR_KEY: str(ex)}

    def execute(self, query, conn=None, host=None, username=None, password=None, port=DEFAULT_POSTGRESQL_PORT,
                database_name=EMPTY, auto_commit=DEFAULT_AUTOCOMMIT_ENABLED):
        """
        Purpose   :   Executes a query on PostgreSQL. If the connection object is not passed it will create a new
                      connection with all the connection related inputs and closes the connection when the query
                      finishes. If the connection is passed it will use the same connection to execute the query and it
                      will not be closed unless the call to close_connection method is made
        Input     :   Query to execute, Optional PostgreSQL connection object fetched from the
                      create_and_get_connection method. If the connection object is not passed, following are required -
                      PostgreSQL host, username, password, optional port (Default is 5432), optional database name
                      (Default is empty), optional flag to enable/disable auto commit (Default is true)
        Output    :   PostgreSQL query result
        """
        conn_created = False
        try:
            # Check whether query is passed
            if query is None:
                raise Exception("Query not passed")
            # Check whether connection is passed
            if conn and auto_commit:
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                logging.debug("Autocommit enabled for connection")
            if conn is None:
                # Check for mandatory arguments if connection not passed
                if host is None or username is None or password is None:
                    raise Exception("Database connection not provided. Please provide host, username "
                                    "and password for creating connection")
                elif database_name is None:
                    logging.warning("Database name is not provided...")
                else:
                    # Create a new connection
                    logging.debug("Connection not found for query. Creating new connection")
                    output = self.create_and_get_connection(host, username, password, port, database_name, auto_commit)
                    if output[STATUS_KEY] == STATUS_SUCCESS:
                        conn = output[RESULT_KEY]
                        conn_created = True
                    else:
                        return output

            cursor = conn.cursor()
            cursor.execute(query)
            logging.debug("Query executed successfully")
            results = []
            # Check whether there is any output for the query execution
            if cursor.rowcount > 0 and cursor.description is not None:
                # If output is present add it to the result list
                for row in cursor:
                    result = {}
                    count = 0
                    for i in cursor.description:
                        if row[count] is None:
                            result[i[0]] = None
                        else:
                            result[i[0]] = str(row[count])
                        count += 1
                    results.append(result)
            else:
                logging.debug("No result found for the query to fetch")

            # Return status and query result
            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: results}

        except Exception as ex:
            logging.error("Error while executing query. ERROR - " + str(traceback.format_exc()))
            return {STATUS_KEY: STATUS_FAILED, ERROR_KEY: str(ex)}

        finally:
            if conn_created and conn:
                self.close_connection(conn)

    def postgres_csv_export(self, query, location, conn, delimeter="|", file_extension="csv", header=False):
        try:
            # Is valid postgres connection...?
            if conn is None:
                raise Exception("Connection not found...")

            # Export as table as csv
            cur = conn.cursor()
            header = "" if header == False else "HEADER"
            filepath = os.path.join(location, datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv")
            export_query = "COPY ({}) TO STDOUT WITH CSV {} delimiter '{}'".format(query, header, delimeter)
            logging.debug("Query - " + export_query)
            with open(filepath, 'w') as f:
                cur.copy_expert(export_query, f)
            conn.commit()
            cur.close()

            # Return status and query result
            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: EMPTY}
        except Exception as ex:
            logging.error("Error in csv export. ERROR - " + str(traceback.format_exc()))
            return {STATUS_KEY: STATUS_FAILED, ERROR_KEY: str(ex)}

    def postgres_csv_import(self, conn, schema, table, location, delimeter="|", file_extension="csv", header=False):
        try:
            # Is valid postgres connection...?
            if conn is None:
                raise Exception("Connection not found...")
            # Export as table as csv
            cur = conn.cursor()
            schema_table = schema +"."+ table
            header = "" if header == False else "HEADER"
            import_query = "copy {} from STDIN CSV {} delimiter '{}'".format(schema_table, header, delimeter)
            logging.debug("Query - " + import_query)
            for file_ in glob.glob(location + "/*." + file_extension):
                with open(file_, 'r') as f:
                    cur.copy_expert(import_query, f)
            conn.commit()
            cur.close()

            # Return status and query result
            return {STATUS_KEY: STATUS_SUCCESS, RESULT_KEY: EMPTY}
        except Exception as ex:
            logging.error("Error in csv import. ERROR - " + str(traceback.format_exc()))
            return {STATUS_KEY: STATUS_FAILED, ERROR_KEY: str(ex)}

    def close_connection(self, conn=None):
        """
        Purpose   :   Closes the PostgreSQL connection
        Input     :   PostgreSQL connection object
        Output    :   None
        """
        try:
            conn.close()
            logging.debug("PostgreSQL connection closed")
        except Exception as ex:
            logging.error("Error while closing database connection. ERROR - " + str(traceback.format_exc()))
            return {STATUS_KEY: STATUS_FAILED, ERROR_KEY: str(ex)}


# Print the usage for the Database Utility
def usage(status=1):
    sys.stdout.write(USAGE_STRING)
    sys.exit(status)

if __name__ == '__main__':
    # Setup basic logging info
    logging.basicConfig(level=logging.DEBUG)

    conf_file_path = None
    conf = None
    opts = None
    try:
        opts, args = getopt.getopt(
            sys.argv[1:], "f:c",
            ["conf_file_path=", "conf="
             "help"])
    except Exception as e:
        sys.stderr.write(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nERROR: " + str(e)}))
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
            STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nERROR: Either JSON configuration file path "
                                                  "or JSON configuration string should be provided\n"}))
        usage(1)

    db_host = None
    db_port = DEFAULT_POSTGRESQL_PORT
    db_username = None
    db_password = None
    db_name = EMPTY
    db_query = None
    try:
        # Parse the configuration
        if conf_file_path:
            with open(conf_file_path) as conf_file:
                db_conf = json.load(conf_file)
        else:
            db_conf = json.loads(conf)

        "host=dirdidev.ckkp7bzktfbr.us-west-2.rds.amazonaws.com user=root dbname=dirdidev port=5432 password=dirdipassword"
        if "db_host" in db_conf:
            db_host = db_conf["db_host"]
        if "db_port" in db_conf:
            db_port = int(db_conf["db_port"])
        if "db_username" in db_conf:
            db_username = db_conf["db_username"]
        if "db_password" in db_conf:
            db_password = db_conf["db_password"]
        if "db_name" in db_conf:
            db_name = db_conf["db_name"]
        if "db_query" in db_conf:
            db_query = db_conf["db_query"]

    except Exception as e:
        sys.stderr.write(json.dumps({STATUS_KEY: STATUS_FAILED, ERROR_KEY: "\nError while parsing configuration."
                                                                           " ERROR: " + str(e)}))
        sys.exit(1)

    # Instantiate and call the Database utility
    db_utility = DatabaseUtility()
    exec_status = db_utility.execute(host=db_host, port=db_port, username=db_username, password=db_password,
                                     database_name=db_name, query=db_query)

    if exec_status[STATUS_KEY] == STATUS_SUCCESS:
        sys.stdout.write(json.dumps(exec_status))
        sys.exit(0)
    else:
        sys.stderr.write(json.dumps(exec_status))
        sys.exit(1)

