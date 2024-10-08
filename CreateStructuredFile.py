"""
  Module Name         :   CreateStructuredFile
  Purpose             :   This module performs below operation:
                              a. Call data integration notebook
                              b. Perform logging in delta table
  Input               :   config_file_path
  Output              :   Return status SUCCESS/FAILED
  Pre-requisites      :
  Last changed on     :   07th Nov 2019
  Last changed by     :   Shashwat Shukla
  Reason for change   :   Updated for Eudract and WHO
"""

import getopt
import json
from urllib.parse import urlparse
import ntpath
import re
import sys
import time
import traceback
from functools import reduce

import logging
import os
from os import path
import datetime
from datetime import datetime
from pytz import timezone
import boto3
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


SERVICE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, SERVICE_DIR_PATH)
CODE_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIR_PATH, "../"))
sys.path.insert(1, CODE_DIR_PATH)

STATUS_KEY = "status"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCESS"
ERROR_KEY = "error"
RESULT_KEY = "result"
PARQUET_FILE_PATH_KEY = "parquet_file_path"
disease_name_filter = ""
DISEASE_AREA_COL_VALUE_KEY = "disease_area_col_value"
from CommonUtils import CommonUtils
from DILogSetup import get_logger
logging = get_logger()


def remove_non_ascii(text):
    """
    Purpose     :   To remove non ascii characters
    Input       :   input text
    Output      :   Return text with non ascii charaters
    """
    return re.sub(r"[^\x00-\x7F]+", "", text)


def get_pmc_id(id_list):
    """
    Purpose     :   This function returns only pmc id among all ids
    Input       :   id list
    Output      :   Return pmc id
    """
    pmc_id = ""
    if id_list is not None:
        for id in id_list:
            if id is not None:
                if id.startswith("PMC"):
                    pmc_id = id
    return pmc_id


def parse_list(nested_list):
    trial_list = []
    if nested_list is not None:
        for each_list in nested_list:
            test_list = str(each_list).strip("[").strip("]").replace(",", ";")
            test_list = str(test_list).replace("u'", "").replace("'", "")
            trial_list.append(test_list)
    return "| ".join(trial_list)

def parse_disease_name(therpeutic_area):
    outer_disease_name = []
    for ta_info in therpeutic_area:
        inner_disease_name = []
        for disease_info in ta_info["trialDiseases"]:
            inner_disease_name.append(disease_info["name"])
        outer_disease_name.append("^".join(inner_disease_name))
    output = "|".join(outer_disease_name)
    return output

def parse_trail(main_key):
    key_list = []
    if main_key:
        for every_key in main_key:
            key_list.append(str(every_key))
    return "| ".join(key_list)


def find(key, obj):
    """
    Purpose     :   this function finds wether instance is of type
                    list or dictionary
    Input       :   kay , object
    Output      :   Returns result
    """
    if isinstance(obj, list):
        for d in obj:
            for result in find(key, d):
                yield result
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                yield v
            elif isinstance(v, dict):
                for result in find(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in find(key, d):
                        yield result
    if isinstance(obj, Row):
        yield obj[key]


def parse_struct_of_list(data, key):
    """
        Purpose     :   To concat values of list which is the value
                       of key in list of key-value pairs
    Input       :   input text, key whose value to be returned
    Output      :   Return flattened text with seperators
    """
    first = 1
    output = ""
    if data:
        for subset in data:
            inner_list = [str(elem) for elem in subset[key]]
            inner_str = "; ".join(inner_list)
            output = inner_str if first == 1 else "{}| {}".format(output, inner_str)
            first = 0
    return output


def parse_struct_of_struct(data, parent, child):
    """
        Purpose     :   To concat values of child key in parent list of key-value
                        pairs within list of key-value pairs
    Input       :   input text, parent whose child to be flattened, child whose
                    value to be returned
    Output      :   Return flattened text with seperators
    """
    output = ""
    first_elem = 1
    first_subelem = 1
    if data:
        for parent_struct in data:
            output = "" if first_elem == 1 else output + "| "
            first_elem = 0
            child_list = parent_struct[parent]
            first_subelem = 1
            for child_struct in child_list:
                val = child_struct[child]
                output = "{}{}".format(output, val) if first_subelem == 1 else "{}; {}".format \
                    (output, val)
                first_subelem = 0
    return output

def parsed_double_nested_list(nested_list):
    parsed_str = ''
    if nested_list:
        for item_list in nested_list:
            if item_list:
                p_str = ''
                for item in item_list:
                    if item:
                        p_str = p_str + str(item) + ';'
                parsed_str = parsed_str + p_str[:-1] + '|'
    return parsed_str

def concat_list(seperator, input_list):
    """
    Purpose     :   This function concat the input list with
                    input seperator
    Input       :   seperator , input list
    Output      :   Return string with concated values
    """
    input_str = ""
    if input_list:
        for item in input_list:
            if item:
                input_str = input_str + seperator + item
            else:
                input_str = input_str + seperator
        return input_str[1:]
    else:
        return input_str


def parse_nested_list(nested_list):
    """
    Purpose     :   This function concat the input nested
                    list with pipe seperator
    Input       :   nested list
    Output      :   Return string with pipe seperated concated values
    """
    parsed_str = []
    if nested_list:
        for item_list in nested_list:
            if item_list:
                for item in item_list:
                    if item:
                        parsed_str.append(item)
    return "|".join(parsed_str)


# UDF: check whether given therapeutic area belongs to multiple myeloma  or ALL
def check_disease(trialTherapeuticAreas_list):
    """
    Purpose     :   This function check the disease in
                    trialTherapeuticAreas_list tag
    Input       :   trialTherapeuticAreas_list
    Output      :   Return flag value
    """
    flag = "False"
    try:
        for area in trialTherapeuticAreas_list:
            for disease in area["trialDiseases"]:
                if disease["name"].lower() == disease_name_filter.lower():
                    flag = "True"
                    break
    except:
        return flag
    return flag


# parse key
def parse_key(x, key):
    """
    Purpose     :   This function parses key
    Input       :   key,x = array
    Output      :   Return string
    """
    res = list(find(key, x))
    return (
        ""
        if len(res) == 0
        else "".join(remove_non_ascii(e) for e in res if e is not None)
    )


# UDF: parse array within array
def parse_arrs(x):
    """
    Purpose     :   This function parse array within array
    Input       :   x = array
    Output      :   Return pipe seperated string
    """
    if x:
        return "| ".join(
            ", ".join(i for i in e if i is not None) for e in x if e is not None
        )
    else:
        ""

# UDF: parse array within array specific for drug
def parse_arrs_drug(x):
    """
    Purpose     :   This function parse array within array
    Input       :   x = array
    Output      :   Return pipe seperated string
    """
    if x:
        return "| ".join(
            "; ".join(i for i in e if i is not None) for e in x if e is not None
        )
    else:
        ""

# UDF: parse directMechanism
def parse_dm(x, key):
    """
    Purpose     :   This function parse directMechanism tag
    Input       :   key, x = array
    Output      :   Return pipe seperated string
    """
    res = list(find(key, x))
    return "| " if len(res) == 0 else ", ".join(str(e) for e in res)


def parse_ps(x, key, key2):
    """
    Purpose       :  This function parse PatientSegments tag
    Input     :  key, key2, x = array
    Output    :  Return pipe seperated string
    """
    ps = []
    ps2 = []
    ps3 = []
    for ta_info in x:
        if "trialDiseases" in ta_info:
            for td_info in ta_info["trialDiseases"]:
                ps2 = ""
                for y in find(key, td_info):
                    l = []
                    l = list(find(key2, y))
                    l = [str(i) for i in l]
                    ps.append("#".join(l))
        else:
            ps.append("")
        ps3.append("^".join(ps))
        ps = []
    return "|".join(ps3)

# UDF: parse PatientEndpoint
def parse_pe(x, key, key2):
    """
    Purpose     :   This function parse PatientEndpoint tag
    Input       :   key, key2, x = array
    Output      :   Return pipe seperated string
    """
    for y in find(key, x):
        return "| ".join(list(find(key2, y)))


# UDF: get nct_id from trialProtocolIDs_list
def get_nct_id(trialProtocolIDs_list):
    """
    Purpose     :   This function returns nct id in trialProtocolIDs_list tag
    Input       :   trialProtocolIDs_list
    Output      :   Return pipe seperated string
    """
    id_list = []
    if not isinstance(trialProtocolIDs_list, list):
        return ""
    else:
        for ID in trialProtocolIDs_list:
            if ID and ID.startswith("NCT"):
                id_list.append(ID)
    return "|".join(id_list)

# UDF: get protocol_id from trialProtocolIDs_list
def get_protocol_id(trialProtocolIDs_list):
    """
    Purpose     :   This function returns nct id in trialProtocolIDs_list tag
    Input       :   trialProtocolIDs_list
    Output      :   Return pipe seperated string
    """
    id_list = []
    if not isinstance(trialProtocolIDs_list, list):
        return ""
    else:
        if len(trialProtocolIDs_list) == 0:
            return ""
        else:
            for ID in trialProtocolIDs_list:
                if ID is None:
                    id_list.append("")
                else:
                    id_list.append(ID)

    return "|".join(id_list)

# UDF: get eudract_number from trialProtocolIDs_list
def get_eudract_number(trialProtocolIDs_list):
    """
    Purpose     :   This function returns nct id in trialProtocolIDs_list tag
    Input       :   trialProtocolIDs_list
    Output      :   Return pipe seperated string
    """
    id_list = []
    string_to_search = "EudraCT Number: "
    if not isinstance(trialProtocolIDs_list, list):
        return ""
    else:
        for element in trialProtocolIDs_list:
            if element and element.startswith(string_to_search):
                id_list.append(element[len(string_to_search):].strip())
    return "|".join(id_list)


def parse_keys(key, obj):
    """
    Purpose     :   This function returns pipe sperator string
    Input       :   key, object = list or dictionary
    Output      :   Return string
    """
    result_list = list(find(key, obj))
    return ", ".join(str(x) for x in result_list)


class CreateStructuredFile:
    """
    Purpose             :   This module performs below operation:
                              a. Call data integration notebook
                              b. Perform logging in delta table
    Input               :   config_file_path
    Output              :   Return status SUCCESS/FAILED
    """

    def __init__(self, access_key=None, secret_key=None):

        self.s3_client = boto3.client("s3")
        self.data_source = self.s3_input_data_path = self.schema_path = None
        self.disease_name = self.s3_parquet_output_path = None
        self.spark_query_path = None
        self.stitching_flag = self.deleted_id_path = None
        self.stitching_columns = self.deleted_id_list = None
        self.step_name = None
        self.spark = self.sqlContext = None
        self.create_investigator_table = None

    def initialize_variables(self, input_config):
        """
        Purpose     :   To initialize input variables
        Input       :   application_variable = configuration parameters needed for
                        initialize input variables
        Output      :   Return status SUCCESS/FAILED
        """
        status_message = ""
        try:
            status_message = (
                    "starting initialize_variables with input_config=" + json.dumps(input_config)
            )
            logging.debug(str(status_message))
            self.data_source = input_config["data_source"]

            if (self.data_source == "eudract"):
                self.parquet_temp_path = input_config["parquet_temp_path"]
                self.parquet_final_path = input_config["parquet_final_path"]
                self.payload_id = input_config["payload_id"]
                self.trigger_id = input_config["trigger_id"]
                self.csv_temp_path = input_config["csv_temp_path"]
                self.csv_temp_path = os.path.join(self.csv_temp_path, self.trigger_id)
                # to remove data from s3 from trigger_id
                self.csv_temp_path_till_trigger_id = self.csv_temp_path
                self.csv_temp_path = os.path.join(self.csv_temp_path, self.payload_id)
                self.parquet_temp_path = os.path.join(self.parquet_temp_path, self.trigger_id)
                self.parquet_temp_path_till_trigger_id = self.parquet_temp_path
                self.parquet_temp_path = os.path.join(self.parquet_temp_path, self.payload_id)
                parquet_file_name = "eudract_" + input_config[
                    DISEASE_AREA_COL_VALUE_KEY] + "_" + self.trigger_id + "_" + self.payload_id + ".parquet"
                self.parquet_final_path = os.path.join(self.parquet_final_path, parquet_file_name)
                self.disease_name = input_config[DISEASE_AREA_COL_VALUE_KEY]
            else:
                self.s3_input_data_path = input_config["s3_input_data_path"]
                self.s3_parquet_output_path = input_config["s3_parquet_output_path"]
                self.disease_name = input_config["disease_name"]
                self.data_source = input_config["data_source"]
                self.temp_s3_path = input_config["temp_s3_path"]

            if self.data_source == "who" and "create_investigator_table" in input_config.keys():
                self.create_investigator_table = input_config["create_investigator_table"]
                self.s3_investigator_parquet_output_path = input_config["s3_investigator_parquet_output_path"]

            spark_application_name = "creating structured file for " + self.data_source
            self.spark = (
                SparkSession.builder.appName(spark_application_name)
                    .enableHiveSupport()
                    .getOrCreate()
            )
            self.sqlContext = SQLContext(self.spark)

            self.spark_query_path = input_config["spark_query_path"]
            if input_config.get("stitching_columns"):
                logging.info("Starting initialising stitiching variables")
                self.stitching_columns = input_config["stitching_columns"]
                self.stitching_flag = True
                self.deleted_id_path = input_config["deleted_id_path"]
                self.step_name = input_config["step_name"]
                output = urlparse(self.deleted_id_path)
                bucket_name = output.netloc
                key_path = output.path.lstrip("/")
                logging.info(str(bucket_name) + str(key_path))
                config_object = self.s3_client.get_object(Bucket=bucket_name, Key=key_path)
                config_data = json.loads(config_object["Body"].read().decode('utf-8'))
                logging.debug(config_data)
                if config_data.get("id"):
                    self.deleted_id_list = config_data["id"]
                else:
                    self.deleted_id_list = []
                logging.info("completed initialising stitiching variables")

            self.schema_path = input_config["s3_schema_path"]
            status_message = "completing initialize_variables"
            logging.info(str(status_message))

            return {STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def create_investigator_file_for_who(self, table_name, target_path):
        try:
            status_message = "Starting method to create investigator file for WHO"
            logging.info(status_message)
            investigator_query = """select ROW_NUMBER() OVER (ORDER BY (Select 1)) AS id,TrialID, pn.Contact_Firstname, pn.Contact_Lastname, pn.Contact_Tel,
                pn.Contact_Email, Contact_Address, Contact_Affiliation from (select concat_ws('\|',collect_set(trialid)) as trialid,
                pn.Contact_Firstname, pn.Contact_Lastname, pn.Contact_Tel,
                pn.Contact_Email, Contact_Address, Contact_Affiliation from {table} lateral view
                posexplode(split(Contact_Email,';'))pn as p1, Contact_Email lateral view
                posexplode(split(Contact_Lastname,';'))pn as p2,Contact_Lastname lateral view
                posexplode(split(Contact_Tel, ';'))pn as p3,Contact_Tel lateral view
                posexplode(split(Contact_Firstname,';'))pn as p4,Contact_Firstname
                where p1==p2 and p2==p3 and p3==p4 group by 2,3,4,5,6,7) pn""".format(table=table_name)
            logging.debug("Executing query - {}".format(investigator_query))
            investigator_df = \
                self.sqlContext.sql(investigator_query)
            logging.debug("Query executed successfully")

            current_time = datetime.strftime(
                datetime.now(timezone("US/Pacific")), "%Y%m%d%H%M%S"
            )

            temp_parquet_output_path = path.join(
                self.temp_s3_path, self.data_source + "_"
                                   + self.disease_name.replace(" ", "_") + "_" + current_time
            )
            status_message = (
                    "Writing dataframe output to parquet format " + temp_parquet_output_path
            )
            logging.debug(str(status_message))

            columns_list = investigator_df.columns
            for column_name in columns_list:
                logging.debug(column_name)
                investigator_df = investigator_df.withColumn(
                    column_name, col(column_name).cast("string")
                )
            investigator_df.coalesce(1).write.format("parquet").mode("append").save(
                temp_parquet_output_path
            )

            parquet_files = CommonUtils().list_files_in_s3_directory(temp_parquet_output_path)

            logging.info("list ----->>>>> " + str(parquet_files))
            for file in parquet_files:
                if file.endswith(".parquet"):
                    logging.info("File copying is " + str(file))
                    command = "aws  s3 cp " + file + " " + target_path + " --sse"
                    logging.info("command for awscli ---->>> " + command)
                    CommonUtils().execute_shell_command(command)

            return {
                PARQUET_FILE_PATH_KEY: temp_parquet_output_path,
                STATUS_KEY: SUCCESS_KEY,
            }
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}


    def copy_file_s3(self, src_location, target_location, copy_with_suffix=None):
        """
        Purpose     :   This function copy files from source location to s3 path
        Input       :   source location, target s3 location
        Output      :   Return status SUCCESS/FAILED
        """
        try:
            output = urlparse(src_location)
            src_bucket_name = output.netloc
            src_key_path = output.path.lstrip("/")

            output = urlparse(target_location)
            target_bucket_name = output.netloc
            target_key_path = output.path.lstrip("/")
            next_token = None
            while True:
                if next_token:
                    output = self.s3_client.list_objects_v2(
                        Bucket=src_bucket_name,
                        Prefix=src_key_path,
                        ContinuationToken=next_token,
                    )
                else:
                    output = self.s3_client.list_objects_v2(
                        Bucket=src_bucket_name, Prefix=src_key_path
                    )

                if "Contents" in output:
                    for content in output["Contents"]:
                        object_path = content["Key"]
                        status_message = (
                                "Copying object= " + object_path + " from bucket " + src_bucket_name
                        )
                        logging.debug(status_message)
                        copy_source = {"Bucket": src_bucket_name, "Key": object_path}
                        target_file_path = os.path.join(
                            target_key_path, ntpath.basename(object_path)
                        )
                        if copy_with_suffix:
                            if not target_file_path.endswith(copy_with_suffix):
                                status_message = "not copying file", target_file_path
                                logging.debug(status_message)
                                continue
                        self.s3_client.copy_object(
                            Bucket=target_bucket_name,
                            CopySource=copy_source,
                            Key=target_file_path,
                            ServerSideEncryption="AES256",
                        )

                if "NextContinuationToken" in output:
                    next_token = output["NextContinuationToken"]
                else:
                    status_message = (
                        "Next token is not found hence breaking out of while loop"
                    )
                    logging.info(status_message)
                    break

            return {STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = str(traceback.format_exc())
            logging.error(str(error_message))
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def create_table_query(self, df_schema, table_name, s3_location):
        """
        Purpose     :   To create table
        Input       :   df_schema = schema of table, table name, s3 location
        Output      :   Return status SUCCESS/FAILED
        """
        status_message = ""
        try:
            status_message = "starting create_table_query function"
            logging.info(status_message)

            column_data_type_list = []
            for col_datatype in df_schema:
                col_name = col_datatype[0]
                data_type = col_datatype[1]
                column_data_type_list.append(col_name + "\t" + data_type)

            str_col_datatype = ",\n".join(column_data_type_list)

            query = (
                "create external table if not exists {table_name} ({column_datatype})"
                " stored as parquet location '{parquet_location}'".format(
                    table_name=table_name,
                    column_datatype=str_col_datatype,
                    parquet_location=s3_location,
                )
            )

            status_message = "Create table query " + query
            logging.debug(str(status_message))

            status_message = "completing create_table_query function"
            logging.info(status_message)

            return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: query}
        except:
            error_message = str(traceback.format_exc())
            logging.error(str(error_message))
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def delete_s3_object(self, s3_path):
        """
        Purpose     :   To delete s3 object
        Input       :   s3 path
        Output      :   Return status SUCCESS/FAILED
        """
        status_message = ""
        try:
            status_message = "starting delete_s3_object object"
            logging.info(status_message)
            output = urlparse(s3_path)
            bucket_name = output.netloc
            key_path = output.path.lstrip("/")
            output = self.s3_client.list_objects(Bucket=bucket_name, Prefix=key_path)
            if "Contents" in output:
                for content in output["Contents"]:
                    object_path = content["Key"]
                    status_message = (
                            "deleting object= " + object_path + " from bucket " + bucket_name
                    )
                    logging.debug(status_message)
                    self.s3_client.delete_object(Bucket=bucket_name, Key=object_path)

            status_message = "completing delete_s3_object object"
            logging.info(status_message)

            return {STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def execute_spark_query(self):
        """
        Purpose     :   this function reads schema and s3 path and execute spark query.
        Output      :   Return status SUCCESS/FAILED
        """
        status_message = ""
        try:
            status_message = "starting execute_spark_query for datasource - {}".format(self.data_source)
            logging.info(status_message)

            self.sqlContext.udf.register("parse_disease_name", parse_disease_name)
            self.sqlContext.udf.register("parse_trail", parse_trail)
            self.sqlContext.udf.register("parse_struct_of_list", parse_struct_of_list)
            self.sqlContext.udf.register("parse_struct_of_struct", parse_struct_of_struct)
            self.sqlContext.udf.register("parse_nest_list", parse_list)
            self.sqlContext.udf.register("parse_dm_udf", parse_dm)
            self.sqlContext.udf.register("get_pmc_id", get_pmc_id)
            self.sqlContext.udf.register("parse_nested_list", parse_nested_list)
            self.sqlContext.udf.register("parse_ps_udf", parse_ps)
            self.sqlContext.udf.register("parse_nctid", get_nct_id)
            self.sqlContext.udf.register("parse_protocolid", get_protocol_id)
            self.sqlContext.udf.register("get_eudract_number",get_eudract_number)
            self.sqlContext.udf.register("parse_disease", check_disease)
            self.sqlContext.udf.register("parse_arrs_udf", parse_arrs)
            self.sqlContext.udf.register("parse_key_udf", parse_key)
            self.sqlContext.udf.register("parse_arrs_drug_udf", parse_arrs_drug)
            self.sqlContext.udf.register("parse_arrs_udf", parse_arrs)
            self.sqlContext.udf.register("parse_keys", parse_keys)
            self.sqlContext.udf.register("concat_list", concat_list)
            self.sqlContext.udf.register("double_nested_list", parsed_double_nested_list)
            self.sqlContext.udf.register("parse_pe", parse_pe)

            if self.data_source == "clinicaltrials_gov":
                logging.info("inside clinicaltrials_gov")

                json_schema = (
                    self.spark.read.format("com.databricks.spark.xml")
                        .options(rowTag="clinical_study", valueTag="attr_value")
                        .load(self.schema_path)
                )
                raw_data_frame = (
                    self.spark.read.format("com.databricks.spark.xml")
                        .options(rowTag="clinical_study", valueTag="attr_value")
                        .load(self.s3_input_data_path, schema=json_schema.schema)
                )
            elif self.data_source[0:4] == "ixrs":
                logging.info("inside ixrs")
                # json_schema = (
                #     self.spark.read.format("com.databricks.spark.xml")
                #         .options(rowTag='AZStudies')
                #         .load(self.schema_path)
                # )
                temp_json = self.spark.read.text(self.schema_path).first()[0]
                schema1 = StructType.fromJson(json.loads(temp_json))
                raw_data_frame = (
                    self.spark.read.format("com.databricks.spark.xml")
                        .options(rowTag='AZStudies')
                        .load(self.s3_input_data_path, schema=schema1)
                )
            elif self.data_source == "pubmed":

                logging.info("inside pubmed")

                json_schema = (
                    self.spark.read.format("com.databricks.spark.xml")
                        .options(rowTag="PubmedArticle")
                        .load(self.schema_path)
                )
                raw_data_frame = (
                    self.spark.read.format("com.databricks.spark.xml")
                        .options(rowTag="PubmedArticle")
                        .load(self.s3_input_data_path, schema=json_schema.schema)
                )

            elif self.data_source == "who":
                logging.info("Reading data from raw XML files")
                # for multiple phase load in a single load, multiple input files will be created
                raw_data_frame = None
                if type(self.s3_input_data_path) == list:
                    for s3_path in self.s3_input_data_path:
                        # temp_df = self.spark.read.csv(s3_path, sep=',', escape='"', header=True,
                        #                                inferSchema=True, multiLine=True)
                        temp_df = self.spark.read.format('xml').option('rowTag', 'Trial').load(s3_path)
                        # merge dataframes for multiple files
                        if raw_data_frame:
                            raw_data_frame = raw_data_frame.union(temp_df)
                        else:
                            raw_data_frame = temp_df
                else:
                    raw_data_frame = self.spark.read.format('xml').option('rowTag', 'Trial').load(
                        self.s3_input_data_path)

            else:
                status_message = "reading json schema= " + self.schema_path
                logging.debug(str(status_message))

                json_schema = self.spark.read.json(self.schema_path, multiLine=True)
                logging.debug("json schema ")

                status_message = (
                        "reading input data and creating dataframe= " + self.s3_input_data_path
                )
                logging.debug(str(status_message))
                raw_data_frame = self.spark.read.json(
                    self.s3_input_data_path, json_schema.schema, multiLine=True
                )

            temp_table_name = "temp_" + self.data_source
            raw_data_frame.createOrReplaceTempView(temp_table_name)
            current_time = datetime.strftime(
                datetime.now(timezone("US/Pacific")), "%Y-%m-%d %H:%M:%S"
            )

            status_message = "reading spark query= " + self.spark_query_path
            logging.debug(str(status_message))
            output = urlparse(self.spark_query_path)
            bucket_name = output.netloc
            logging.debug(str(bucket_name))
            key_path = output.path.lstrip("/")
            config_object = self.s3_client.get_object(Bucket=bucket_name, Key=key_path)
            spark_query = config_object["Body"].read()
            logging.info(str(spark_query))
            spark_query = (
                spark_query.decode("utf-8")
                    .replace("$$table$$", temp_table_name)
                    .replace("$$disease_area$$", self.disease_name)
                    .format(current_ts=current_time)
            )
            logging.debug("changed spark query" + str(spark_query))
            formatted_df = self.sqlContext.sql(spark_query)
            formatted_df.count()

            # Stitching on adapter files
            if self.stitching_flag:
                unique_column_names = self.stitching_columns
                deleted_id_list = self.deleted_id_list
                history_publish_data_location = CommonUtils().fetch_successful_parquet_path_for_adapter(self.data_source,
                                                                                                        self.step_name)

                formatted_df.createOrReplaceTempView(temp_table_name)

                if history_publish_data_location is not None:
                    formatted_df = CommonUtils().perform_data_stitching_type_1(unique_column_names=unique_column_names,
                                                                               source_location=history_publish_data_location,
                                                                               current_df=formatted_df)
                    formatted_df.createOrReplaceTempView(temp_table_name)
                    formatted_df.count()
                    logging.debug("Stitching of " + self.data_source + " adapter ran successfully")

                #delete id from formatted final df
                if len(deleted_id_list) > 0:
                    column = unique_column_names[0]
                    logging.debug(str(deleted_id_list) + " -- "+ str(len(deleted_id_list)))
                    formatted_df = formatted_df.filter(~col(column).isin(deleted_id_list))
                    formatted_df.count()
                    formatted_df.createOrReplaceTempView(temp_table_name)
                    logging.debug("Deletion of id of " + self.data_source + " adapter ran successfully")
            # Stitching on adapter files ends here

            # creating separate table for investigator
            if self.create_investigator_table == "Y":
                investigator_output_status = self.create_investigator_file_for_who(
                    table_name=temp_table_name, target_path=self.s3_investigator_parquet_output_path)
                if investigator_output_status[STATUS_KEY] != SUCCESS_KEY:
                    raise Exception("Failed to create investigator table, ERROR - {}".format(
                        investigator_output_status[ERROR_KEY]))

            logging.debug(str(status_message))

            current_time = datetime.strftime(
                datetime.now(timezone("US/Pacific")), "%Y%m%d%H%M%S"
            )

            temp_parquet_output_path = path.join(
                self.temp_s3_path, self.data_source + "_"
                                   + self.disease_name.replace(" ", "_") + "_" + current_time
            )
            status_message = (
                    "Writing dataframe output to parquet format " + temp_parquet_output_path
            )

            columns_list = formatted_df.columns
            for column_name in columns_list:
                logging.debug(column_name)
                formatted_df = formatted_df.withColumn(
                    column_name, col(column_name).cast("string")
                )
            formatted_df.coalesce(1).write.format("parquet").mode("append").save(
                temp_parquet_output_path
            )
            logging.info("temp path for trials  ----->>>>> " + str(temp_parquet_output_path))
            status_message = "Completing execute_spark_query"
            logging.info(status_message)

            return {
                PARQUET_FILE_PATH_KEY: temp_parquet_output_path,
                STATUS_KEY: SUCCESS_KEY,
            }
        except:
            error_message = status_message + ": " + str(traceback.format_exc())
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def unionAll(self, *dfs):
        return reduce(DataFrame.unionAll, dfs)

    def merge_s3_parquets(self, list_of_input_s3_paths, output_parquet_s3_path):
        """
        Purpose : To merge multiple parquets on S3 and place combined parquet back on S3
        Input   : list_of_input_s3_paths are the S3 paths of input parquets,
                  output_parquet_s3_path is output parquet S3 path
        Output  : 1 if successfully merged else throw Exception
        """
        try:
            dfs_arr = [self.spark.read.parquet(path) for path in list_of_input_s3_paths]
            combined = self.unionAll(*dfs_arr)
            combined.coalesce(1).write.mode('overwrite').parquet(output_parquet_s3_path)
            return {STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = str(traceback.format_exc())
            logging.error(error_message)
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}

    def usage(self):
        """
        :def: USAGE() Function
        :return: prints help statement
        """
        MSG = """Usage: python DataIngestionAdapter.py -j
        This will trigger the ingestion adapters based on the filters provided
        by the .  contains all the filters
        needed to trigger the respective data source ingestion. eg.(trial_trove
        pubmed and Clinical_trials)
                    """
        logging.info(str(MSG))

    def execute_spark_query_for_eudract(self, drop_duplicates_flag):
        try:
            dataframe = self.spark.read.csv(self.csv_temp_path, sep=',', escape='"', header=True, inferSchema=True)
            if drop_duplicates_flag == "Y":
                dataframe = dataframe.dropDuplicates()
            dataframe = dataframe.withColumn(DISEASE_AREA_COL_VALUE_KEY, lit(self.disease_name))
            dataframe.coalesce(1).write.format("parquet").mode("overwrite").save(self.parquet_temp_path)
            return {STATUS_KEY:SUCCESS_KEY, PARQUET_FILE_PATH_KEY:self.parquet_temp_path}
        except:
            error_msg = str(traceback.format_exc())
            return {STATUS_KEY:FAILED_KEY, ERROR_KEY:error_msg}

    def main(self, input_json_file_path):
        """
        Purpose     :   This function reads the configuration from given config file path
        Input       :   config file path
        Output      :   Return status SUCCESS/FAILED
        """
        try:
            status_message = (
                    "starting main function with input parameter " + input_json_file_path
            )
            logging.debug(str(status_message))
            output = urlparse(input_json_file_path)
            bucket_name = output.netloc
            key_path = output.path.lstrip("/")
            logging.info(str(bucket_name) + str(key_path))
            config_object = self.s3_client.get_object(Bucket=bucket_name, Key=key_path)
            config_data = json.loads(config_object["Body"].read().decode('utf-8'))
            logging.debug(config_data)
            config_data_list = config_data["spark_config"]

            parquet_file_path_list = []

            for json_data in config_data_list:
                output = self.initialize_variables(json_data)

                if output[STATUS_KEY] == FAILED_KEY:
                    return output

                output = ""
                if self.data_source == "eudract":
                    drop_duplicates_flag = config_data["drop_duplicates"]
                    output = self.execute_spark_query_for_eudract(drop_duplicates_flag)
                else:
                    output = self.execute_spark_query()

                if output[STATUS_KEY] == FAILED_KEY:
                    return output

                parquet_file_path_list.append(output[PARQUET_FILE_PATH_KEY])


            if self.data_source == "eudract":
                parquet_output_path = self.parquet_final_path
            else:
                current_time = datetime.strftime(
                    datetime.now(timezone("US/Pacific")), "%Y%m%d%H%M%S"
                )
                parquet_output_path = self.s3_parquet_output_path
                temp_parquet_output_path = path.join(
                    self.temp_s3_path, self.data_source + "_"
                                       + self.disease_name.replace(" ", "_") + "_" + current_time
                )

                if len(config_data_list) > 1:
                    output = self.merge_s3_parquets(
                        parquet_file_path_list,
                        temp_parquet_output_path,
                    )

                    if output[STATUS_KEY] == FAILED_KEY:
                        return output
                    parquet_file_path_list[0] = temp_parquet_output_path

            logging.debug(parquet_file_path_list[0])
            if not str(parquet_file_path_list[0]).endswith("/"):
                parquet_file_path_list[0] = parquet_file_path_list[0] + "/"

            parquet_files = CommonUtils().list_files_in_s3_directory(
                parquet_file_path_list[0]
            )
            logging.info("list ----->>>>> " + str(parquet_files))
            for file in parquet_files:
                if file.endswith(".parquet"):
                    logging.info("File copying is " + str(file))
                    command = "aws  s3 cp " + file + " " + parquet_output_path + " --sse"
                    logging.info("command for awscli ---->>> " + command)
                    CommonUtils().execute_shell_command(command)

            self.spark.stop()
            return {STATUS_KEY: SUCCESS_KEY}
        except:
            error_message = str(traceback.format_exc())
            logging.error(str(error_message))
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}


if __name__ == "__main__":
    try:
        access_key = secret_key = input_json_file_path = None
        try:
            opts, args = getopt.getopt(sys.argv[1:], "hj:")
            for opt, arg in opts:
                print(opt + "\t" + arg)
                if opt == "-h":
                    CreateStructuredFile().usage()
                if opt == "-j":
                    input_json_file_path = arg
                    logging.debug("input_json_file_path : " + str(input_json_file_path))
            STRUCTURED_FILE_OBJ = CreateStructuredFile(access_key, secret_key)
            OUTPUT = STRUCTURED_FILE_OBJ.main(input_json_file_path)
            logging.info(json.dumps(OUTPUT))
            if OUTPUT[STATUS_KEY] == FAILED_KEY:
                raise Exception()
        except getopt.GetoptError:
            MSG = "Invalid arguments"
            CreateStructuredFile().usage()
            raise Exception(MSG)
        except Exception:
            raise Exception(str(traceback.format_exc()))
    except Exception:
        raise str(traceback.format_exc())
