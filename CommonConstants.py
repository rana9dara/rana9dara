MODULE_NAME_KEY = 'module_name'
PROCESS_NAME_KEY = 'process_name'
PROCESS_MESSAGE_KEY = 'message'
STATUS_KEY = 'status'
STATUS_FAILED = 'FAILED'
STATUS_SUCCEEDED = 'SUCCEEDED'
STATUS_SUCCESS = 'SUCCESS'
RESULT_KEY = "result"
STATUS_RUNNING = 'IN PROGRESS'
ERROR_KEY = 'error'

ROW_ID_EXCLUSION_IND_NO = "N"
ROW_ID_EXCLUSION_IND_YES = "Y"

STATUS_SKIPPED = "SKIPPED"

ENVIRONMENT_CONFIG_FILE = "environment_params.json"

ENVIRONMENT_PARAMS_KEY = "EnvironmentParams"

VARIABLE_PREFIX = "$$"
SKIPPED_DESC = "BATCH SKIPPED"

# Instance Fleet S3 Path Default Value None
INSTANCE_FLEET_S3_PATH = None

MAX_RETRY_LIMIT = 5
LOG_DATA_ACQUISITION_SMRY_TABLE = "log_adapter_smry"
LOG_DATA_ACQUISITION_DTL_TABLE = "log_adapter_dtl"
ADAPTER_DETAILS_TABLE = "ctl_adapter_details"
PAYLOAD_DETAILS_TABLE = "ctl_adapter_payload_details"
DATASOURCE_INFORMATION_TABLE_NAME = "ctl_dataset_master"
PROCESS_LOG_TABLE_NAME = 'log_file_dtl'
FILE_AUDIT_TABLE = 'log_file_smry'
COLUMN_METADATA_TABLE = "ctl_column_metadata"

IN_PROGRESS_DESC = "IN PROGRESS"
PUBLISH_TYPE = "TABLE"
HIVE_QUERY_TEMPLATE_FILE = "hive_query_template.json"
PARTITION_MODE = "FULL"
PROCESS_NAME = 'process_name'
PROCESS_STATUS = 'process_status'
# FILE_STATUS = 'file_status'
PARQUET = 'parquet'
DATASET_ID = 'dataset_id'
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
CSV_FORMAT = "com.databricks.spark.csv"
WITHDRAW_PROCESS_NAME = "withdraw"

MYSQL_DATASET_ID = "dataset_id"
MYSQL_ACTIVE_IND = "active_flag"
ACTIVE_IND_VALUE = "Y"

EMR_CLUSTER_CONFIGURATION_TABLE = "ctl_cluster_config"
EMR_CLUSTER_DETAILS_TABLE = "log_cluster_dtl"
EMR_CLUSTER_HOST_DETAILS = "log_cluster_host_dtl"
CLUSTER_EMR_TERMINATE_STATUS = "TERMINATED"
CLUSTER_ID = "cluster_id"
WORKFLOW_ID = "workflow_id"
CLUSTER_ACTIVE_STATE = "WAITING"
WORKFLOW_DATASET_ACTIVE_VALUE = 'Y'
EMR_CLUSTER_ID_COLUMN = "cluster_id"
EMR_MASTER_DNS = "master_node_dns"
EMR_CLUSTER_PROCESS_COLUMN = "process_name"
EMR_CLUSTER_STATUS_COLUMN = "cluster_status"
EMR_PROCESS_WORKFLOW_MAP_TABLE = "ctl_workflow_master"
EMR_PROCESS_WORKFLOW_COLUMN = "process_id"
EMR_PROCESS_WORKFLOW_ACTIVE_COLUMN = "active_flag"
EMR_USER_NAME = 'hadoop'

# status
EMR_WAITING = 'WAITING'
EMR_STARTING = 'STARTING'
EMR_BOOTSTRAPPING = 'BOOTSTRAPPING'
EMR_TERMINATED = 'TERMINATED'
EMR_RUNNING = 'RUNNING'

DAG_TEMPLATE_FILE_NAME = "dag.template"
# AIRFLOW_CODE_PATH = "/app/clinical_design_center/data_management/sanofi_ctfo/code/"
AIRFLOW_CODE_PATH = "/usr/local/airflow/plugins/code"
# DAGS_FOLDER = "/usr/lib/airflow/dags"
DAGS_FOLDER = "/usr/local/airflow/dags"
DAG_PYTHON_FILE_PREFIX = "StagingProcess"
# EMR_CODE_PATH = "/app/clinical_design_center/data_management/sanofi_ctfo"
EMR_CODE_PATH = "/usr/local/airflow/plugins"

WF_NOT_FOUND = "Workflow name not found in process workflow map table"
PROCESS_NOT_FOUND = "Process name not found in cluster config table"

# EMR specific code constants starts here

AUDIT_DB_NAME = "audit_information"
RUN_ID_GENERATOR_SEQUENCE_NAME = "run_id_seq"
BATCH_ID_GENERATOR_SEQUENCE_NAME = "batch_id_seq"
PROCESS_ID_GENERATOR_SEQUENCE_NAME = "process_sequence"
FILE_ID_GENERATOR_SEQUENCE_NAME = "file_sequence"
CYCLE_ID_GENERATOR_SEQUENCE_NAME = "cycle_sequence"
TABLE_METADATA_TABLE = "ctl_table_metadata"
BATCH_TABLE = "log_batch_dtl"
STATUS_TABLE = "status_master"

DQM_DB_NAME = ""
DQM_METADATA_TABLE = "ctl_dqm_master"
DQM_ERROR_LOCATION = "/user/hadoop/test/dqm/dqm_error_details"
DQM_SUMMARY_LOCATION = "/user/hadoop/test/dqm/dqm_summary"

PAYLOAD_S3_PATH = "s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical_data_lake/tmp"
PRE_LANDING_LOCATION_TYPE = "S3"
IN_PROGRESS_ID = "200000"
SUCCESS_ID = "200001"
FAILED_ID = "200002"

BATCH_ID = 'batch_id'
FILE_ID = 'file_id'
FILE_NAME = 'file_name'
FILE_STATUS = 'file_status'

PARTITION_FILE_ID = "pt_file_id"
PARTITION_BATCH_ID = "pt_batch_id"

HADOOP_CONF_DIR = "HADOOP_CONF_DIR"
HADOOP_CONF_PATH = "/etc/hadoop/conf"
HADOOP_HOME_PATH = "/opt/cloudera/parcels/CDH/lib/hadoop"
HADOOP_HOME_DIR = "HADOOP_HOME"

S3_CREDENTIAL_FILE_NAME = "$$current_user$$_s3_cred_file.json"
S3_CREDENTIAL_HDFS_LOCATION = ""

S3_ACCESS_KEY = 'aws_access_key'
S3_SECRET_KEY = 'aws_secret_key'
S3_REGION_ENDPOINT = 's3_region_endpoint'
S3_AWS_REGION = 'aws_region'
S3A_PREFIX = 's3://'
S3A_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem"
S3_PATH = "/clinical-data-lake/code/"
S3_PREFIX = "s3://"

HADOOP_CONF_PROPERTY_DICT = {"spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3.S3FileSystem",
                             "mapreduce.fileoutputcommitter.algorithm.version": "2",
                             "spark.speculation": "true"}

# unused as Non-EMR mode is not supported
HADOOP_FS_ACCESS_KEY_PROPERTY = "fs.s3a.access.key"
HADOOP_FS_SECRET_KEY_PROPERTY = "fs.s3a.secret.key"
HADOOP_FS_REGION_ENDPOINT_PROPERTY = "fs.s3a.endpoint"
HADOOP_FS_S3A_IMPL_PROPERTY = "spark.hadoop.fs.s3a.impl"

SPARK_JOB_SUCCESS_DIR_NAME = '_SUCCESS'
# DQM Configurations
DQM_NULL_CHECK = "Null"
DQM_INTEGER_CHECK = "Integer"
DQM_DECIMAL_CHECK = "Decimal"
DQM_DATE_CHECK = "Date"
DQM_CUSTOM_CHECK = "Custom"
DQM_UNIQUE_CHECK = "Unique"
DQM_DOMAIN_CHECK = "Domain"
DQM_LENGTH_CHECK = "Length"
DQM_REFERENTIAL_INTEGRITY_CHECK = "Referential Integrity"

DATASET_LANDING_LOCATION = "hdfs_landing_location"
MYSQL_FILE_ID = "file_id"
MYSQL_FILE_NAME = "file_name"

DQM_PASS_STATUS = "1"
DQM_FAILED_STATUS = "0"
MULTI_PART_FILE_IND_NO = "N"
MULTI_PART_FILE_IND_YES = "Y"

MESSAGE_MAX_CHAR = "150"
MAX_THREAD_LIMIT = "8"

# Date Params & Regex


DATE_TIME_FORAMT_1 = "%Y-%m-%dT%H:%M:%S+0000"
DATE_TIME_FORAMT_1_REGEX = "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+[0-9]{4})"

DATE_TIME_FORAMT_2 = "%Y"
DATE_TIME_FORAMT_2_REGEX = "([0-9]{4})"

DATE_TIME_FORAMT_3 = "%Y-%m-%dT%H:%M:%S.%f+0000"
DATE_TIME_FORAMT_3_REGEX = "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}\+[0-9]{4})"

DATE_TIME_FORAMT_4 = "MM/DD/YYYY"
DATE_TIME_FORAMT_4_REGEX = "([0-9]{2}/[0-9]{2}/[0-9]{4})"

DATE_TIME_FORAMT_5 = "YYYY-MM-DD HH24:MI:SS"
DATE_TIME_FORAMT_5_REGEX = "([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})"

DATE_TIME_FORAMT_6 = "YYYY-MM-DD"
DATE_TIME_FORAMT_6_REGEX = "([0-9]{4}-[0-9]{2}-[0-9]{2})"
DATE_TIME_FORAMT_7 = "%Y-%m-%dT%H:%M:%S.%fZ"
DATE_TIME_FORAMT_7_REGEX = "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.*[0-9]*Z*)"

DATE_TIME_FORAMT_DEFAULT = "MM/DD/YYYY"
DATE_TIME_FORAMT_DEFAULT_REGEX = "([0-9]{2}/[0-9]{2}/[0-9]{4})"

# Integer and Decimal Regex

INTEGER_REGEX = "(^-?\d+$)"
DECIMAL_REGEX = "(^-?\d*[.]?\d+$)"

# EMR Alert Utility
TERMINATE_IDLE_EMR_FLAG = 'Y'
TERMINATE_ORPHAN_EMR_FLAG = 'Y'
CLOUDWATCH_EMR_NAME_SPACE = "AWS/ElasticMapReduce"
EMR_STATE_ISIDLE = "IsIdle"
CLOUDWATCH_MATRIX_PERIOD = 60
CLOUDWATCH_MATRIX_STATISTICS = "Average"
CLOUDWATCH_MATRIX_UNIT = None
IDLE_CLUSTER_TIME_LIMIT = 3
LONG_RUNNING_CLUSTER_TIME_LIMIT = 3

# Column Names
QC_ID = "qc_id"
ERROR_COUNT = "error_count"
QC_PARAM = "qc_param"
QC_MESSAGE = "qc_message"
CRITICALITY = "criticality"
QC_FAILURE_PERCENTAGE = "qc_failure_percentage"
CREATE_BY = "insert_by"
CREATE_TS = "insert_date"
QC_TYPE = "qc_type"
DATSET_ID = "dataset_id"
COLUMN_NAME = "column_name"
ROW_ID = "row_id"
ERROR_VALUE = "error_value"
NOT_AVAILABLE = "NA"

COLUMN_SEQUENCE_NUMBER = "column_sequence_number"
HEADER_FLAG_PRESENT = "Y"
HEADER_FLAG_ABSENT = "N"

LOAD_SUMMARY_TABLE = "load_summary_dtl"
MODIFIED_OOZIE_PROPERTY_FILE_NAME = "_job.properties"

GET_EMR_HOST_IP = "get_emr_host_ip"
GET_EMR_HOST_DETAILS = "emr_cluster_host_details"

STANDARDIZATION_MASTER_TABLE = "ctl_data_standardization_master"
TYPE_CONVERSION_FUNC_NAME = "type_conversion"
BLANK_CONVERSION_FUNC_NAME = "blank_conversion"
LEFT_PADDING_FUNC_NAME = "left_padding"
RIGHT_PADDING_FUNC_NAME = "right_padding"
TRIM_FUNCTION_FUNC_NAME = "trim"
SUB_STRING_FUNC_NAME = "sub_string"
REPLACE_FUNC_NAME = "replace"
ZIP_EXTRACTOR_FUNC_NAME = "zip_extractor"
DATE_CONVERSION_FUNC_NAME = "date_conversion"

FUNTION_NAME = "function_name"
FUNCTION_PARAMS = "function_params"

EXCEPTION_ON_REJECT_LIST = "True"

PROVIDE_ALL_ARGUMENTS = "Please provide all required arguments for the module"
PROVIDE_BATCH_FILE_PATH = "Please provide batch file path to store batch id"

SOURCE_LOCATION_NOT_PROVIDED = "Source File Location is not provided"
PRE_LANDING_LOCATION_NOT_PROVIDED = "Pre Landing location is not provided"

FILE_CHECK_VALID_ARGUMENTS = "Please provide valid arguments for File Check module"

LOG_PRE_DQM_DTL = "log_pre_dqm_dtl"
LOG_DQM_SMRY = "log_dqm_smry"
STATUS_NOT_STARTED = "EXECUTION NOT STARTED"
TERMINATION_ON_FAILURE_FLAG = "N"
TERMINATION_ON_SUCCESS_FLAG = "N"

CHECK_LAUNCH_COLUMN = "check_launch_flag"
CHECK_LAUNCH_ENABLE = "Y"
CHECK_LAUNCH_DISABLE = "N"

MULTIPLE_BATCH_FLAG = "N"

# Ingestion Level Application log messages
DATASET_ID_NOT_PROVIDED = "Dataset id is not provided"
BATCH_ID_NOT_PROVIDED = "Batch ID for current batch is not provided"
BATCH_ID_LIST_NOT_PROVIDED = "Batch ID List is not provided"
CLUSTER_ID_NOT_PROVIDED = "Cluster ID is not provided"
WORKFLOW_ID_NOT_PROVIDED = "Workflow ID is not provided"
PROCESS_ID_NOT_PROVIDED = "Process ID is not provided"
DATASET_NOT_PRESENT_FOR_ID = "No dataset is configured for the provided dataset id"
POST_DQM_LOC_NOT_PROVIDED = "Post DQM location is not provided in ctl_dataset_master table for the input dataset id "
PUBLISH_LOC_NOT_PROVIDED = "Publish location is not provided in ctl_dataset_master table for the input dataset id "
MISSING_FILE_DETAILS_FOR_BATCH_ID = "No details found in file audit table for current batch"
INVALID_FILE_AUDIT_DETAILS = "File audit details for batch id has invalid file name or file id"
STG_LOC_NOT_PROVIDED = "Staging location is not provided in ctl_dataset_master table for the input dataset id"

# s3-dist-cp-path
S3_DIST_CP_BIN_PATH = "/usr/share/aws/emr/s3-dist-cp/bin/"

# S3 To Cluster Utility
JSON_PATH_KEY = "json_path"
ACCESS_KEY = "aws_access_key_id"
SECRET_KEY = "aws_secret_access_key"
HADOOP_OPTIONS_KEY = "hadoop_options"
S3_DISTCP_OPTIONS_KEY = "s3_distcp__options"
SOURCE_LOCATION_KEY = "source_location"
TARGET_LOCATION_KEY = "target_location"
DATA_COPY_LOG_TABLE = "log_data_copy_dtl"

# cluster to s3 utility
EMR_JOB_FLOW_FILE_PATH = "/emr/instance-controller/lib/info/job-flow.json"
MASTER_PRIVATE_DNS_KEY = "masterPrivateDnsName"

# Restartability
FILE_PROCESS_NAME_PRE_LANDING = "Download Files from source location"
FILE_PROCESS_NAME_FILE_CHECK = "File Schema Check"
FILE_PROCESS_NAME_LANDING_COPY = "Copy To Landing"
FILE_PROCESS_NAME_PRE_DQM = "Pre DQM Standardization"
FILE_PROCESS_NAME_DQM = "DQM Check"
FILE_PROCESS_NAME_STAGING = "Staging"
FILE_PROCESS_NAME_HDFS_TO_S3 = "Copy Files from HDFS to S3"
FILE_PROCESS_NAME_PUBLISH = "Publish"
FILE_PROCESS_NAME_ARCHIVE = "Archive File From Source"

CLUSTER_MASTER_SUBMIT_FLAG = "Y"

# DW

CTL_RULE_CONFIG = 'ctl_rule_config'
LOG_RULE_DTL = 'log_rule_dtl'
LOG_VALIDATION_DTL = 'log_rule_validation_dtl'
PROCESS_DEPENDENCY_MASTER_TABLE = 'ctl_process_dependency_master'
PROCESS_DEPENDENCY_DETAILS_TABLE = 'ctl_process_dependency_details'
VALIDATION_TYPE = 'validation'
SPARK_BIN_PATH = '/usr/lib/spark/bin/'
RSCRIPT_BIN_PATH = '/usr/lib64/R/bin/'
JOB_EXECUTOR = 'job_executor'
CONFIG = 'configs'
CODE = 'code'
DW_RULE_TEMPLATE_FILENAME = 'rule_query.template'
DW_R_RULE_TEMPLATE_FILENAME = 'r_rule_query.template'
STEP_WORKFLOW_MASTER_TABLE = 'ctl_step_workflow_master'
DW_INACTIVE_FLAG = 'N'
DW_DAG_TEMPLATE_FILE = 'dw_dag.template'
CYCLE_TIMESTAMP_FORMAT = '%Y%m%d%H%M%S%f'
LOG_STEP_DTL = 'log_step_dtl'
DATA_COPY_S3_HDFS_STEP = "copy_data_s3_to_hdfs"
LAUNCH_DDL_CREATION_STEP = "ddl_creation"
SOURCE_TABLE_TYPE = 'source'
TARGET_TABLE_TYPE = 'target'
RAW_DATASET_TYPE = 'raw'
PROCESSED_DATASET_TYPE = 'processed'
LATEST_LOAD_TYPE = 'latest'
FULL_LOAD_TYPE = 'full'
NOT_AVAILABLE_IND_VALUE = '-99'
NOT_APPLICABLE_VALUE = '-1'
LOG_CYCLE_DTL = 'log_cycle_dtl'
STATUS_NEW = 'NEW'
TABLE_TEMPLATE_FILE = 'table_ddl_query.template'
PROCESS_DATE_TABLE = 'ctl_process_date_mapping'
# data date format
CYCLE_DATE_FORMAT = '%Y%m%d'
HIVE_WAREHOUSE_PATH = '/user/hive/warehouse'
PARQUET_FORMAT = 'PARQUET'
TEXT_FORMAT = 'TEXTFILE'
MANAGED_TABLE_TYPE = 'managed'
EXTERNAL_TABLE_TYPE = 'external'
TABLE_TEXT_FORMAT = 'TEXT'
SRC_PATTERN = '.*[^$]$'

# staging partition data type
BATCH_PARTITION_TYPE = 'bigint'
FILE_PARTITION_TYPE = 'integer'

# dw partition column
DATE_PARTITION = 'pt_data_dt'
CYCLE_PARTITION = 'pt_cycle_id'

# dw partition column data type
DATE_PARTITION_TYPE = 'string'
CYCLE_PARTITION_TYPE = 'bigint'

STRING_DATATYPE = 'string'

PUBLISH_STEP = 'publish'

# Dynamic Repartition
# keeping 120 to avoid ovehead by syatemm
BLOCK_SIZE = 120
BYTES_TO_MB = 1048576
GZ_COMPRESSION_RATIO = 5
ZIP_COMPRESSION_RATIO = 5
BZ2_COMPRESSION_RATIO = 9
BYTES_TO_GB = 1073741824

# Ganglia reports
SEND_GANGLIA_PDF_AS_ATTACHMENT = 'N'
SAVE_GANGLIA_PDF_TO_S3 = 'N'
SAVE_GANGLIA_CSV_TO_S3 = 'N'
S3_FOLDER = '/ganglia_reports'
S3_GANGLIA_FOLDER_PATH = 'demo/ganglia_reports'
GANGLIA_PDF_DIRECTORY = 'ganglia'
GANGLIA_METRIC_LIST = 'mem_report,disk_free,yarn.QueueMetrics.AppsSubmitted,yarn.QueueMetrics.AvailableMB'
PHANTOMJS_LOG_FILE_NAME = "phantomJs-driver.log"
PHANTOMJS_SERVICE_LOG_PATH = '/home/airflow/common_components/python3_CC/code/'
PHANTOMJS_EXECUTABLE_PATH = '/usr/local/share/phantomjs-1.9.8-linux-x86_64/bin/'
WITHDRAW_PROCESS_NAME = "withdraw"
FILE_PROCESS_NAME_DATA_COPY_LND = "DATA_COPY_LANDING"
FILE_PROCESS_NAME_DATA_COPY_PREDQM = "DATA_COPY_PREDQM"
FILE_PROCESS_NAME_DATA_COPY_DQM = "DATA_COPY_DQM"
FILE_PROCESS_NAME_DATA_COPY_POSTDQM = "DATA_COPY_POSTDQM"

FILE_PROCESS_STEPS = [FILE_PROCESS_NAME_PRE_LANDING, FILE_PROCESS_NAME_FILE_CHECK, FILE_PROCESS_NAME_LANDING_COPY,
                      FILE_PROCESS_NAME_PRE_DQM, FILE_PROCESS_NAME_DQM, FILE_PROCESS_NAME_STAGING,
                      FILE_PROCESS_NAME_PUBLISH, FILE_PROCESS_NAME_ARCHIVE, FILE_PROCESS_NAME_DATA_COPY_LND,
                      FILE_PROCESS_NAME_DATA_COPY_PREDQM, FILE_PROCESS_NAME_DATA_COPY_DQM,
                      FILE_PROCESS_NAME_DATA_COPY_POSTDQM]

S3_LANDING_LOCATION_KEY = "s3_landing_location"
HDFS_PRE_DQM_LOCATION_KEY = "hdfs_pre_dqm_location"
S3_PRE_DQM_LOCATION_KEY = "s3_pre_dqm_location"
HDFS_POST_DQM_LOCATION_KEY = "hdfs_post_dqm_location"
S3_POST_DQM_LOCATION_KEY = "s3_post_dqm_location"
S3_STAGING_LOCATION_KEY = "staging_location"

# copy-spark-logs-to-s3
SPARK_LOGS_DEFAULT_PATH_EMR = "/var/log/spark/apps/"

# logging-keys
CLUSTER_ID_LOG_KEY = "cluster_id"
WORKFLOW_ID_LOG_KEY = "workflow_id"
PROCESS_ID_LOG_KEY = "process_id"
MODULE_NAME_LOG_KEY = "module_name"
FUNCTION_NAME_LOG_KEY = "function_name"
TRACEBACK_LOG_KEY = "traceback"
ENVIRONMENT_LOG_KEY = "environment"
DATASET_ID_LOG_KEY = "dataset_id"
BATCH_ID_LOG_KEY = "batch_id"
FILE_ID_LOG_KEY = "file_id"
CYCLE_ID_LOG_KEY = "cycle_id"
FREQUENCY_LOG_KEY = "frequency"
DATA_DATE_LOG_KEY = "data_date"
STEP_NAME_LOG_KEY = "step_name"

# GLUE
GLUE = "glue"
RUNNING_STATE = "RUNNING"
COLUMN_PREFIX = 'pt_'
glue_avoid_table_path_list = []
# Repartition utility
HDFS_REPARTION_PATH = "/hive_repartitioned_data/"
# MySQL Execution Utility
ROWS_TO_UPDATE = 10
ROWS_TO_DELETE = 10
# Redshift
REDSHIFT_PARAMS = "redshift_params"
WLM_PARAMS = "wlm_params"
PARAM_TO_CHANGE = 'wlm_json_configuration'
PARAMETER_KEY = "Parameters"
PARAMETER_NAME_KEY = "ParameterName"
PARAMETER_VALUE_KEY = "ParameterValue"
QUERY_CONCURRENCY_KEY = "query_concurrency"
MEMEORY_PERCENT_TO_USE_KEY = "memory_percent_to_use"
# data stitching
SCDTYPE1 = "SCD_1"
# HouseKeepingutility
HOUSEKEEPING_PARAMS_KEY = "housekeeping_params"
ROLLOVER_DAYS = 15
######## new constants added by CC
# Variable File Schema
COLUMN_NAME_SPECIAL_CHARACTER_STRING = "[^A-Za-z0-9_ ]+"
EXCLUDED_COLUMN_LIST = ["PT_BATCH_ID", "PT_FILE_ID"]
# Threshold
THRESHOLD_FLAG = 'N'
# Checkpointing
CHECKPOINT_COUNT = 15
MULTILINE_READ_FLAG = "Y"
VARIABLE_FILE_FORMAT_TYPE_KEY = "V"
VARIABLE_SCHEMA_HANDLING_FLAG = "Y"
CHUNK_SIZE = 15
EXCEL_FORMAT = 'excel'
BOTO_HOST = 's3.amazonaws.com'
AWS_REGION = 'us-east-1'
######################## SUPERDAG
DAG_STATE_POLL_INTERVAL = 3
DAG_STATE_MAX_POLL_TIMEOUT = 18000
DAG_ENTRY_MAX_POLL_TIMEOUT = 20
DAG_ENTRY_POLL_INTERVAL = 3
RETRY_COUNT = 3
AIRFLOW_CONFIG = "airflow_config"
RUNNING_STATE = "running"
SECRET_MANAGER_FLAG = 'Y'

# RDS
RDS_QUERY_PATH = "clinical-data-lake/uploads/VALIDATION_QUERIES/snfi_rds_validation_queries.xlsx"
OFR_QUERY_PATH = "clinical-data-lake/uploads/VALIDATION_QUERIES/snfi_dw_validation_queries.xlsx"
RUN_TIME_OUTPUT_LOCAL_PATH = "clinical-data-lake/uploads/VALIDATION_QUERIES/run_time_tracker.xlsx"
RUN_TIME_OUTPUT_PATH = "clinical-data-lake/uploads/VALIDATION_QUERIES/run_time_tracker.xlsx"
MSCK_REPAIR_TC_KEY = "refresh_table"
OFR_VALIDATION_OUTPUT_FILE_NAME = "snfi_validation_queries_output"
OFR_VALIDATION_OUTPUT_FILE_PATH = "clinical-data-lake/uploads/VALIDATION_QUERIES/"
OFR_VALIDATION_OUTPUT_FILE_S3_PATH = "clinical-data-lake/uploads/VALIDATION_QUERIES/"

# SSM
# SSM_DOC_NAME="EC2SampleCommandPOC"
SSM_DOC_NAME = "awsa0199-use1-00-0-d-ssm-snfi-air-mwaa01"
SSM_TIMEOUT = 18000
SSM_CLOUDWATCH_LOG_GRP_NM = "SystemsManager"
SSM_CLOUDWATCH_ENABLE_FLAG = True
SSM_LIST_CMD_INV_RETRY_LIMIT = 60
SSM_CMD_INV_RETRY_INTERVAL = 30

S3_PATH_PREFIX = "s3a://"
SEPARATOR = "/"

# Mapping QC

QC_QUERY_PATH = "s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/uploads/QC_QUERIES/mapping_qc.xlsx"

# ATHENA
UPLOAD_S3_PATH = "s3://aws-a0199-use1-00-q-s3b-snfi-ctf-data01/clinical-data-lake/uploads/"
WorkgroupName = "aws-a0199-use1-00-$$env-wkg-snfi-ctf-wkg01"

#DQMCheckUtility Constants
DATE_TIME_FORAMT_8 = "DD-MMM-YY"
DATE_TIME_FORAMT_8_REGEX = "([0-9]{2}-[A-Za-z]{3}-[0-9]{2})"
DATE_TIME_FORAMT_9 = "DD-MMM-YYYY"
DATE_TIME_FORAMT_9_REGEX = "([0-9]{2}-[A-Za-z]{3}-[0-9]{4})"
DATE_TIME_FORAMT_10 = "DD-MMM-YYYY HH.MM.SS.SSS A"
DATE_TIME_FORAMT_10_REGEX = "([0-9]{2}-[A-Za-z]{3}-[0-9]{4} [0-9]{2}\.[0-9]{2}\.[0-9]{2}\.[0-9]{3} [A-Za-z]{2})"