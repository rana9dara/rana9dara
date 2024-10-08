#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   Delta Records Standardization
#  Purpose             :   This module will perform the will check for delta records in sponsor, status , disease and
#                          phase mappings.
#  Input Parameters    :
#  Output Value        :   Mail will be sent
#  Pre-requisites      :
#  Last changed on     :   28th Feb 2022
#  Last changed by     :   Kashish Mogha
#  Reason for change   :   Enhancement to provide restartability at file level
# ######################################################################################################################


# Library and external modules declaration
from CommonUtils import CommonUtils
command = "sudo /usr/bin/pip-3.6 uninstall numpy -y"
execution_status = CommonUtils().execute_shell_command(command)
command4 = "sudo /usr/bin/pip-3.6 install numpy==1.19.5"
execution_status = CommonUtils().execute_shell_command(command4)
command3 = "sudo /usr/bin/pip-3.6 install pandas"
execution_status = CommonUtils().execute_shell_command(command3)

import json
import traceback
import sys
import os
import datetime
import time
sys.path.insert(0, os.getcwd())
from pyspark.sql import *
import CommonConstants as CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
from ConfigUtility import JsonConfigUtility
import MySQLdb
import smtplib
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import pyspark.sql.functions as f
import CommonConstants as CommonConstants
from PySparkUtility import PySparkUtility
from ExecutionContext import ExecutionContext
from DiseaseStandardizationProposed import DiseaseStandardizationProposed
import pandas as pd
from pyspark.sql.types import StructType,StructField, StringType, IntegerType



execution_context = ExecutionContext()
spark = PySparkUtility(execution_context).get_spark_context("DeltaAutomation",CommonConstants.HADOOP_CONF_PROPERTY_DICT)
#configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
email_recipient_list = configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_email_type_configurations", "ses_recipient_list"])
print(email_recipient_list)
sender_email = "sanofi.clinical@zs.com"
recipient_list = ", ".join(email_recipient_list)

def email_delta(message, subject, attachment=None):
    if message == "disease":
        body = 'Hello Team ,\n\nBased on our delta automator, attached is the list of diseases which are missing in the mapping file.\nPlease update this file on priority and acknowledge once done!\n\nRegards,\nSanofi DE Team\nClinical Development Excellence'
        attachment_name = 'disease_delta_records.csv'
        status = send_email(subject, body, delta_new, attachment_name)
        if status:
            print("Email sent Successfully")
        else:
            print("Check Logs")
    elif message == "No Delta":
        subject_email = subject
        body = 'Hello Team ,\n\nBased on our delta automator, no delta exists in the recently ingested data.\n\nRegards,\nSanofi DE Team\nClinical Development Excellence'
        status = send_email(subject_email, body)
        if status:
            print("Email sent Successfully")
        else:
            print("Check Logs")
    else:
        body = """Hello Team ,\n\nBased on our delta automator, attached are the {} delta records missing from the mapping file.\nPlease update this file on priority and acknowledge once done!\n\nRegards,\nSanofi DE Team\nClinical Development Excellence""".format(
            message)
        attachment_name = "{}_delta.csv".format(message)
        status = send_email(subject, body, attachment, attachment_name)
        if status:
            print("Email sent Successfully")
        else:
            print("Check Logs")


def send_email(mail_subject, body, attachment=None, attachment_name=None):
    try:
        subject = mail_subject
        # Create a multipart message and set headers
        msg = MIMEMultipart('alternative')
        msg["From"] = sender_email
        msg["To"] =recipient_list
        msg["Subject"] = subject
        msg.attach(MIMEText(body))
        if (attachment and attachment_name):
            part = MIMEApplication(attachment.toPandas().to_csv(escapechar="\\",doublequote=False,encoding='utf-8'), Name=attachment_name)
            content_dis = "attachment; filename=" + attachment_name
            part['Content-Disposition'] = content_dis
            msg.attach(part)
        server = smtplib.SMTP(host="10.121.0.176", port="25")
        server.connect("10.121.0.176", "25")
        server.starttls()
        server.send_message(msg)
        server.quit()
        return True
    except:
        raise Exception(
            "Issue Encountered while sending the email to recipients --> " + str(traceback.format_exc()))
        return False


configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
mysql_connection = MySQLConnectionManager().get_my_sql_connection()
cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)
delta_env=configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_automator_env"])
env=configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "environment"])
path_conf = ('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/delta_automator/automation_mapping_configuration.csv').replace("$$s3_env",delta_env)
automation_mapping_configuration = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load(path_conf)

result = []
counter = automation_mapping_configuration.select().where(
    automation_mapping_configuration.Type == 'Disease').count()
dataCollect = automation_mapping_configuration.rdd.toLocalIterator()
i = 0
count_citeline = 0
for row in dataCollect:
    print("Executing query to get latest successful batch_id")
    cursor.execute(
        "select batch_id from {orchestration_db_name}.log_file_smry where trim(lower(file_status)) = 'succeeded' and dataset_id={dataset_id} order by file_process_start_time desc limit 1 ".format(
            orchestration_db_name=audit_db, dataset_id=row['Dataset_Id']))
    fetch_enable_flag_result = cursor.fetchone()
    latest_stable_batch_id = str(fetch_enable_flag_result['batch_id'])
    if row['Type'] == "Disease":
        print("Disease")
        mapping_path = row['Mapping_File_Path'].replace("$$s3_env",delta_env)
        disease_mapping = spark.read.format('csv').option("header", "true").option("delimiter", ",").load(
            mapping_path)
        disease_mapping.createOrReplaceTempView("disease_mapping")
        file_path = "{Dataset_Path}/pt_batch_id={pt_batch_id}/".format(pt_batch_id=latest_stable_batch_id,
                                                                       Dataset_Path=row['Dataset_Path']).replace(
            '$$s3_env',
            delta_env)
        print(file_path)
        print(type(file_path), type(latest_stable_batch_id))
        sdf = spark.read.parquet(file_path)
        ds = row['Dataset_Name'].lower() + "_data"
        print("Datasourse name Sankarsh : ",ds)
        sdf.write.mode("overwrite").saveAsTable(ds)
        if row['Data_Source'].lower() == "dqs":
            dqs_query = """SELECT 'ir' AS source,disease AS disease_name FROM 
            default.dqs_study_data lateral VIEW posexplode (split(primary_indication,'\\\;'))three AS pos3,
            disease""".format(Column_Name=row['Column_Name'])
            df_dqs = spark.sql(dqs_query)
            df_dqs.write.mode("overwrite").saveAsTable("dqs_disease")
        elif row['Data_Source'].lower() == "citeline":
        
            print('**************',row['Dataset_Name'])
            if row['Dataset_Name'].lower()== "citeline_trialtrove":
                count_citeline+=1
                exp_1 = spark.sql("""SELECT   trial_id, Trim(ta)    AS therapeutic_area,Trim(dis)    AS disease_name,
                Trim(ps)   AS patient_segment
                FROM     default.citeline_trialtrove_data lateral view
                posexplode(split(trial_therapeutic_areas_name,'\\\|'))one AS pos1,
                ta lateral VIEW posexplode (split(disease_name,'\\\|'))two    AS pos2,
                dis lateral VIEW posexplode (split(patient_segment,'\\\|'))three AS pos3,
                ps WHERE    pos1=pos2 AND pos1=pos3 AND pos2=pos3 and dis <> ''
                GROUP BY 1,2,3,4 ORDER BY therapeutic_area
                """)
                exp_1.createOrReplaceTempView('exp_1')
                # applying explode on disease for citeline
                exp_2 = spark.sql("""SELECT   'citeline' as datasource,trial_id,
                therapeutic_area,
                Trim(dis)    AS disease_name,
                Trim(ps)      AS patient_segment
                FROM     exp_1
                lateral view posexplode (split(disease_name,'\\\^'))two    AS pos2,
                dis lateral VIEW posexplode (split(patient_segment,'\\\^'))three AS pos3,
                ps
                WHERE    pos2=pos3
                GROUP BY 1,2,3,4,5
                """)
                exp_2.createOrReplaceTempView('exp_2')
                print('trialtrove ran')
                if count_citeline==2:
                    df_citeline = spark.sql("""select 'citeline' as source, disease_name 
                    from exp_2 group by 1,2
                    union
                    select 'citeline' as source,disease_name
                    from citeline_pharma_diseases group by 1,2
                    """)
                    print('************citeline df ready')
                    
                    df_citeline.write.mode("overwrite").saveAsTable("citeline_disease")
                  
                # exp_2.write.mode('overwrite').saveAsTable('exp_2')
                # creating  trial -TA -disease- patient segment mapping for citeline
                
                ##
            elif row['Dataset_Name'].lower()== "citeline_pharmaprojects":
                count_citeline+=1
                citeline_pharma_temp=spark.sql("""
                select lower(drugprimaryname) as drg_nm,max(indications_diseasename) as disease_nm
                from 
                default.citeline_pharmaprojects_data citeline_pharma
                where lower(citeline_pharma.ispharmaprojectsdrug) ='true'
                group by 1
                """)
                citeline_pharma_temp.registerTempTable('citeline_pharma_temp')
    
                citeline_pharma_diseases=spark.sql("""
                select trim(disease_name1)  as disease_name,'' as trial_id,'' as therapeutic_area from citeline_pharma_temp  lateral view
                posexplode(split(regexp_replace(disease_nm,"\;","\\\|"),"\\\|"))one
                as pos1,disease_name1
                """)
                citeline_pharma_diseases.registerTempTable('citeline_pharma_diseases')
                print('pharma ran')
                if count_citeline==2:
                    df_citeline = spark.sql("""select 'citeline' as source, disease_name 
                    from exp_2 group by 1,2
                    union
                    select 'citeline' as source,disease_name
                    from citeline_pharma_diseases group by 1,2
                    """)
                    print('************citeline df ready')
                    df_citeline.write.mode("overwrite").saveAsTable("citeline_disease")
        elif row['Data_Source'].lower() == "aact":
            df_aact = spark.sql("""SELECT DISTINCT 'aact' as source,regexp_replace(name,'\\\"','\\\'')  AS disease_name
            from default.aact_conditions_data
            """)
            df_aact.write.mode("overwrite").saveAsTable("aact_disease")
        else:
            query2 = "select '{source}' as source,{column_name} as disease_name from " \
                     "default.{source}_data group by 1,2 ".format(source=row['Dataset_Name'].lower(),
                                                                  column_name=row['Column_Name'])
            data = spark.sql(query2)
            data.write.mode("overwrite").saveAsTable(row['Data_Source'] + '_disease')
        if i == 0 and row['Data_Source'].lower()!="citeline":
            query3 = "SELECT source, disease_name FROM default." + row['Data_Source'] + "_disease "
            print("inside if i:", i, query3)
        elif i == 0 and (row['Data_Source'].lower()=="citeline" and count_citeline==2) :
            query3 = "SELECT source, disease_name FROM default." + row['Data_Source'] + "_disease "
            print("inside if i:", i, query3)
        elif i>=1 and (count_citeline==2 and row['Data_Source'].lower()=="citeline") :
            query3 = query3 + " union SELECT source, disease_name FROM default." + row['Data_Source'] + "_disease"
            print("inside else i:", i, query3)
        elif i>=1 and row['Data_Source'].lower()!="citeline":
            query3 = query3 + " union SELECT source, disease_name FROM default." + row['Data_Source'] + "_disease"
            print("inside else i:", i, query3)
        i += 1
        print("outside if else", query3)
        df_union = spark.sql(query3)
        df_union.write.mode("overwrite").saveAsTable("df_union")
        print("after union", i)
        if counter == i:
            print("final", row['Data_Source'])
            # df_union.write.mode("overwrite").saveAsTable("df_union")
            delta_records = spark.sql("""select a.source as datasource,a.disease_name from  default.df_union a left join
            (select trial_id,datasource,disease as disease_name from disease_mapping )   b on
            lower(trim(a.disease_name)) =lower(trim(b.disease_name)) where
            ( b.disease_name  is null )
            """)
            delta_records.write.mode("overwrite").saveAsTable("delta_records")
            if (delta_records.count() == 0):
                print("No Delta")
                email_delta("No Delta", "Environment: {env} | [Update] No Action Required In Disease Mapping".format(env=env))
            else:
                delta_records.repartition(1).write.format('csv').option('header', True).mode('overwrite').option('sep', ',') \
                    .save('/user/hive/warehouse/Delta')
                dis_std = DiseaseStandardizationProposed()
                dis_std.main()
                d_map_path = ("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/temp_disease_mapping_holder/delt_disease_mapping_temp.csv").replace("$$s3_env",delta_env)
                #test=pd.read_csv('disease_mapping.csv')
                #test=test.astype(str)
                #disease_mapping=spark.createDataFrame(test)
                disease_mapping = spark.read.format('csv').option("header", "true").\
                option("delimiter", ",").\
                load(d_map_path)
                disease_mapping.write.mode('overwrite').saveAsTable("disease_mapping")
                delta_new=spark.sql("""select a.*,b.standard_disease as proposed_disease from delta_records a left join 
                (select trial_id,standard_disease,disease from default.disease_mapping )   b on
                lower(trim(a.disease_name)) =lower(trim(b.disease)) 
                """)
                email_delta("disease", "Environment: {env} | [URGENT] Update Delta Records In Disease Mapping".format(env=env))
                print("delta")
    else:
        print("hi")
        res = []
        print(row['Mapping_File_Path'] + "," + row['Type'], "in else")
        # latest_stable_file_id = str(fetch_enable_flag_result['file_id']).replace("-", "")
        path = "{Dataset_Path}/pt_batch_id={pt_batch_id}/".format(pt_batch_id=latest_stable_batch_id,
                                                                  Dataset_Path=row['Dataset_Path']).replace('$$s3_env',delta_env)
        print(path)
        previous_snapshot_data = spark.read.parquet(path)
        previous_snapshot_data.createOrReplaceTempView("previous_snapshot_data")
        query = """select distinct {Column_Name} as {Column_Name} from previous_snapshot_data where {Column_Name} is not null""".format(
            Column_Name=row['Column_Name'])
        if (row['multiple_values'] == "yes"):
            print("row multiple_values ", row['multiple_values'])
            print("Delim :" + row['delimiter'])
            df = spark.sql(query)
            for colname in df.columns:
                df = df.withColumn(colname, f.trim(f.col(colname)))
            if row['delimiter']=="pipe":
                res=df.withColumn(row['Column_Name'],f.explode(f.split(row['Column_Name'],"\\|"))).rdd.flatMap(lambda x: x).distinct().collect()
            elif row['delimiter']=="semicolon":
                res=df.withColumn(row['Column_Name'],f.explode(f.split(row['Column_Name'],";"))).rdd.flatMap(lambda x: x).distinct().collect()
        else:
            print("No multiple values")
            res = spark.sql(query).select(row['Column_Name']).rdd.flatMap(lambda x: x).distinct().collect()
        # Trim Spaces
        res = [x.strip() for x in res]
        # Remove Duplicates
        distinct_value = []
        [distinct_value.append(x) for x in res if x not in distinct_value]
        # Remove Empty Space
        distinct_value = [i for i in distinct_value if i]
        mapping_path = row['Mapping_File_Path'].replace("$$s3_env",delta_env)
        print(mapping_path)
        df_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',').load(mapping_path)
        df_mapping.createOrReplaceTempView('df_mapping')
        query_mapping = """select distinct {Column_Name} as {Column_Name} from df_mapping""".format(Column_Name=row['Mapping_File_Column_Name'])
        print(query_mapping)
        df_mapping_distinct_values = spark.sql(query_mapping).select(row['Mapping_File_Column_Name']).rdd.flatMap(lambda x: x).distinct().collect()
        print("Length of ",row['Type'],"distinct_value", len(distinct_value))
        print(distinct_value)
        print("df_mapping_distinct_values",df_mapping_distinct_values)
        delt=list(set(x.lower() for x in distinct_value) - set(x.lower() for x in df_mapping_distinct_values))
        print("Number of delta","For",row['Type'],len(delt))
        for j in delt:
            result.append([row['Type'], row['Data_Source'], row['Dataset_Id'], row['Dataset_Name'], row['Column_Name'], j,row['Mapping_File_Column_Name']])

if (len(result)):
    Typedf = [row['Type'] for row in automation_mapping_configuration.collect()]
    TypeList = list(set(Typedf))
    print("Delta Exists")
    df = spark.createDataFrame(result,
                               ['Type', 'Data_Source', 'Dataset_Id', 'Dataset_Name', 'Column_Name',
                                'Difference_Value',
                                'Mapping_File_Column_Name'])
    df_type_distinct_values = df.select('Type').rdd.flatMap(lambda x: x).distinct().collect()
    df.createOrReplaceTempView('df')
    for k in df_type_distinct_values:
        query_slice = """select * from df where Type='{k}'""".format(k=k)
        data_slice = spark.sql(query_slice)
        email_delta(k, "Environment: {env} | [URGENT] Update Delta Records In ".format(env=env) + k + " Mapping", data_slice)
        #data_slice.show()
    for each_type in TypeList:
        if each_type=="Disease":
            continue
        if each_type not in df_type_distinct_values:
            email_delta("No Delta","Environment: {env} | No Delta Records for ".format(env=env) + each_type + " Mapping")
else:
    print("No Delta Exist")
    Typedf = [row['Type'] for row in automation_mapping_configuration.collect()]
    TypeList = list(set(Typedf))
    # remove Disease from the list
    TypeList.remove('Disease')
    # Updated Mail Subject List
    print('Updated List: ', TypeList)
    str_subject = ','.join(TypeList)
    email_delta("No Delta", "Environment: {env} | [Update] No Action Required For: ".format(env=env)+str(str_subject)+" Mappings")
