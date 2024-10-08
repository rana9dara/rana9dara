 
 Insert into audit_information.ctl_dataset_master values (501,'raw','HEALTHVERITY','HEALTHVERITY','HEALTHVERITY','Clinical','NULL','NULL','NULL','NULL',NULL,'NULL','HEALTHVERITY_YYYYMMDD*.csv','N','N','csv',',','Y','N',NULL,'s3','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/pre-ingestion/HEALTHVERITY/HEALTHVERITY_DATA/','NULL',14,'s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/pre-landing/HEALTHVERITY/HEALTHVERITY_DATA/','/data/landing/HEALTHVERITY/HEALTHVERITY_DATA/','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/landing/HEALTHVERITY/HEALTHVERITY_DATA/','/data/pre-dqm/HEALTHVERITY/HEALTHVERITY_DATA/','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/pre-dqm/HEALTHVERITY/HEALTHVERITY_DATA/','/data/staging/HEALTHVERITY/HEALTHVERITY_DATA/','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/staging/HEALTHVERITY/HEALTHVERITY_DATA/','NULL','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/staging/HEALTHVERITY/HEALTHVERITY_DATA/','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/archive/HEALTHVERITY/HEALTHVERITY_DATA/','NULL','NULL','NULL','NULL','NULL','sanofi_ctfo_datastore_staging_dev.HEALTHVERITY','NULL','NULL','NULL','NULL','NULL','NULL','batch_id,file_id','NULL','NULL','NULL','NULL','NULL','','NULL','Y','latest','NULL','NULL','NULL','NULL','NULL','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/publish/staging/HEALTHVERITY/HEALTHVERITY_DATA/','NULL');
 
 
 
 Insert into audit_information.ctl_column_metadata values (1405,'HEALTH_VERITY',501,'npi','string','','','npi',1,'');
Insert into audit_information.ctl_column_metadata values (1406,'HEALTH_VERITY',501,'gender','string','','','gender',2,'');
Insert into audit_information.ctl_column_metadata values (1407,'HEALTH_VERITY',501,'city','string','','','city',3,'');
Insert into audit_information.ctl_column_metadata values (1408,'HEALTH_VERITY',501,'state','string','','','state',4,'');
Insert into audit_information.ctl_column_metadata values (1409,'HEALTH_VERITY',501,'zip','integer','','','zip',5,'');
Insert into audit_information.ctl_column_metadata values (1410,'HEALTH_VERITY',501,'specialty','string','','','specialty',6,'');
Insert into audit_information.ctl_column_metadata values (1411,'HEALTH_VERITY',501,'count_asian','integer','','','count_asian',7,'');
Insert into audit_information.ctl_column_metadata values (1412,'HEALTH_VERITY',501,'count_african_american','integer','','','count_african_american',8,'');
Insert into audit_information.ctl_column_metadata values (1413,'HEALTH_VERITY',501,'count_hispanic','integer','','','count_hispanic',9,'');
Insert into audit_information.ctl_column_metadata values (1414,'HEALTH_VERITY',501,'count_white','integer','','','count_white',10,'');
Insert into audit_information.ctl_column_metadata values (1415,'HEALTH_VERITY',501,'percent_asian','percentage','','','percent_asian',11,'');
Insert into audit_information.ctl_column_metadata values (1416,'HEALTH_VERITY',501,'percent_african_american','percentage','','','percent_african_american',12,'');
Insert into audit_information.ctl_column_metadata values (1417,'HEALTH_VERITY',501,'percent_hispanic','percentage','','','percent_hispanic',13,'');
Insert into audit_information.ctl_column_metadata values (1418,'HEALTH_VERITY',501,'percent_white','percentage','','','percent_white',14,'');


Insert into audit_information.ctl_dqm_master values (20328,501,'Null','npi','','','','C','','na17763',now(),'NULL','NULL','0');
Insert into audit_information.ctl_dqm_master values (20329,501,'Domain','Gender','Male,Female','','Gender is not Null','NC','','na17763',now(),'NULL','NULL','0');
Insert into audit_information.ctl_dqm_master values (20330,501,'Null','Gender','','','','NC','','na17763',now(),'NULL','','0');


Insert into audit_information.ctl_cluster_config values (500,'Sanofi-DataEngineering-L1-HEALTHVERITY-Monthly-Full','monthly','vpc-002dedeb4c69ac582','subnet-0173fdc9ec7312084','aws-a0199-use1-1a-d-emr-snfi-ctf-ingestion-HEALTHVERITY','us-east-2','emr-5.25.1','r4.2xlarge',NULL,'r4.xlarge',NULL,'SPOT','NULL',NULL,'NULL',3,NULL,'EMR_DefaultRole','aws-a0199-glbl-00-d-rol-snfi-ctf-EMR_EC2_role01','aws-a0199-use1-00-d-kpr-snfi-ctf-emr01','N','Y','{"log_uri": "s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/emrlogs", "applications": [{"Name": "Hive"}, {"Name": "Pig"}, {"Name": "Ganglia"}, {"Name": "Hue"}, {"Name": "Spark"}, {"Name": "Sqoop"}, {"Name": "Hadoop"}, {"Name": "Zeppelin"}, {"Name": "Livy"}, {"Name": "Tez"}, {"Name": "Presto"}], "cluster_tags": [{"Key": "Name", "Value": "aws-a0199-use1-1a-d-emr-snfi-ctf-ingestion-HEALTHVERITY"},{"Key": "ZS_Project_Code", "Value": "1205BO3611"}],"check_launch_flag": "N", "step_creation_property": [{"Name": "CustomJAR", "HadoopJarStep": {"Jar": "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar", "Args": ["s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/python3/emr_step_creation.sh"]}, "ActionOnFailure": "TERMINATE_CLUSTER"}],"termination_on_failure_flag": "N", "termination_on_success_flag": "Y","cc_branch_name": "integration", "dm_branch_name": "integration","hdfs_disk_space":500,"ServiceAccessSecurityGroup": "sg-081cd03adcb10733e", "EmrManagedSlaveSecurityGroup": "sg-0ae266ee04d7cc014", "EmrManagedMasterSecurityGroup": "sg-011a1c64d5af20322", "AdditionalMasterSecurityGroups": "sg-011a1c64d5af20322","security_conf": "aws-a0199-use1-00-d-sec-snfi-ctf-emr02", "cluster_bootstrap_s3_location": "s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/code/bootstrap/python3_emr_bootstrap_bitbucket.sh"}',0.378,NULL);



Insert into audit_information.ctl_workflow_master values (501,'Sanofi-DataEngineering-L1-HEALTHVERITY-Monthly-Full',501,500,'Y','na17763','now()','na17763',current_timestamp(),'{}','--driver-memory 3G --executor-cores 3 --executor-memory 17G --conf spark.driver.allowMultipleContexts=true --conf spark.scheduler.mode=FAIR --conf spark.sql.shuffle.repartitions=500 --conf spark.yarn.executor.memoryOverhead=2000M --conf spark.default.parallelism=700 --conf spark.port.maxRetries=200 --conf spark.rpc.askTimeout=600s --conf spark.network.timeout=10000002');
 
 Insert into audit_information.ctl_dataset_master values (501,'raw','HEALTHVERITY','HEALTHVERITY','HEALTHVERITY','Clinical','NULL','NULL','NULL','NULL',NULL,'NULL','HEALTHVERITY_YYYYMMDD*.csv','N','N','csv',',','Y','N',NULL,'s3','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/pre-ingestion/HEALTHVERITY/HEALTHVERITY_DATA/','NULL',14,'s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/pre-landing/HEALTHVERITY/HEALTHVERITY_DATA/','/data/landing/HEALTHVERITY/HEALTHVERITY_DATA/','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/landing/HEALTHVERITY/HEALTHVERITY_DATA/','/data/pre-dqm/HEALTHVERITY/HEALTHVERITY_DATA/','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/pre-dqm/HEALTHVERITY/HEALTHVERITY_DATA/','/data/staging/HEALTHVERITY/HEALTHVERITY_DATA/','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/staging/HEALTHVERITY/HEALTHVERITY_DATA/','NULL','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/staging/HEALTHVERITY/HEALTHVERITY_DATA/','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/archive/HEALTHVERITY/HEALTHVERITY_DATA/','NULL','NULL','NULL','NULL','NULL','sanofi_ctfo_datastore_staging_dev.HEALTHVERITY','NULL','NULL','NULL','NULL','NULL','NULL','batch_id,file_id','NULL','NULL','NULL','NULL','NULL','','NULL','Y','latest','NULL','NULL','NULL','NULL','NULL','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/publish/staging/HEALTHVERITY/HEALTHVERITY_DATA/','NULL');
 
 
 
 Insert into audit_information.ctl_column_metadata values (1405,'HEALTH_VERITY',501,'npi','string','','','npi',1,'');
Insert into audit_information.ctl_column_metadata values (1406,'HEALTH_VERITY',501,'gender','string','','','gender',2,'');
Insert into audit_information.ctl_column_metadata values (1407,'HEALTH_VERITY',501,'city','string','','','city',3,'');
Insert into audit_information.ctl_column_metadata values (1408,'HEALTH_VERITY',501,'state','string','','','state',4,'');
Insert into audit_information.ctl_column_metadata values (1409,'HEALTH_VERITY',501,'zip','integer','','','zip',5,'');
Insert into audit_information.ctl_column_metadata values (1410,'HEALTH_VERITY',501,'specialty','string','','','specialty',6,'');
Insert into audit_information.ctl_column_metadata values (1411,'HEALTH_VERITY',501,'count_asian','integer','','','count_asian',7,'');
Insert into audit_information.ctl_column_metadata values (1412,'HEALTH_VERITY',501,'count_african_american','integer','','','count_african_american',8,'');
Insert into audit_information.ctl_column_metadata values (1413,'HEALTH_VERITY',501,'count_hispanic','integer','','','count_hispanic',9,'');
Insert into audit_information.ctl_column_metadata values (1414,'HEALTH_VERITY',501,'count_white','integer','','','count_white',10,'');
Insert into audit_information.ctl_column_metadata values (1415,'HEALTH_VERITY',501,'percent_asian','percentage','','','percent_asian',11,'');
Insert into audit_information.ctl_column_metadata values (1416,'HEALTH_VERITY',501,'percent_african_american','percentage','','','percent_african_american',12,'');
Insert into audit_information.ctl_column_metadata values (1417,'HEALTH_VERITY',501,'percent_hispanic','percentage','','','percent_hispanic',13,'');
Insert into audit_information.ctl_column_metadata values (1418,'HEALTH_VERITY',501,'percent_white','percentage','','','percent_white',14,'');


Insert into audit_information.ctl_dqm_master values (20328,501,'Null','npi','','','','C','','na17763',now(),'NULL','NULL','0');
Insert into audit_information.ctl_dqm_master values (20329,501,'Domain','Gender','Male,Female','','Gender is not Null','NC','','na17763',now(),'NULL','NULL','0');
Insert into audit_information.ctl_dqm_master values (20330,501,'Null','Gender','','','','NC','','na17763',now(),'NULL','','0');


Insert into audit_information.ctl_cluster_config values (500,'Sanofi-DataEngineering-L1-HEALTHVERITY-Monthly-Full','monthly','vpc-002dedeb4c69ac582','subnet-0173fdc9ec7312084','aws-a0199-use1-1a-d-emr-snfi-ctf-ingestion-HEALTHVERITY','us-east-2','emr-5.25.1','r4.2xlarge',NULL,'r4.xlarge',NULL,'SPOT','NULL',NULL,'NULL',3,NULL,'EMR_DefaultRole','aws-a0199-glbl-00-d-rol-snfi-ctf-EMR_EC2_role01','aws-a0199-use1-00-d-kpr-snfi-ctf-emr01','N','Y','{"log_uri": "s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/emrlogs", "applications": [{"Name": "Hive"}, {"Name": "Pig"}, {"Name": "Ganglia"}, {"Name": "Hue"}, {"Name": "Spark"}, {"Name": "Sqoop"}, {"Name": "Hadoop"}, {"Name": "Zeppelin"}, {"Name": "Livy"}, {"Name": "Tez"}, {"Name": "Presto"}], "cluster_tags": [{"Key": "Name", "Value": "aws-a0199-use1-1a-d-emr-snfi-ctf-ingestion-HEALTHVERITY"},{"Key": "ZS_Project_Code", "Value": "1205BO3611"}],"check_launch_flag": "N", "step_creation_property": [{"Name": "CustomJAR", "HadoopJarStep": {"Jar": "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar", "Args": ["s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/python3/emr_step_creation.sh"]}, "ActionOnFailure": "TERMINATE_CLUSTER"}],"termination_on_failure_flag": "N", "termination_on_success_flag": "Y","cc_branch_name": "integration", "dm_branch_name": "integration","hdfs_disk_space":500,"ServiceAccessSecurityGroup": "sg-081cd03adcb10733e", "EmrManagedSlaveSecurityGroup": "sg-0ae266ee04d7cc014", "EmrManagedMasterSecurityGroup": "sg-011a1c64d5af20322", "AdditionalMasterSecurityGroups": "sg-011a1c64d5af20322","security_conf": "aws-a0199-use1-00-d-sec-snfi-ctf-emr02", "cluster_bootstrap_s3_location": "s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/code/bootstrap/python3_emr_bootstrap_bitbucket.sh"}',0.378,NULL);



Insert into audit_information.ctl_workflow_master values (501,'Sanofi-DataEngineering-L1-HEALTHVERITY-Monthly-Full',501,500,'Y','na17763','now()','na17763',current_timestamp(),'{}','--driver-memory 3G --executor-cores 3 --executor-memory 17G --conf spark.driver.allowMultipleContexts=true --conf spark.scheduler.mode=FAIR --conf spark.sql.shuffle.repartitions=500 --conf spark.yarn.executor.memoryOverhead=2000M --conf spark.default.parallelism=700 --conf spark.port.maxRetries=200 --conf spark.rpc.askTimeout=600s --conf spark.network.timeout=10000002');
 
 Insert into audit_information.ctl_dataset_master values (501,'raw','HEALTHVERITY','HEALTHVERITY','HEALTHVERITY','Clinical','NULL','NULL','NULL','NULL',NULL,'NULL','HEALTHVERITY_YYYYMMDD*.csv','N','N','csv',',','Y','N',NULL,'s3','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/pre-ingestion/HEALTHVERITY/HEALTHVERITY_DATA/','NULL',14,'s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/pre-landing/HEALTHVERITY/HEALTHVERITY_DATA/','/data/landing/HEALTHVERITY/HEALTHVERITY_DATA/','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/landing/HEALTHVERITY/HEALTHVERITY_DATA/','/data/pre-dqm/HEALTHVERITY/HEALTHVERITY_DATA/','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/pre-dqm/HEALTHVERITY/HEALTHVERITY_DATA/','/data/staging/HEALTHVERITY/HEALTHVERITY_DATA/','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/staging/HEALTHVERITY/HEALTHVERITY_DATA/','NULL','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/staging/HEALTHVERITY/HEALTHVERITY_DATA/','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/archive/HEALTHVERITY/HEALTHVERITY_DATA/','NULL','NULL','NULL','NULL','NULL','sanofi_ctfo_datastore_staging_dev.HEALTHVERITY','NULL','NULL','NULL','NULL','NULL','NULL','batch_id,file_id','NULL','NULL','NULL','NULL','NULL','','NULL','Y','latest','NULL','NULL','NULL','NULL','NULL','s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/publish/staging/HEALTHVERITY/HEALTHVERITY_DATA/','NULL');
 
 
 
 Insert into audit_information.ctl_column_metadata values (1405,'HEALTH_VERITY',501,'npi','string','','','npi',1,'');
Insert into audit_information.ctl_column_metadata values (1406,'HEALTH_VERITY',501,'gender','string','','','gender',2,'');
Insert into audit_information.ctl_column_metadata values (1407,'HEALTH_VERITY',501,'city','string','','','city',3,'');
Insert into audit_information.ctl_column_metadata values (1408,'HEALTH_VERITY',501,'state','string','','','state',4,'');
Insert into audit_information.ctl_column_metadata values (1409,'HEALTH_VERITY',501,'zip','integer','','','zip',5,'');
Insert into audit_information.ctl_column_metadata values (1410,'HEALTH_VERITY',501,'specialty','string','','','specialty',6,'');
Insert into audit_information.ctl_column_metadata values (1411,'HEALTH_VERITY',501,'count_asian','integer','','','count_asian',7,'');
Insert into audit_information.ctl_column_metadata values (1412,'HEALTH_VERITY',501,'count_african_american','integer','','','count_african_american',8,'');
Insert into audit_information.ctl_column_metadata values (1413,'HEALTH_VERITY',501,'count_hispanic','integer','','','count_hispanic',9,'');
Insert into audit_information.ctl_column_metadata values (1414,'HEALTH_VERITY',501,'count_white','integer','','','count_white',10,'');
Insert into audit_information.ctl_column_metadata values (1415,'HEALTH_VERITY',501,'percent_asian','percentage','','','percent_asian',11,'');
Insert into audit_information.ctl_column_metadata values (1416,'HEALTH_VERITY',501,'percent_african_american','percentage','','','percent_african_american',12,'');
Insert into audit_information.ctl_column_metadata values (1417,'HEALTH_VERITY',501,'percent_hispanic','percentage','','','percent_hispanic',13,'');
Insert into audit_information.ctl_column_metadata values (1418,'HEALTH_VERITY',501,'percent_white','percentage','','','percent_white',14,'');


Insert into audit_information.ctl_dqm_master values (20328,501,'Null','npi','','','','C','','na17763',now(),'NULL','NULL','0');
Insert into audit_information.ctl_dqm_master values (20329,501,'Domain','Gender','Male,Female','','Gender is not Null','NC','','na17763',now(),'NULL','NULL','0');
Insert into audit_information.ctl_dqm_master values (20330,501,'Null','Gender','','','','NC','','na17763',now(),'NULL','','0');


Insert into audit_information.ctl_cluster_config values (500,'Sanofi-DataEngineering-L1-HEALTHVERITY-Monthly-Full','monthly','vpc-002dedeb4c69ac582','subnet-0173fdc9ec7312084','aws-a0199-use1-1a-d-emr-snfi-ctf-ingestion-HEALTHVERITY','us-east-2','emr-5.25.1','r4.2xlarge',NULL,'r4.xlarge',NULL,'SPOT','NULL',NULL,'NULL',3,NULL,'EMR_DefaultRole','aws-a0199-glbl-00-d-rol-snfi-ctf-EMR_EC2_role01','aws-a0199-use1-00-d-kpr-snfi-ctf-emr01','N','Y','{"log_uri": "s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/emrlogs", "applications": [{"Name": "Hive"}, {"Name": "Pig"}, {"Name": "Ganglia"}, {"Name": "Hue"}, {"Name": "Spark"}, {"Name": "Sqoop"}, {"Name": "Hadoop"}, {"Name": "Zeppelin"}, {"Name": "Livy"}, {"Name": "Tez"}, {"Name": "Presto"}], "cluster_tags": [{"Key": "Name", "Value": "aws-a0199-use1-1a-d-emr-snfi-ctf-ingestion-HEALTHVERITY"},{"Key": "ZS_Project_Code", "Value": "1205BO3611"}],"check_launch_flag": "N", "step_creation_property": [{"Name": "CustomJAR", "HadoopJarStep": {"Jar": "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar", "Args": ["s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/python3/emr_step_creation.sh"]}, "ActionOnFailure": "TERMINATE_CLUSTER"}],"termination_on_failure_flag": "N", "termination_on_success_flag": "Y","cc_branch_name": "integration", "dm_branch_name": "integration","hdfs_disk_space":500,"ServiceAccessSecurityGroup": "sg-081cd03adcb10733e", "EmrManagedSlaveSecurityGroup": "sg-0ae266ee04d7cc014", "EmrManagedMasterSecurityGroup": "sg-011a1c64d5af20322", "AdditionalMasterSecurityGroups": "sg-011a1c64d5af20322","security_conf": "aws-a0199-use1-00-d-sec-snfi-ctf-emr02", "cluster_bootstrap_s3_location": "s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/code/bootstrap/python3_emr_bootstrap_bitbucket.sh"}',0.378,NULL);



Insert into audit_information.ctl_workflow_master values (501,'Sanofi-DataEngineering-L1-HEALTHVERITY-Monthly-Full',501,500,'Y','na17763','now()','na17763',current_timestamp(),'{}','--driver-memory 3G --executor-cores 3 --executor-memory 17G --conf spark.driver.allowMultipleContexts=true --conf spark.scheduler.mode=FAIR --conf spark.sql.shuffle.repartitions=500 --conf spark.yarn.executor.memoryOverhead=2000M --conf spark.default.parallelism=700 --conf spark.port.maxRetries=200 --conf spark.rpc.askTimeout=600s --conf spark.network.timeout=10000002');
