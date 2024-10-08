################################# Module Information ######################################
#  Module Name         : Dedupe Utility
#  Purpose             : This contains generic functions for maintaining Golden IDs across
#                        runs, incorporating KM valdidated output.
#  Pre-requisites      : This script should be present in the job executor folder.
#  Last changed on     : 08-01-2020
#  Last changed by     : Subhash Nadkarni
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

import os


def maintainGoldenID(cur_data_df, d_enty, process_id, prev_data_flg, env):
    """
    Purpose: This function maps the golden IDs of last successful run with the current run on the basis of hash_uid.
    Input  : Intermediate HDFS table consisting columns of hash_uid and golden ID evaluated for the latest run.
    Output : HDFS table with hash_uids mapped with the correct golden IDs based on correctness of previous and current run.
    """

    print("cur_data_df: {}, d_enty: {}, process_id: {}, prev_data_flg: {}".format(cur_data_df, d_enty, process_id,
                                                                                  prev_data_flg))

    # Defining cursor
    from PySparkUtility import PySparkUtility
    from CommonUtils import CommonUtils
    import CommonConstants as CommonConstants
    from MySQLConnectionManager import MySQLConnectionManager
    from ConfigUtility import JsonConfigUtility
    import MySQLdb

    configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    mysql_connection = MySQLConnectionManager().get_my_sql_connection()
    cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local").appName('sprk-job').enableHiveSupport().getOrCreate())
    spark.sql("""show databases""").show()
    spark.sql("""show tables""").show()
    try:

        # get latest successful data_dt and cycle_id
        print("Executing query to get latest successful cycle_id")
        cursor.execute(
            "select cycle_id,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = {process_id} order by cycle_start_time desc limit 1 ".format(
                orchestration_db_name=audit_db, process_id=process_id))
        fetch_enable_flag_result = cursor.fetchone()
        latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
        latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])

        # read data for latest data_date and cycle_id
        # data is getting as investigator under mastering therfore this change
        if (d_enty=="inv"):
             d_enty_path=d_enty
             d_enty_path=d_enty_path.replace("inv","investigator")
             path="s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/commons/temp/mastering/{}/temp_xref_{}/pt_data_dt={}/pt_cycle_id={}/".format(d_enty_path,d_enty,latest_stable_data_date,latest_stable_cycle_id).replace('$$s3_env', env)
             print(path)
             
             previous_snapshot_data = spark.read.parquet(path)
        else:
            previous_snapshot_data = spark.read.parquet("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/commons/temp/mastering/{}/"
            "temp_xref_{}/pt_data_dt={}/pt_cycle_id={}/".format(d_enty, d_enty, latest_stable_data_date,
                                                                latest_stable_cycle_id).replace('$$s3_env', env))
            

        previous_snapshot_data.registerTempTable("previous_snapshot_data")
        previous_data = spark.sql("""
        select
            hash_uid,
            """ + d_enty + """_id as golden_id
        from previous_snapshot_data
        group by 1,2
        """)
        previous_data.registerTempTable("previous_data")
        
        current_data = spark.sql("""
        select
            hash_uid,
             """ + d_enty + """_id as golden_id
        from """ + cur_data_df + """
        group by 1,2
        """)

        current_data.registerTempTable("current_data")
        
        # Mapping the golden id of previous run on basis of hash uid
        old_to_new_map_1 = spark.sql("""
        select
            a.*,
            concat('old-',b.golden_id) as old_golden_id,
            concat('new-',a.golden_id) as new_golden_id
       from current_data a
        left join (select hash_uid, golden_id from previous_data group by 1,2) b
        on a.hash_uid=b.hash_uid""")
        old_to_new_map_1.registerTempTable("old_to_new_map_1")
        old_to_new_map_1.write.mode("overwrite").saveAsTable("old_to_new_map_1_{}".format(d_enty))
        if prev_data_flg == 'N':
            print("New Run is correct")
            # Case - Old: Clustered New: Clustered + Extra
            old_to_new_map_2 = spark.sql("""
            select /*+ broadcast(b) */
                a.hash_uid,
                split(coalesce(a.old_golden_id,b.old_golden_id),'-')[1] as old_golden_id,
                a.new_golden_id
            from old_to_new_map_1 a
            left outer join
            (select old_golden_id, new_golden_id from old_to_new_map_1 where old_golden_id is not null group by 1,2 ) b
            on split(a.new_golden_id,'-')[1] = split(b.new_golden_id,'-')[1]
            group by 1,2,3
            """)

            old_to_new_map_2.registerTempTable("old_to_new_map_2")

            # Case - Old: Declustered New: Clustered
            old_new_golden_id_min = spark.sql("""
            select
                min(old_golden_id) as old_golden_id,
                new_golden_id
            from old_to_new_map_2
            group by 2
            """)

            old_new_golden_id_min.registerTempTable("old_new_golden_id_min")

            old_to_new_map_3 = spark.sql("""
            select /*+ broadcast(b) */
                a.hash_uid,
                b.old_golden_id,
                a.new_golden_id
            from old_to_new_map_2 a
            left outer join old_new_golden_id_min b
            on split(a.new_golden_id,'-')[1] = split(b.new_golden_id,'-')[1]
            group by 1,2,3
            """)

            old_to_new_map_3.registerTempTable("old_to_new_map_3")

            # Logic to nullify the old golden id for declustered records identified in new run
            old_new_golden_id_count = spark.sql("""
            select /*+ broadcast(b) */
                a.old_golden_id,
                a.new_golden_id,
                count(*) as rec_count
            from old_to_new_map_3 a
            inner join
            (select old_golden_id from (select old_golden_id, count(distinct new_golden_id) from old_to_new_map_3 group by 1 having count(distinct new_golden_id)> 1)) b
            on a.old_golden_id = b.old_golden_id
            group by 1,2
            """)

            old_new_golden_id_count.registerTempTable("old_new_golden_id_count")

            nullify_declustered_rec = spark.sql("""
            select
                new_golden_id,
                case when rec_count = 1 and a.new_golden_id <> b.min_golden_id then NULL else b.old_golden_id end as  old_golden_id
            from old_new_golden_id_count a
            left outer join
            (select old_golden_id, min(new_golden_id) as min_golden_id from old_new_golden_id_count group by 1) b
            on a.old_golden_id = b.old_golden_id
            """)

            nullify_declustered_rec.registerTempTable("nullify_declustered_rec")

            old_to_new_map_4 = spark.sql("""
            select /*+ broadcast(b) */
                a.hash_uid,
               case when b.new_golden_id is null then a.old_golden_id else b.old_golden_id end as old_golden_id,
                a.new_golden_id
            from old_to_new_map_3 a
            left outer join nullify_declustered_rec b
            on a.new_golden_id = b.new_golden_id
            """)

            old_to_new_map_4.write.mode("overwrite").saveAsTable("old_to_new_map_4_{}".format(d_enty))
        else:
            print("Old Run is correct")
            try:
                dedupe_km_input = spark.read.format('csv').option("header", "true").option("delimiter", ",").load(
                    "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/commons/temp/km_validation/km_inputs/{}/".format(
                       d_enty).replace('$$s3_env', env))
                dedupe_km_input.registerTempTable("dedupe_km_input")
                print('KM output loaded successfully')
                old_to_new_map_4 = spark.sql("""
                select /*+ broadcast(b) */
                    a.hash_uid,
                    case when km.new_cluster_id is not null then null else split(coalesce(a.old_golden_id,b.old_golden_id),'-')[1] end as old_golden_id,
                    coalesce(km.new_cluster_id,a.new_golden_id) as new_golden_id
                from old_to_new_map_1 a
                left outer join
                (select old_golden_id, new_golden_id from old_to_new_map_1 where old_golden_id is not null group by 1,2 ) b
                on split(a.new_golden_id,'-')[1] = split(b.new_golden_id,'-')[1]
                left outer join
                dedupe_km_input km
                on a.hash_uid = km.hash_uid
                group by 1,2,3
                """)

                old_to_new_map_4.write.mode("overwrite").saveAsTable("old_to_new_map_4_{}".format(d_enty))
            except Exception as e:
                print('KM output not available')
                old_to_new_map_4 = spark.sql("""
                select /*+ broadcast(b) */
                    a.hash_uid,
                    split(coalesce(a.old_golden_id,b.old_golden_id),'-')[1] as old_golden_id,
                    a.new_golden_id
                from old_to_new_map_1 a
                left outer join
                (select old_golden_id, new_golden_id from old_to_new_map_1 where old_golden_id is not null group by 1,2 ) b
                on split(a.new_golden_id,'-')[1] = split(b.new_golden_id,'-')[1]
                group by 1,2,3
                """)

                old_to_new_map_4.write.mode("overwrite").saveAsTable("old_to_new_map_4_{}".format(d_enty))
        # Generating new golden IDs for records having no golden ID assigned in previous run
        new_golden_id_for_null_rec = spark.sql("""
        select
            a.new_golden_id,
            concat("ctfo_","{}_", row_number() over(order by null) + coalesce(temp.max_id,0)) as final_golden_id
        from
        (select new_golden_id from old_to_new_map_4_{} where old_golden_id is null group by 1) a cross join
        (select max(cast(split(old_golden_id,"{}_")[1] as bigint)) as max_id from old_to_new_map_4_{}) temp""".format(
            d_enty, d_enty, d_enty, d_enty))

        new_golden_id_for_null_rec.registerTempTable("new_golden_id_for_null_rec")

        temp_golden_id_xref = spark.sql("""
                select
                    hash_uid,
                    new_golden_id as cur_run_id,
                    old_golden_id as """ + "ctfo_" +d_enty + """_id
                from old_to_new_map_4_""" + d_enty + """ where old_golden_id is not null
                union
                select
                    a.hash_uid,
                    a.new_golden_id as cur_run_id,
                    b.final_golden_id as """ + "ctfo_" + d_enty + """_id
                from old_to_new_map_4_""" + d_enty + """ a left outer join new_golden_id_for_null_rec b on a.new_golden_id = b.new_golden_id
                where a.old_golden_id is null""")

        temp_golden_id_xref.write.mode("overwrite").saveAsTable("maintained_golden_id_xref_" + d_enty)

        final_table = "maintained_golden_id_xref_" + d_enty
        print("Final Table created: ".format(final_table))
    except Exception as e:
        final_table = "{}".format(cur_data_df)
        print("Could not find data on s3 for any previous run",e)
    finally:
        print("Function for maintaining Golden ID executed")
    return final_table


def maintainUniqueID(cur_data_df, d_enty, process_id, env):
    """
    Purpose: This function maps the golden IDs of last successful run with the current run on the basis of hash_uid.
    Input  : Intermediate HDFS table consisting columns of hash_uid and golden ID evaluated for the latest run.
    Output : HDFS table with hash_uids mapped with the correct golden IDs based on correctness of previous and current run.
    """

    print("cur_data_df: {}, d_enty: {}, process_id: {}".format(cur_data_df, d_enty, process_id))

    # Defining cursor
    from PySparkUtility import PySparkUtility
    from CommonUtils import CommonUtils
    import CommonConstants as CommonConstants
    from MySQLConnectionManager import MySQLConnectionManager
    from ConfigUtility import JsonConfigUtility
    import MySQLdb

    configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    mysql_connection = MySQLConnectionManager().get_my_sql_connection()
    cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local").appName('sprk-job').enableHiveSupport().getOrCreate())
    spark.sql("""show databases""").show()
    spark.sql("""show tables""").show()
    try:
        # get latest successful data_dt and cycle_id
        print("Executing query to get latest successful cycle_id")
        cursor.execute(
            "select cycle_id,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = {process_id} order by cycle_start_time desc limit 1 ".format(
                orchestration_db_name=audit_db, process_id=process_id))
        fetch_enable_flag_result = cursor.fetchone()
        latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
        latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])
        # read data for latest data_date and cycle_id
        previous_snapshot_data = spark.read.parquet(
            "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/commons/temp/dimensions/temp_d_{}_hash_map/"
            "/pt_data_dt={}/pt_cycle_id={}/".format(d_enty, latest_stable_data_date, latest_stable_cycle_id).replace(
                '$$s3_env', env))

        previous_snapshot_data.registerTempTable("previous_snapshot_data")

        previous_data = spark.sql("""
        select
            hash_uid,
            {d_enty}_id as golden_id
        from previous_snapshot_data
        group by 1,2
        """.format(d_enty=d_enty))

        previous_data.registerTempTable("previous_data")

        current_data = spark.sql("""
        select
            hash_uid,
            {d_enty}_id as golden_id
        from {cur_data_df}
        group by 1,2
        """.format(d_enty=d_enty, cur_data_df=cur_data_df))

        current_data.registerTempTable("current_data")

        # Mapping the golden id of previous run on basis of hash uid
        old_to_new_map = spark.sql("""
        select
            a.*,
            b.golden_id as old_golden_id,
            a.golden_id as new_golden_id
        from current_data a
        left join (select hash_uid, golden_id from previous_data group by 1,2) b
        on a.hash_uid=b.hash_uid
        """)
        old_to_new_map.registerTempTable("old_to_new_map")

        # Generating new golden IDs for records having no golden ID assigned in previous run
        new_golden_id_for_null_rec = spark.sql("""
        select
            a.new_golden_id,
            concat("{d_enty}_", row_number() over(order by null) + coalesce(temp.max_id,0)) as final_golden_id
        from
        (select new_golden_id from old_to_new_map where old_golden_id is null group by 1) a cross join
        (select max(cast(split(old_golden_id,"{d_enty}_")[1] as bigint)) as max_id from old_to_new_map) temp""".format(
            d_enty=d_enty))

        new_golden_id_for_null_rec.registerTempTable("new_golden_id_for_null_rec")

        maintain_uniqueID_map = spark.sql("""
                select
                    c.hash_uid,
                    c.new_golden_id as cur_run_id,
                    c.old_golden_id as {d_enty}_id
                from old_to_new_map c where old_golden_id is not null
                union
                select
                    a.hash_uid,
                    a.new_golden_id as cur_run_id,
                    b.final_golden_id as {d_enty}_id
                from old_to_new_map a left outer join new_golden_id_for_null_rec b on a.new_golden_id = b.new_golden_id
                where a.old_golden_id is null""".format(d_enty=d_enty))
        maintain_uniqueID_map = maintain_uniqueID_map.withColumnRenamed(d_enty+"_id","ctfo_"+d_enty+"_id")
        maintain_uniqueID_map.write.mode("overwrite").saveAsTable("maintained_id_d_" + d_enty)

        final_table = "maintained_id_d_" + d_enty
        print("Final Table created: ".format(final_table))
    except Exception as e:
        final_table = "{}".format(cur_data_df)
        print("Could not find data on s3 for any previous run")
    finally:
        print("Function for maintaining Golden ID executed")
    return final_table


def KMValidate(data_df, d_enty, data_date, cycle_id, env):
    """
    Purpose: This function maps the KM cluster ID and KM score with the data on basis of hash_uid.
    Input  : Intermediate HDFS table consisting columns of cluster ID and score.
    Output : HDFS table with hash_uids mapped with the correct golden IDs based on KM validated output.
    """
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local").appName('sprk-job').enableHiveSupport().getOrCreate())
    km_output_available = 'N'
    path = "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/commons/temp/mastering/{}/table_name/pt_data_dt={}/pt_cycle_id={}".format(
        d_enty, data_date, cycle_id).replace('$$s3_env', env)
    try:
        dedupe_km_input = spark.read.format('csv').option("header", "true").option("delimiter", ",").load(
            "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/commons/temp/dimensions/{}/".format(
                d_enty).replace('$$s3_env', env))
        command = "aws s3 cp s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/commons/temp/km_validation/km_inputs/{}/ s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/commons/temp/km_validation/km_inputs/processed/{}/{}/{}/ --sse".format(
            d_enty, d_enty, data_date, cycle_id).replace('$$s3_env', env)
        os.system(command)
        print("\n\nKM data is available, starting KM validation step\n\n")
        km_output_available = 'Y'
        dedupe_km_input.write.mode('overwrite').saveAsTable('dedupe_km_input')
        # Modifying cluster ID and score based on KM input
        temp_all_dedupe_results_km_opt_union = spark.sql("""
        select
            a.hash_uid,
            coalesce(b.new_cluster_id, a.cluster_id) as final_cluster_id,
            cast(coalesce(b.new_score, a.score) as double) as final_score,
            coalesce(b.comment, '') as final_KM_comment
        from
        """ + data_df + """ a
        left outer join (select hash_uid, max(new_cluster_id) as new_cluster_id, max(new_score) as new_score, max(comment) as comment from dedupe_km_input group by 1) b
        on a.hash_uid = b.hash_uid
        """)
        # save on HDFS
        temp_all_dedupe_results_km_opt_union.write.mode("overwrite").saveAsTable(
            "temp_all_{}_dedupe_results_km_opt_union".format(d_enty))
    except Exception as e:
        if km_output_available == 'N':
            print("KM output is not available")
            temp_all_dedupe_results_km_opt_union = spark.sql("""
            select
                hash_uid,
                cluster_id as final_cluster_id,
                score as final_score,
                '' final_KM_comment
            from
            """ + data_df + """
            """)
            # save on HDFS
            temp_all_dedupe_results_km_opt_union.write.mode("overwrite").saveAsTable(
                "temp_all_{}_dedupe_results_km_opt_union".format(d_enty))
        else:
            print(
                "\n\nKM data was present, but a runtime exception occurred during KM validation. ERROR - {}\n\n".format(
                    str(e)))
            raise
    finally:
        temp_all_dedupe_results_km_opt_union_final = spark.sql("""
        select
            a.*,
            b.final_cluster_id,
            b.final_score,
            b.final_KM_comment
        from
        """ + data_df + """ a
        left outer join
        temp_all_{d_enty}_dedupe_results_km_opt_union b
        on a.hash_uid = b.hash_uid
        """.format(d_enty=d_enty))
        temp_all_dedupe_results_km_opt_union_final.write.mode("overwrite").saveAsTable(
            "temp_all_{}_dedupe_results_km_opt_union_final".format(d_enty))
        # Push to S3
        write_path = path.replace("table_name", "temp_all_{}_dedupe_results_km_opt_union_final".format(d_enty))
        temp_all_dedupe_results_km_opt_union_final.repartition(100).write.mode("overwrite").parquet(write_path)
        print("KM Validation completed")
    return "temp_all_{}_dedupe_results_km_opt_union_final".format(d_enty)


