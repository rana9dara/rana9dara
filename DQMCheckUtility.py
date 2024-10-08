#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package and available on bit bucket code repository ( https://sourcecode.jnj.com/projects/ASX-NCKV/repos/jrd_fido_cc/browse ) & all static servers.

#####################################################Module Information################################################
#  Module Name         :   DQMCheckUtility
#  Purpose             :   This module will perform the actual DQM checks on all QC Ids fetched from DQM configs.
#  Input Parameters    :   file_location, DQM checks dict, file id
#  Output Value        :   returns the status SUCCESS or FAILURE
#  Pre-requisites      :
#  Last changed on     :   15 July 2019
#  Last changed by     :   Ashutosh Soni
#  Reason for change   :   DQM Redesign
#######################################################################################################################

# Library and external modules declaration

import sys
import os
import re

sys.path.insert(0, os.getcwd())
import traceback
from datetime import datetime
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col, when, lit, length, count, concat
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
from ExecutionContext import ExecutionContext
from LogSetup import logger
import CommonConstants as CommonConstants
from CommonUtils import CommonUtils
from MySQLConnectionManager import MySQLConnectionManager
from ConfigUtility import JsonConfigUtility

# all module level constants are defined here

MODULE_NAME = 'DQMCheckUtility'
TEMP_DF_FLAG_COLUMN = 'flag'
TEMP_DF_FLAG_COLUMN_PASS_VALUE = '1'


class DQMCheckUtility:

    # Default constructor

    def __init__(self, execution_context=None):
        self.execution_context = ExecutionContext()
        self.mysql_connector = MySQLConnectionManager()
        self.excptn_in_summary_check_flag = False
        self.app_id = ''
        self.dataset_id = 0
        self.batch_id = 0
        self.qc_id = 0
        self.qc_status = ''
        if execution_context is None:
            self.execution_context.set_context({'module_name': MODULE_NAME})
        else:
            self.execution_context = execution_context

        self.execution_context.set_context({'module_name': MODULE_NAME})

    def __enter__(self):
        return self

    def __exit__(
            self,
            exc_type,
            exc_val,
            exc_tb,
    ):
        pass

    # ########################################### save_dqm_error_summary ###############################################
    # Purpose            :   Save DQM Error Details And Summary
    # Input              :   dqm checks,file dataframe,file id,bathc id,sql context
    # Output             :   NA
    #
    #
    #
    # ##################################################################################################################

    def save_dqm_error_summary(
            self,
            log_info,
            dqm_checks=None,
            uni_list=None,
            file_df=None,
            batch_id=None,
            spark_context=None,
            file_field_delimeter=None,
    ):
        file_error_df = None
        status_message = ''

        # file_df.explain(extended=True)

        total_count_df = \
            file_df.groupby(CommonConstants.PARTITION_FILE_ID).count()
        total_count_dict = {}
        total_count_rows = total_count_df.collect()
        for row in total_count_rows:
            file_id = row.asDict()[CommonConstants.PARTITION_FILE_ID]
            count = row.asDict()['count']
            total_count_dict[file_id] = count

        error_schema = StructType([
            StructField(CommonConstants.BATCH_ID, StringType(), False),
            StructField(CommonConstants.PARTITION_FILE_ID,
                        StringType(), True),
            StructField(CommonConstants.ROW_ID, StringType(), True),
            StructField(CommonConstants.QC_ID, StringType(), True),
            StructField(CommonConstants.QC_TYPE, StringType(), True),
            StructField(CommonConstants.DATASET_ID, StringType(),
                        True),
            StructField(CommonConstants.COLUMN_NAME, StringType(),
                        True),
            StructField(CommonConstants.ERROR_VALUE, StringType(),
                        True),
            StructField(CommonConstants.CRITICALITY, StringType(),
                        True),
            StructField(CommonConstants.CREATE_BY, StringType(), True),
            StructField(CommonConstants.CREATE_TS, StringType(), True),
            StructField('app_id', StringType(), True),
        ])
        error_union_df = spark_context.createDataFrame([], error_schema)
        summary_schema = StructType([
            StructField(CommonConstants.QC_ID, StringType(), False),
            StructField(CommonConstants.ERROR_COUNT, StringType(),
                        True),
            StructField(CommonConstants.BATCH_ID, StringType(), True),
            StructField(CommonConstants.DATASET_ID, StringType(),
                        True),
            StructField(CommonConstants.PARTITION_FILE_ID,
                        StringType(), True),
            StructField(CommonConstants.QC_TYPE, StringType(), True),
            StructField(CommonConstants.QC_PARAM, StringType(), True),
            StructField(CommonConstants.QC_MESSAGE, StringType(),
                        True),
            StructField(CommonConstants.CRITICALITY, StringType(),
                        True),
            StructField(CommonConstants.QC_FAILURE_PERCENTAGE,
                        StringType(), True),
            StructField(CommonConstants.CREATE_BY, StringType(), True),
            StructField(CommonConstants.CREATE_TS, StringType(), True),
            StructField('app_id', StringType(), True),
            StructField('total_count', StringType(), True),
        ])
        summary_union_df = spark_context.createDataFrame([],
                                                         summary_schema)
        try:
            status_message = \
                'Starting Function to Write DQM Error And Summary'
            self.execution_context.set_context({'function_status': 'STARTED'
                                                })
            logger.info(status_message,
                        extra=self.execution_context.get_context())

            self.batch_id = batch_id
            checkpoint_count = 0
            file_id_list_processed = []
            for dqm_error in dqm_checks:

                if checkpoint_count > CommonConstants.CHECKPOINT_COUNT:
                    file_error_df.checkpoint()
                    checkpoint_count = 0
                app_id = dqm_error['application_id']
                self.app_id = app_id
                qc_id = str(dqm_error[CommonConstants.QC_ID])
                self.qc_id = qc_id
                qc_type = str(dqm_error[CommonConstants.QC_TYPE])
                dataset_id_fk = \
                    str(dqm_error[CommonConstants.DATSET_ID])
                self.dataset_id = dataset_id_fk
                column_name = \
                    str(dqm_error[CommonConstants.COLUMN_NAME])
                criticality = \
                    str(dqm_error[CommonConstants.CRITICALITY])
                create_by = str(dqm_error[CommonConstants.CREATE_BY])
                create_ts = \
                    str(datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip())
                qc_param = str(dqm_error[CommonConstants.QC_PARAM])

                if qc_type == CommonConstants.DQM_CUSTOM_CHECK:
                    file_error_df = file_df.filter(col(str(qc_id))
                                                   == CommonConstants.DQM_FAILED_STATUS).withColumn(
                        CommonConstants.BATCH_ID,
                        lit(batch_id)).withColumn(CommonConstants.DATASET_ID,
                                                  lit(dataset_id_fk)).withColumn(CommonConstants.QC_ID,
                                                                                 lit(qc_id)).withColumn(
                        CommonConstants.QC_TYPE,
                        lit(qc_type)).withColumn(CommonConstants.COLUMN_NAME,
                                                 lit(column_name)).withColumn(CommonConstants.ERROR_VALUE,
                                                                              lit(
                                                                                  CommonConstants.NOT_AVAILABLE)).withColumn(
                        CommonConstants.CRITICALITY,
                        lit(criticality)).withColumn(CommonConstants.CREATE_BY,
                                                     lit(create_by)).withColumn(CommonConstants.CREATE_TS,
                                                                                lit(create_ts)).withColumn('app_id',
                                                                                                           lit(
                                                                                                               app_id)).select(
                        col(CommonConstants.BATCH_ID),
                        col(CommonConstants.PARTITION_FILE_ID),
                        col(CommonConstants.ROW_ID),
                        col(CommonConstants.QC_ID),
                        col(CommonConstants.QC_TYPE),
                        col(CommonConstants.DATASET_ID),
                        col(CommonConstants.COLUMN_NAME),
                        col(CommonConstants.ERROR_VALUE),
                        col(CommonConstants.CRITICALITY),
                        col(CommonConstants.CREATE_BY),
                        col(CommonConstants.CREATE_TS),
                        col('app_id'),
                    )
                elif qc_type == CommonConstants.DQM_UNIQUE_CHECK:

                    column_name_list = column_name.split(',', -1)
                    column_name_expr = ''
                    l = len(column_name_list)
                    counter = 0
                    if file_field_delimeter is None:
                        file_field_delimeter = '|'
                    elif file_field_delimeter == '':
                        file_field_delimeter = '|'

                    for c_name in column_name_list:
                        if counter == 0:
                            column_name_expr = 'concat(when(col("' \
                                               + str(c_name) + '").isNotNull(), col("' \
                                               + str(c_name) \
                                               + '")).otherwise(lit("NA"))'
                        else:
                            column_name_expr += ',lit("' \
                                                + str(file_field_delimeter) \
                                                + '"),when(col("' + str(c_name) \
                                                + '").isNotNull(), col("' + str(c_name) \
                                                + '")).otherwise(lit("NA"))'

                        counter = counter + 1
                    if l >= 1:
                        column_name_expr += ')'
                    file_error_df = file_df.filter(col(str(qc_id))
                                                   == CommonConstants.DQM_FAILED_STATUS).withColumn(
                        CommonConstants.BATCH_ID,
                        lit(batch_id)).withColumn(CommonConstants.DATASET_ID,
                                                  lit(dataset_id_fk)).withColumn(CommonConstants.QC_ID,
                                                                                 lit(qc_id)).withColumn(
                        CommonConstants.QC_TYPE,
                        lit(qc_type)).withColumn(CommonConstants.COLUMN_NAME,
                                                 lit(column_name)).withColumn(CommonConstants.ERROR_VALUE,
                                                                              eval(column_name_expr)).withColumn(
                        CommonConstants.CRITICALITY,
                        lit(criticality)).withColumn(CommonConstants.CREATE_BY,
                                                     lit(create_by)).withColumn(CommonConstants.CREATE_TS,
                                                                                lit(create_ts)).withColumn('app_id',
                                                                                                           lit(
                                                                                                               app_id)).select(
                        col(CommonConstants.BATCH_ID),
                        col(CommonConstants.PARTITION_FILE_ID),
                        col(CommonConstants.ROW_ID),
                        col(CommonConstants.QC_ID),
                        col(CommonConstants.QC_TYPE),
                        col(CommonConstants.DATASET_ID),
                        col(CommonConstants.COLUMN_NAME),
                        col(CommonConstants.ERROR_VALUE),
                        col(CommonConstants.CRITICALITY),
                        col(CommonConstants.CREATE_BY),
                        col(CommonConstants.CREATE_TS),
                        col('app_id'),
                    )
                else:
                    file_error_df = file_df.filter(col(str(qc_id))
                                                   == CommonConstants.DQM_FAILED_STATUS).withColumn(
                        CommonConstants.BATCH_ID,
                        lit(batch_id)).withColumn(CommonConstants.DATASET_ID,
                                                  lit(dataset_id_fk)).withColumn(CommonConstants.QC_ID,
                                                                                 lit(qc_id)).withColumn(
                        CommonConstants.QC_TYPE,
                        lit(qc_type)).withColumn(CommonConstants.COLUMN_NAME,
                                                 lit(column_name)).withColumn(CommonConstants.ERROR_VALUE,
                                                                              col(column_name)).withColumn(
                        CommonConstants.CRITICALITY,
                        lit(criticality)).withColumn(CommonConstants.CREATE_BY,
                                                     lit(create_by)).withColumn(CommonConstants.CREATE_TS,
                                                                                lit(create_ts)).withColumn('app_id',
                                                                                                           lit(
                                                                                                               app_id)).select(
                        col(CommonConstants.BATCH_ID),
                        col(CommonConstants.PARTITION_FILE_ID),
                        col(CommonConstants.ROW_ID),
                        col(CommonConstants.QC_ID),
                        col(CommonConstants.QC_TYPE),
                        col(CommonConstants.DATASET_ID),
                        col(CommonConstants.COLUMN_NAME),
                        col(CommonConstants.ERROR_VALUE),
                        col(CommonConstants.CRITICALITY),
                        col(CommonConstants.CREATE_BY),
                        col(CommonConstants.CREATE_TS),
                        col('app_id'),
                    )
                error_c = \
                    file_error_df.filter(col(CommonConstants.QC_ID)
                                         == qc_id).groupby(CommonConstants.PARTITION_FILE_ID,
                                                           CommonConstants.QC_ID).count().select(
                        CommonConstants.PARTITION_FILE_ID,
                        'count')
                try:
                    len_error_c = len(error_c.head(1))
                    logger.info('Value of len_error_c is :- ' + str(len_error_c) + "  ")
                except:
                    logger.info('Exception in len(error_c.head(1)) : manually placing value of len_error_c = 1')
                    len_error_c = 1

                error_count = 0
                qc_failure_percentage = 0
                total_count = 0
                qc_message = str(error_count) \
                             + ' Records Out Of Total Records ' \
                             + str(total_count) + ' Failed'
                error_summary_df = spark_context.createDataFrame([(
                    str(qc_id),
                    0,
                    str(batch_id),
                    str(dataset_id_fk),
                    str(0),
                    str(qc_type),
                    str(qc_param),
                    str(qc_message),
                    str(criticality),
                    str(qc_failure_percentage),
                    str(create_by),
                    str(create_ts),
                    str(app_id),
                )], [
                    CommonConstants.QC_ID,
                    CommonConstants.ERROR_COUNT,
                    CommonConstants.BATCH_ID,
                    CommonConstants.DATASET_ID,
                    CommonConstants.PARTITION_FILE_ID,
                    CommonConstants.QC_TYPE,
                    CommonConstants.QC_PARAM,
                    CommonConstants.QC_MESSAGE,
                    CommonConstants.CRITICALITY,
                    CommonConstants.QC_FAILURE_PERCENTAGE,
                    CommonConstants.CREATE_BY,
                    CommonConstants.CREATE_TS,
                    'app_id',
                ])
                if len_error_c == 0:
                    qc_failure_percentage = 0

                    file_id_result = \
                        CommonUtils().get_file_ids_by_batch_id(CommonConstants.AUDIT_DB_NAME,
                                                               batch_id)
                    for i in range(0, len(file_id_result)):
                        file_id = file_id_result[i]['file_id']
                        total_count = total_count_dict.get(file_id)
                        qc_message = str(error_count) \
                                     + ' Records Out Of Total Records ' \
                                     + str(total_count) + ' Failed'
                        temp_error_summary_df = \
                            spark_context.createDataFrame([(
                                str(qc_id),
                                0,
                                str(batch_id),
                                str(dataset_id_fk),
                                str(file_id),
                                str(qc_type),
                                str(qc_param),
                                str(qc_message),
                                str(criticality),
                                str(qc_failure_percentage),
                                str(create_by),
                                str(create_ts),
                                str(app_id),
                            )], [
                                CommonConstants.QC_ID,
                                CommonConstants.ERROR_COUNT,
                                CommonConstants.BATCH_ID,
                                CommonConstants.DATASET_ID,
                                CommonConstants.PARTITION_FILE_ID,
                                CommonConstants.QC_TYPE,
                                CommonConstants.QC_PARAM,
                                CommonConstants.QC_MESSAGE,
                                CommonConstants.CRITICALITY,
                                CommonConstants.QC_FAILURE_PERCENTAGE,
                                CommonConstants.CREATE_BY,
                                CommonConstants.CREATE_TS,
                                'app_id',
                            ])
                        error_summary_df = \
                            error_summary_df.union(temp_error_summary_df)
                    error_summary_df = \
                        error_summary_df.filter(col('qc_message')
                                                != '0 Records Out Of Total Records 0 Failed'
                                                )
                else:
                    error_summary_df = \
                        file_error_df.groupby(CommonConstants.PARTITION_FILE_ID,
                                              CommonConstants.QC_ID).count().withColumnRenamed('count'
                                                                                               ,
                                                                                               CommonConstants.ERROR_COUNT).withColumn(
                            CommonConstants.BATCH_ID,
                            lit(batch_id)).withColumn(CommonConstants.DATASET_ID,
                                                      lit(dataset_id_fk)).withColumn(CommonConstants.QC_TYPE,
                                                                                     lit(qc_type)).withColumn(
                            CommonConstants.QC_PARAM,
                            lit(qc_param)).withColumn(CommonConstants.CRITICALITY,
                                                      lit(criticality)).withColumn(CommonConstants.CREATE_BY,
                                                                                   lit(create_by)).withColumn(
                            CommonConstants.CREATE_TS,
                            lit(create_ts)).withColumn('app_id',
                                                       lit(app_id)).select(
                            col(CommonConstants.QC_ID),
                            col(CommonConstants.PARTITION_FILE_ID),
                            col(CommonConstants.ERROR_COUNT),
                            col(CommonConstants.BATCH_ID),
                            col(CommonConstants.DATASET_ID),
                            col(CommonConstants.QC_TYPE),
                            col(CommonConstants.QC_PARAM),
                            col(CommonConstants.CRITICALITY),
                            col(CommonConstants.CREATE_BY),
                            col(CommonConstants.CREATE_TS),
                            col('app_id'),
                        )
                col_a = str('a.' + CommonConstants.PARTITION_FILE_ID)
                col_b = str('b.' + CommonConstants.PARTITION_FILE_ID)
                error_summary_df = error_summary_df.alias('a'
                                                          ).join(total_count_df.alias('b'), col(col_a)
                                                                 == col(col_b), how='inner'
                                                                 ).selectExpr('a.*',
                                                                              'b.count as total_count')

                error_summary_df = \
                    error_summary_df.withColumn(CommonConstants.QC_MESSAGE,
                                                concat(error_summary_df['error_count'],
                                                       lit(' Records Out Of Total Records '),
                                                       error_summary_df['total_count'], lit(' Failed'
                                                                                            ))).withColumn(
                        CommonConstants.QC_FAILURE_PERCENTAGE,
                        lit(error_summary_df['error_count'] * 1.0
                            / error_summary_df['total_count']
                            * 100)).select(
                        col(CommonConstants.QC_ID),
                        col(CommonConstants.ERROR_COUNT),
                        col(CommonConstants.BATCH_ID),
                        col(CommonConstants.DATASET_ID),
                        col(CommonConstants.PARTITION_FILE_ID),
                        col(CommonConstants.QC_TYPE),
                        col(CommonConstants.QC_PARAM),
                        col(CommonConstants.QC_MESSAGE),
                        col(CommonConstants.CRITICALITY),
                        col(CommonConstants.QC_FAILURE_PERCENTAGE),
                        col(CommonConstants.CREATE_BY),
                        col(CommonConstants.CREATE_TS),
                        col('app_id'),
                        col('total_count'),
                    )

                error_union_df = error_union_df.unionAll(file_error_df)
                summary_union_df = \
                    summary_union_df.unionAll(error_summary_df)
                logger.info('Done creating summary_union_df')

                # Logic to get details for individual file
                logger.info('Started marking qc_status as "Succeded"')
                error_summary_df.drop('total_count')
                error_summary_rows = error_summary_df.collect()
                error_summary_df.unpersist()

                file_id_list_processed = []

                for row in error_summary_rows:
                    log_info['file_id'] = \
                        row.asDict()[CommonConstants.PARTITION_FILE_ID]
                    file_id_list_processed.append(log_info['file_id'])
                    log_info['qc_id'] = row.asDict()['qc_id']
                    log_info['error_count'] = row.asDict()['error_count'
                    ]
                    log_info['qc_message'] = row.asDict()['qc_message']
                    log_info['qc_failure_percentage'] = \
                        row.asDict()['qc_failure_percentage']

                    # Here we will mark qc_status as "Succeded"

                    self.qc_status = CommonConstants.STATUS_SUCCEEDED
                    qc_end_time = \
                        datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip()
                    log_info['qc_status'] = self.qc_status
                    log_info['qc_end_time'] = qc_end_time
                    CommonUtils().update_dqm_status(log_info)
                checkpoint_count = checkpoint_count + 1
                logger.info('Done marking qc_status as "Succeded"')
                # #Changes to handle logging for file_ids which were left in IN PROGRESS State
                logger.info('Started Changes to handle logging for file_ids which were left in IN PROGRESS State')
                file_id_result = \
                    CommonUtils().get_file_ids_by_batch_id(CommonConstants.AUDIT_DB_NAME,
                                                           batch_id)
                file_id_list_total = []
                for i in range(0, len(file_id_result)):
                    file_id = file_id_result[i]['file_id']
                    file_id_list_total.append(file_id)

                file_ids_not_processed = list(set(file_id_list_total)
                                              - set(file_id_list_processed))
                logger.info('Unprocessed File ID List:'
                            + str(file_ids_not_processed))
                for file_id in file_ids_not_processed:
                    log_info['file_id'] = file_id
                    log_info['qc_id'] = qc_id
                    log_info['error_count'] = '0'
                    total_count = total_count_dict.get(file_id)
                    qc_message = '0' + ' Records Out Of Total Records ' \
                                 + str(total_count) + ' Failed'
                    log_info['qc_message'] = qc_message
                    log_info['qc_failure_percentage'] = '0'
                    self.qc_status = CommonConstants.STATUS_SUCCEEDED
                    qc_end_time = \
                        datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip()
                    log_info['qc_status'] = self.qc_status
                    log_info['qc_end_time'] = qc_end_time
                    CommonUtils().update_dqm_status(log_info)
                logger.info('Done Changes to handle logging for file_ids which were left in IN PROGRESS State')
            logger.info('Done updating DQM summary logs')
            error_union_df.createOrReplaceTempView('error_union_df')

            # Filter error details for all the non-unique checks#
            logger.info('Started Filter error details for all the non-unique checks')
            for uni in uni_list:
                uni_qc_id = uni[CommonConstants.QC_ID]
                app_id = uni['application_id']
                self.app_id = app_id
                self.qc_id = uni_qc_id
                qc_type = str(uni[CommonConstants.QC_TYPE])
                dataset_id_fk = str(uni[CommonConstants.DATSET_ID])
                self.dataset_id = dataset_id_fk
                column_name = str(uni[CommonConstants.COLUMN_NAME])
                criticality = str(uni[CommonConstants.CRITICALITY])
                create_by = str(uni[CommonConstants.CREATE_BY])
                create_ts = \
                    str(datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip())
                qc_param = str(uni[CommonConstants.QC_PARAM])
                df_non_unique = \
                    spark_context.sql('select * from error_union_df where qc_id !='
                                      + str(uni_qc_id) + " and criticality ='C'")
                df_non_unique.createOrReplaceTempView('df_non_unique')
                temp_unique = \
                    spark_context.sql('select * from error_union_df where qc_id='
                                      + str(uni_qc_id) + " and criticality = 'C'")
                temp_unique.createOrReplaceTempView('temp_unique')
                records_to_save = temp_unique.alias('a'
                                                    ).join(df_non_unique.alias('b'), col('a.row_id'
                                                                                         ) == col('b.row_id'),
                                                           how='left_outer'
                                                           ).selectExpr('a.*',
                                                                        'b.row_id as tem_row_id'
                                                                        ).filter(col('tem_row_id'
                                                                                     ).isNull()).drop('tem_row_id')
                records_to_save.createOrReplaceTempView('records_to_save'
                                                        )

                logger.info('Done Filter error details for all the non-unique checks')
                # #pick one random record from the records that need to be saved###

                logger.info('Started pick one random record from the records that need to be saved')
                save_single_record = \
                    spark_context.sql('select batch_id,'
                                      + CommonConstants.PARTITION_FILE_ID
                                      + ',row_id ,qc_id,qc_type,dataset_id,column_name,error_value,criticality,insert_by,insert_date,app_id from(select *, rank() over(partition by batch_id,qc_id,qc_type,dataset_id,column_name,error_value,criticality,insert_by,insert_date,app_id order by '
                                      + CommonConstants.PARTITION_FILE_ID
                                      + ' desc,row_id desc) as rnk from records_to_save) where rnk =1'
                                      )
                save_single_record.createOrReplaceTempView('save_single_record'
                                                           )
                logger.info('Done pick one random record from the records that need to be saved')
                # #write the remaining records in the error details df##
                logger.info('Started creating error_union_df')
                error_union_df = error_union_df.alias('a'
                                                      ).join(save_single_record.alias('b'),
                                                             col('a.row_id') == col('b.row_id'),
                                                             'left_outer').filter(col('b.row_id'
                                                                                      ).isNull()).select('a.*')

                # error_union_df.createOrReplaceTempView('error_union_df_final')
                logger.info('Done creating error_union_df')

            # revised_error_count = \
            #     error_union_df.groupby(CommonConstants.PARTITION_FILE_ID,
            #         CommonConstants.QC_ID).count().withColumnRenamed('count'
            #         , CommonConstants.ERROR_COUNT)
            # revised_error_count.createOrReplaceTempView('revised_error_count'
            #         )
            # summary_union_df.createOrReplaceTempView('summary_union_df')
            # summary_union_df_unique = spark_context.sql("""select a."""
            #         + CommonConstants.QC_ID
            #         + """,
            #                         case when b.error_count is null then 0 else b.error_count end as error_count,
            #                         a.batch_id,a.dataset_id,a."""
            #         + CommonConstants.PARTITION_FILE_ID
            #         + """,a.qc_type, a.qc_param,
            #                         concat(cast(case when b.error_count is null then 0 else b.error_count end as string) ,' Records Out Of Total Records ',a.total_count,' Failed') as qc_message,
            #                         a.criticality, a.qc_failure_percentage, a.insert_by, a.insert_date, a.app_id
            #                         from summary_union_df a
            #                         inner join
            #                         revised_error_count b
            #                         on a."""
            #         + CommonConstants.QC_ID + """ = b."""
            #         + CommonConstants.QC_ID + """ and a."""
            #         + CommonConstants.PARTITION_FILE_ID + """ = b."""
            #         + CommonConstants.PARTITION_FILE_ID)
            # summary_union_df_unique.createOrReplaceTempView('summary_union_df_unique'
            #         )
            # summary_union_df_final = \
            #     spark_context.sql("""
            # select a."""
            #                       + CommonConstants.QC_ID
            #                       + """,
            # case when criticality = 'C' and trim(lower(qc_type)) = 'unique' then 0 else a.error_count end as error_count,
            # a.batch_id,a.dataset_id,a."""
            #                       + CommonConstants.PARTITION_FILE_ID
            #                       + """,a.qc_type, a.qc_param,
            # concat(cast(case when criticality = 'C' and trim(lower(qc_type)) = 'unique' then 0 else a.error_count end as string) ,' Records Out Of Total Records ',a.total_count,' Failed') as qc_message,
            # a.criticality, a.qc_failure_percentage, a.insert_by, a.insert_date, a.app_id
            # from
            # summary_union_df a
            # left outer join
            # revised_error_count b
            # on a."""
            #                       + CommonConstants.QC_ID + """ = b."""
            #                       + CommonConstants.QC_ID
            #                       + """ and a."""
            #                       + CommonConstants.PARTITION_FILE_ID
            #                       + """ = b."""
            #                       + CommonConstants.PARTITION_FILE_ID
            #                       + """
            # where b.qc_id is null
            # """)
            # summary_union_df_final.createOrReplaceTempView('summary_union_df_final'
            #         )

            logger.info('Started pushing data to S3')
            configuration = \
                JsonConfigUtility(CommonConstants.ENVIRONMENT_CONFIG_FILE)
            dqm_s3_error_location = \
                str(configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                     'dqm_s3_error_location']))
            dqm_s3_summary_location = \
                str(configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,
                                                     'dqm_s3_summary_location']))

            error_union_df.coalesce(1).write.mode('overwrite').format('parquet'
                                                                      ).partitionBy(
                CommonConstants.PARTITION_FILE_ID).save(dqm_s3_error_location
                                                        + '/' + CommonConstants.PARTITION_BATCH_ID + '='
                                                        + str(batch_id))

            # summary_union_df.write.mode('overwrite'
            #         ).format('parquet'
            #                  ).partitionBy(CommonConstants.PARTITION_FILE_ID).save(dqm_s3_summary_location
            #         + '/' + CommonConstants.PARTITION_BATCH_ID + '='
            #         + str(batch_id))

            logger.info('Done pushing data to S3')

            # update_dqm_logs = \
            #     spark_context.sql("""
            # select * from
            # summary_union_df_final
            # where criticality = 'C' and trim(lower(qc_type)) = 'unique'
            # """)
            # update_dqm_logs.createOrReplaceTempView('update_dqm_logs')
            # error_summary_rows = update_dqm_logs.collect()
            # update_dqm_logs.unpersist()

            # file_id_list_processed_unique = []
            # qc_id_list = []

            # for row in error_summary_rows:
            #     log_info['file_id'] = \
            #         row.asDict()[CommonConstants.PARTITION_FILE_ID]
            #     file_id_list_processed_unique.append(log_info['file_id'
            #             ])
            #     log_info['qc_id'] = row.asDict()['qc_id']
            #     qc_id_list.append(log_info['qc_id'])
            #     log_info['error_count'] = row.asDict()['error_count']
            #     log_info['qc_message'] = row.asDict()['qc_message']
            #     log_info['qc_failure_percentage'] = \
            #         row.asDict()['qc_failure_percentage']

            #     # Here we will mark qc_status as "Succeded"

            #     self.qc_status = CommonConstants.STATUS_SUCCEEDED
            #     qc_end_time = \
            #         datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip()
            #     log_info['qc_status'] = self.qc_status
            #     log_info['qc_end_time'] = qc_end_time
            #     CommonUtils().update_dqm_status(log_info)

            # # #Changes to handle logging for file_ids which were left in IN PROGRESS State

            # file_id_result = \
            #     CommonUtils().get_file_ids_by_batch_id(CommonConstants.AUDIT_DB_NAME,
            #         batch_id)
            # file_id_list_total = []
            # for i in range(0, len(file_id_result)):
            #     file_id = file_id_result[i]['file_id']
            #     file_id_list_total.append(file_id)

            # file_ids_not_processed_unique = \
            #     list(set(file_id_list_total)
            #          - set(file_id_list_processed)
            #          - set(file_id_list_processed_unique))
            # logger.info('Unprocessed File ID List For Unique Check:'
            #             + str(file_ids_not_processed_unique))
            # for file_id in file_ids_not_processed_unique:
            #     log_info['file_id'] = file_id
            #     for qc_id in qc_id_list:
            #         log_info['qc_id'] = qc_id
            #         log_info['error_count'] = '0'
            #         total_count = total_count_dict.get(file_id)
            #         qc_message = '0' + ' Records Out Of Total Records ' \
            #             + str(total_count) + ' Failed'
            #         log_info['qc_message'] = qc_message
            #         log_info['qc_failure_percentage'] = '0'
            #         self.qc_status = CommonConstants.STATUS_SUCCEEDED
            #         qc_end_time = \
            #             datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip()
            #         log_info['qc_status'] = self.qc_status
            #         log_info['qc_end_time'] = qc_end_time
            #         CommonUtils().update_dqm_status(log_info)

            logger.info('Done updating DQM error counts')
            status_message = 'DQM Errors And Summary Created '
            self.execution_context.set_context({'function_status': 'COMPLETED'})
            logger.info(status_message,
                        extra=self.execution_context.get_context())

        except Exception as exception:

            self.excptn_in_summary_check_flag = True
            self.qc_status = CommonConstants.STATUS_FAILED
            qc_end_time = \
                datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip()
            log_info['qc_status'] = self.qc_status
            log_info['qc_end_time'] = qc_end_time
            CommonUtils().update_dqm_status(log_info)
            error = 'ERROR in ' \
                    + self.execution_context.get_context_param('current_module'
                                                               ) + ' ERROR MESSAGE: ' + str(traceback.format_exc()
                                                                                            + str(exception))
            self.execution_context.set_context({'function_status': 'FAILED'
                                                   , 'traceback': error})
            logger.error(status_message,
                         extra=self.execution_context.get_context())
            raise exception

    # ########################################### perform_dqm_check ####################################################
    # Purpose            :   Perform DQM checks on the input file
    # Input              :   file path,dqm checks,file id,bathc id,spark context
    # Output             :   NA
    #
    #
    #
    # ##################################################################################################################

    def perform_dqm_check(
            self,
            file_complete_path=None,
            dqm_checks=None,
            batch_id=None,
            spark_context=None,
            file_field_delimeter=None,
    ):
        status_message = ''
        log_info = {}
        try:
            status_message = 'Starting function to perform DQM checks'
            self.execution_context.set_context({'function_status': 'STARTED'
                                                })
            logger.info(status_message,
                        extra=self.execution_context.get_context())
            logger.info(status_message,
                        extra=self.execution_context.get_context())
            sc = spark_context.sparkContext
            sc.setCheckpointDir('hdfs:///user/hadoop/')
            file_df = spark_context.read.parquet(file_complete_path)
            file_df.persist()
            temp_checks_df_main = file_df.select('row_id')
            temp_checks_df_main.persist()
            uni_list = []
            self.batch_id = batch_id
            dqm_check_chunks = [dqm_checks[i:i
                                             + CommonConstants.CHUNK_SIZE] for i in
                                range(0, len(dqm_checks),
                                      CommonConstants.CHUNK_SIZE)]
            for chunk in dqm_check_chunks:
                for check in chunk:
                    app_id = check['application_id']
                    self.app_id = app_id
                    dataset_id_fk = \
                        str(check[CommonConstants.DATSET_ID])
                    self.dataset_id = dataset_id_fk
                    batch_id = batch_id
                    qc_id_pk = check[CommonConstants.QC_ID]
                    self.qc_id = qc_id_pk
                    column_name = check[CommonConstants.COLUMN_NAME]
                    qc_type = check[CommonConstants.QC_TYPE]
                    param = check[CommonConstants.QC_PARAM]
                    criticality = \
                        str(check[CommonConstants.CRITICALITY])
                    error_count = 0
                    qc_message = ''
                    qc_failure_percentage = ''
                    qc_status = CommonConstants.STATUS_RUNNING
                    self.qc_status = qc_status
                    qc_start_time = \
                        datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip()
                    create_by = str(check[CommonConstants.CREATE_BY])
                    create_ts = \
                        datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip()
                    filter_column = (check['qc_filter'
                                     ] if check['qc_filter'] is not '' else None)

                    filter_column = \
                        (filter_column.strip() if filter_column
                                                  is not None else None)

                    # Modified to fix logs for unique check

                    if qc_type != CommonConstants.DQM_UNIQUE_CHECK:
                        log_info = {
                            'application_id': app_id,
                            'dataset_id': dataset_id_fk,
                            'batch_id': batch_id,
                            'qc_id': qc_id_pk,
                            'column_name': column_name,
                            'qc_type': qc_type,
                            'qc_param': param,
                            'criticality': criticality,
                            'error_count': error_count,
                            'qc_message': qc_message,
                            'qc_failure_percentage': qc_failure_percentage,
                            'qc_status': qc_status,
                            'qc_start_time': qc_start_time,
                            'qc_create_by': create_by,
                            'qc_create_date': create_ts,
                        }
                        CommonUtils().insert_dqm_status(log_info)

                    status_message = 'Dqm Check Started On Qc ID : ' \
                                     + str(qc_id_pk)
                    self.execution_context.set_context({'function_status': 'STARTED'
                                                        })
                    logger.info(status_message,
                                extra=self.execution_context.get_context())

                    if qc_type == CommonConstants.DQM_NULL_CHECK:
                        if filter_column is not None:
                            temp_file_df = \
                                self.filter_column_function(filter_column,
                                                            qc_id_pk, column_name, file_df,
                                                            spark_context)
                            temp_checks_df = temp_file_df.select('flag'
                                                                 , column_name, 'row_id')
                            temp_file_df.unpersist()

                            temp_checks_df = \
                                temp_checks_df.withColumn(str(qc_id_pk),
                                                          when(col(TEMP_DF_FLAG_COLUMN)
                                                               == TEMP_DF_FLAG_COLUMN_PASS_VALUE,
                                                               when(col(column_name).isNull(),
                                                                    lit(CommonConstants.DQM_FAILED_STATUS)).otherwise(
                                                                   lit(CommonConstants.DQM_PASS_STATUS))).otherwise(
                                                              col(TEMP_DF_FLAG_COLUMN))).drop(col(TEMP_DF_FLAG_COLUMN))
                            temp_checks_df_1 = \
                                temp_checks_df.drop(column_name)
                            temp_checks_df_main = \
                                temp_checks_df_main.join(temp_checks_df_1,
                                                         'row_id')

                            temp_checks_df.unpersist()
                            temp_checks_df_1.unpersist()
                        else:
                            temp_checks_df = \
                                file_df.select(column_name, 'row_id')
                            temp_checks_df = \
                                temp_checks_df.withColumn(str(qc_id_pk),
                                                          when(col(column_name).isNull(),
                                                               lit(CommonConstants.DQM_FAILED_STATUS)).otherwise(
                                                              lit(CommonConstants.DQM_PASS_STATUS)))
                            temp_checks_df_2 = \
                                temp_checks_df.drop(column_name)
                            temp_checks_df_main = \
                                temp_checks_df_main.join(temp_checks_df_2,
                                                         'row_id')
                            temp_checks_df.unpersist()
                    elif qc_type == CommonConstants.DQM_LENGTH_CHECK:

                        if filter_column is not None:
                            temp_file_df = \
                                self.filter_column_function(filter_column,
                                                            qc_id_pk, column_name, file_df,
                                                            spark_context)

                            param_exp = ''.join([i for i in param
                                                 if not i.isdigit()])
                            param_value = ''.join(c for c in param
                                                  if c.isdigit())
                            if param_exp == '>=':
                                temp_checks_df = \
                                    temp_file_df.select('flag',
                                                        column_name, 'row_id')
                                temp_file_df.unpersist()
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              when(col(TEMP_DF_FLAG_COLUMN)
                                                                   == TEMP_DF_FLAG_COLUMN_PASS_VALUE,
                                                                   when(length(col(column_name)).__ge__(param_value),
                                                                        lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                       lit(
                                                                           CommonConstants.DQM_FAILED_STATUS))).otherwise(
                                                                  col(TEMP_DF_FLAG_COLUMN))).drop(
                                        col(TEMP_DF_FLAG_COLUMN))
                                temp_checks_df_1 = \
                                    temp_checks_df.drop(column_name)
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df_1,
                                                             'row_id')

                                temp_checks_df.unpersist()
                            elif param_exp == '<=':
                                temp_checks_df = \
                                    temp_file_df.select('flag',
                                                        column_name, 'row_id')
                                temp_file_df.unpersist()
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              when(col(TEMP_DF_FLAG_COLUMN)
                                                                   == TEMP_DF_FLAG_COLUMN_PASS_VALUE,
                                                                   when(length(col(column_name)).__le__(param_value),
                                                                        lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                       lit(
                                                                           CommonConstants.DQM_FAILED_STATUS))).otherwise(
                                                                  col(TEMP_DF_FLAG_COLUMN))).drop(
                                        col(TEMP_DF_FLAG_COLUMN))
                                temp_checks_df_1 = \
                                    temp_checks_df.drop(column_name)
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df_1,
                                                             'row_id')

                                temp_checks_df.unpersist()
                            elif param_exp == '>':
                                temp_checks_df = \
                                    temp_file_df.select('flag',
                                                        column_name, 'row_id')
                                temp_file_df.unpersist()
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              when(col(TEMP_DF_FLAG_COLUMN)
                                                                   == TEMP_DF_FLAG_COLUMN_PASS_VALUE,
                                                                   when(length(col(column_name)).__gt__(param_value),
                                                                        lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                       lit(
                                                                           CommonConstants.DQM_FAILED_STATUS))).otherwise(
                                                                  col(TEMP_DF_FLAG_COLUMN))).drop(
                                        col(TEMP_DF_FLAG_COLUMN))
                                temp_checks_df_1 = \
                                    temp_checks_df.drop(column_name)
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df_1,
                                                             'row_id')

                                temp_checks_df.unpersist()
                            elif param_exp == '<':
                                temp_checks_df = \
                                    temp_file_df.select('flag',
                                                        column_name, 'row_id')
                                temp_file_df.unpersist()
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              when(col(TEMP_DF_FLAG_COLUMN)
                                                                   == TEMP_DF_FLAG_COLUMN_PASS_VALUE,
                                                                   when(length(col(column_name)).__lt__(param_value),
                                                                        lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                       lit(
                                                                           CommonConstants.DQM_FAILED_STATUS))).otherwise(
                                                                  col(TEMP_DF_FLAG_COLUMN))).drop(
                                        col(TEMP_DF_FLAG_COLUMN))
                                temp_checks_df_1 = \
                                    temp_checks_df.drop(column_name)
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df_1,
                                                             'row_id')

                                temp_checks_df.unpersist()
                            elif param_exp == '=':
                                temp_checks_df = \
                                    temp_file_df.select('flag',
                                                        column_name, 'row_id')
                                temp_file_df.unpersist()
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              when(col(TEMP_DF_FLAG_COLUMN)
                                                                   == TEMP_DF_FLAG_COLUMN_PASS_VALUE,
                                                                   when(length(col(column_name)).__eq__(param_value),
                                                                        lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                       lit(
                                                                           CommonConstants.DQM_FAILED_STATUS))).otherwise(
                                                                  col(TEMP_DF_FLAG_COLUMN))).drop(
                                        col(TEMP_DF_FLAG_COLUMN))
                                temp_checks_df_1 = \
                                    temp_checks_df.drop(column_name)
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df_1,
                                                             'row_id')

                                temp_checks_df.unpersist()
                            elif param_exp == '!':
                                temp_checks_df = \
                                    temp_file_df.select('flag',
                                                        column_name, 'row_id')
                                temp_file_df.unpersist()
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              when(col(TEMP_DF_FLAG_COLUMN)
                                                                   == TEMP_DF_FLAG_COLUMN_PASS_VALUE,
                                                                   when(length(col(column_name)).__ne__(param_value),
                                                                        lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                       lit(
                                                                           CommonConstants.DQM_FAILED_STATUS))).otherwise(
                                                                  col(TEMP_DF_FLAG_COLUMN))).drop(
                                        col(TEMP_DF_FLAG_COLUMN))
                                temp_checks_df_1 = \
                                    temp_checks_df.drop(column_name)
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df_1,
                                                             'row_id')

                                temp_checks_df.unpersist()
                            else:
                                temp_checks_df = \
                                    temp_file_df.select('flag', 'row_id'
                                                        )
                                temp_file_df.unpersist()
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              when(col(TEMP_DF_FLAG_COLUMN)
                                                                   == TEMP_DF_FLAG_COLUMN_PASS_VALUE,
                                                                   lit(CommonConstants.DQM_FAILED_STATUS)).otherwise(
                                                                  col(TEMP_DF_FLAG_COLUMN))).drop(
                                        col(TEMP_DF_FLAG_COLUMN))
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df,
                                                             'row_id')

                                temp_checks_df.unpersist()
                        else:
                            param_exp = ''.join([i for i in param
                                                 if not i.isdigit()])
                            param_value = ''.join(c for c in param
                                                  if c.isdigit())
                            if param_exp == '>=':
                                temp_checks_df = \
                                    file_df.select(column_name, 'row_id'
                                                   )
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              when(length(col(column_name)).__ge__(param_value),
                                                                   lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                  lit(CommonConstants.DQM_FAILED_STATUS)))
                                temp_checks_df_1 = \
                                    temp_checks_df.drop(column_name)
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df_1,
                                                             'row_id')
                                temp_checks_df.unpersist()
                            elif param_exp == '<=':
                                temp_checks_df = \
                                    file_df.select(column_name, 'row_id'
                                                   )
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              when(length(col(column_name)).__le__(param_value),
                                                                   lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                  lit(CommonConstants.DQM_FAILED_STATUS)))
                                temp_checks_df_1 = \
                                    temp_checks_df.drop(column_name)
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df_1,
                                                             'row_id')
                                temp_checks_df.unpersist()
                            elif param_exp == '>':
                                temp_checks_df = \
                                    file_df.select(column_name, 'row_id'
                                                   )
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              when(length(col(column_name)).__gt__(param_value),
                                                                   lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                  lit(CommonConstants.DQM_FAILED_STATUS)))
                                temp_checks_df_1 = \
                                    temp_checks_df.drop(column_name)
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df_1,
                                                             'row_id')
                                temp_checks_df.unpersist()
                            elif param_exp == '<':
                                temp_checks_df = \
                                    file_df.select(column_name, 'row_id'
                                                   )
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              when(length(col(column_name)).__lt__(param_value),
                                                                   lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                  lit(CommonConstants.DQM_FAILED_STATUS)))
                                temp_checks_df_1 = \
                                    temp_checks_df.drop(column_name)
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df_1,
                                                             'row_id')
                                temp_checks_df.unpersist()
                            elif param_exp == '=':
                                temp_checks_df = \
                                    file_df.select(column_name, 'row_id'
                                                   )
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              when(length(col(column_name)).__eq__(param_value),
                                                                   lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                  lit(CommonConstants.DQM_FAILED_STATUS)))
                                temp_checks_df_1 = \
                                    temp_checks_df.drop(column_name)
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df_1,
                                                             'row_id')
                                temp_checks_df.unpersist()
                            elif param_exp == '!':
                                temp_checks_df = \
                                    file_df.select(column_name, 'row_id'
                                                   )
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              when(length(col(column_name)).__ne__(param_value),
                                                                   lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                  lit(CommonConstants.DQM_FAILED_STATUS)))
                                temp_checks_df_1 = \
                                    temp_checks_df.drop(column_name)
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df_1,
                                                             'row_id')
                                temp_checks_df.unpersist()
                            else:
                                temp_checks_df = file_df.select('row_id'
                                                                )
                                temp_checks_df = \
                                    temp_checks_df.withColumn(str(qc_id_pk),
                                                              lit(CommonConstants.DQM_FAILED_STATUS))
                                temp_checks_df_main = \
                                    temp_checks_df_main.join(temp_checks_df,
                                                             'row_id')
                    elif qc_type == CommonConstants.DQM_DATE_CHECK:

                        if filter_column is not None:
                            temp_file_df = \
                                self.filter_column_function(filter_column,
                                                            qc_id_pk, column_name, file_df,
                                                            spark_context)
                            temp_checks_df = temp_file_df.select('flag'
                                                                 , column_name, 'row_id')
                            temp_file_df.unpersist()
                            temp_checks_df = \
                                temp_checks_df.withColumn(str(qc_id_pk),
                                                          when(col(TEMP_DF_FLAG_COLUMN)
                                                               == TEMP_DF_FLAG_COLUMN_PASS_VALUE,
                                                               when(col(column_name).rlike(self.get_date_regex(param)),
                                                                    lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                   lit(CommonConstants.DQM_FAILED_STATUS))).otherwise(
                                                              col(TEMP_DF_FLAG_COLUMN))).drop(col(TEMP_DF_FLAG_COLUMN))
                            temp_checks_df_1 = \
                                temp_checks_df.drop(column_name)
                            temp_checks_df_main = \
                                temp_checks_df_main.join(temp_checks_df_1,
                                                         'row_id')
                            temp_checks_df.unpersist()
                        else:

                            temp_checks_df = \
                                file_df.select(column_name, 'row_id')
                            temp_checks_df = \
                                temp_checks_df.withColumn(str(qc_id_pk),
                                                          when(col(column_name).rlike(self.get_date_regex(param)),
                                                               lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                              lit(CommonConstants.DQM_FAILED_STATUS)))
                            temp_checks_df_1 = \
                                temp_checks_df.drop(column_name)
                            temp_checks_df_main = \
                                temp_checks_df_main.join(temp_checks_df_1,
                                                         'row_id')
                            temp_checks_df.unpersist()
                    elif qc_type == CommonConstants.DQM_INTEGER_CHECK:
                        if filter_column is not None:
                            temp_file_df = \
                                self.filter_column_function(filter_column,
                                                            qc_id_pk, column_name, file_df,
                                                            spark_context)

                            regex = CommonConstants.INTEGER_REGEX

                            temp_checks_df = temp_file_df.select('flag'
                                                                 , column_name, 'row_id')
                            temp_file_df.unpersist()
                            temp_checks_df = \
                                temp_checks_df.withColumn(str(qc_id_pk),
                                                          when(col(TEMP_DF_FLAG_COLUMN)
                                                               == TEMP_DF_FLAG_COLUMN_PASS_VALUE,
                                                               when(col(column_name).rlike(regex),
                                                                    lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                   lit(CommonConstants.DQM_FAILED_STATUS))).otherwise(
                                                              col(TEMP_DF_FLAG_COLUMN))).drop(col(TEMP_DF_FLAG_COLUMN))
                            temp_checks_df_1 = \
                                temp_checks_df.drop(column_name)
                            temp_checks_df_main = \
                                temp_checks_df_main.join(temp_checks_df_1,
                                                         'row_id')
                            temp_checks_df.unpersist()
                        else:

                            regex = CommonConstants.INTEGER_REGEX
                            temp_checks_df = \
                                file_df.select(column_name, 'row_id')
                            temp_checks_df = \
                                temp_checks_df.withColumn(str(qc_id_pk),
                                                          when(col(column_name).rlike(regex),
                                                               lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                              lit(CommonConstants.DQM_FAILED_STATUS)))
                            temp_checks_df_2 = \
                                temp_checks_df.drop(column_name)
                            temp_checks_df_main = \
                                temp_checks_df_main.join(temp_checks_df_2,
                                                         'row_id')
                            temp_checks_df.unpersist()
                    elif qc_type == CommonConstants.DQM_DECIMAL_CHECK:
                        if filter_column is not None:
                            temp_file_df = \
                                self.filter_column_function(filter_column,
                                                            qc_id_pk, column_name, file_df,
                                                            spark_context)

                            regex = CommonConstants.DECIMAL_REGEX
                            temp_checks_df = temp_file_df.select('flag'
                                                                 , column_name, 'row_id')
                            temp_file_df.unpersist()
                            temp_checks_df = \
                                temp_checks_df.withColumn(str(qc_id_pk),
                                                          when(col(TEMP_DF_FLAG_COLUMN)
                                                               == TEMP_DF_FLAG_COLUMN_PASS_VALUE,
                                                               when(col(column_name).rlike(regex),
                                                                    lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                   lit(CommonConstants.DQM_FAILED_STATUS))).otherwise(
                                                              col(TEMP_DF_FLAG_COLUMN))).drop(col(TEMP_DF_FLAG_COLUMN))
                            temp_checks_df_1 = \
                                temp_checks_df.drop(column_name)
                            temp_checks_df_main = \
                                temp_checks_df_main.join(temp_checks_df_1,
                                                         'row_id')
                            temp_checks_df.unpersist()
                        else:

                            regex = CommonConstants.DECIMAL_REGEX
                            temp_checks_df = \
                                file_df.select(column_name, 'row_id')
                            temp_checks_df = \
                                temp_checks_df.withColumn(str(qc_id_pk),
                                                          when(col(column_name).rlike(regex),
                                                               lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                              lit(CommonConstants.DQM_FAILED_STATUS)))
                            temp_checks_df_1 = \
                                temp_checks_df.drop(column_name)
                            temp_checks_df_main = \
                                temp_checks_df_main.join(temp_checks_df_1,
                                                         'row_id')
                            temp_checks_df.unpersist()
                    elif qc_type == CommonConstants.DQM_DOMAIN_CHECK:
                        if filter_column is not None:
                            temp_file_df = \
                                self.filter_column_function(filter_column,
                                                            qc_id_pk, column_name, file_df,
                                                            spark_context)
                            param_arr = \
                                re.split(r',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)'
                                         , param)
                            param_arr = [x.replace('"', '') for x in
                                         param_arr]
                            param_arr = [x.strip() for x in param_arr]
                            temp_checks_df = temp_file_df.select('flag'
                                                                 , column_name, 'row_id')
                            temp_file_df.unpersist()
                            temp_checks_df = \
                                temp_checks_df.withColumn(str(qc_id_pk),
                                                          when(col(TEMP_DF_FLAG_COLUMN)
                                                               == TEMP_DF_FLAG_COLUMN_PASS_VALUE,
                                                               when(col(column_name).isin(param_arr),
                                                                    lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                                   lit(CommonConstants.DQM_FAILED_STATUS))).otherwise(
                                                              col(TEMP_DF_FLAG_COLUMN))).drop(col(TEMP_DF_FLAG_COLUMN))
                            temp_checks_df_1 = \
                                temp_checks_df.drop(column_name)
                            temp_checks_df_main = \
                                temp_checks_df_main.join(temp_checks_df_1,
                                                         'row_id')
                            temp_checks_df.unpersist()
                        else:
                            param_arr = \
                                re.split(r',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)'
                                         , param)
                            param_arr = [x.replace('"', '') for x in
                                         param_arr]
                            param_arr = [x.strip() for x in param_arr]
                            temp_checks_df = \
                                file_df.select(column_name, 'row_id')
                            temp_checks_df = \
                                temp_checks_df.withColumn(str(qc_id_pk),
                                                          when(col(column_name).isin(param_arr),
                                                               lit(CommonConstants.DQM_PASS_STATUS)).otherwise(
                                                              lit(CommonConstants.DQM_FAILED_STATUS)))
                            temp_checks_df_1 = \
                                temp_checks_df.drop(column_name)
                            temp_checks_df_main = \
                                temp_checks_df_main.join(temp_checks_df_1,
                                                         'row_id')
                            temp_checks_df.unpersist()
                    elif qc_type == CommonConstants.DQM_UNIQUE_CHECK:
                        uni_list.append(check)
                    elif qc_type == CommonConstants.DQM_CUSTOM_CHECK:

                        temp_table_name = 'table' + '_' + str(qc_id_pk)
                        file_df.createOrReplaceTempView(temp_table_name)
                        temp_checks_df = spark_context.sql(param)
                        temp_checks_df_main = \
                            temp_checks_df_main.join(temp_checks_df,
                                                     'row_id')
                        temp_checks_df.unpersist()
                    else:
                        dqm_checks_list = list(dqm_checks)
                        dqm_checks_list[:] = [l for l in
                                              dqm_checks_list
                                              if l.get(CommonConstants.QC_ID)
                                              != qc_id_pk]
                        dqm_checks = tuple(dqm_checks_list)

                    status_message = 'Dqm Check Finished On Qc Id : ' \
                                     + str(qc_id_pk)
                    self.execution_context.set_context({'function_status': 'STARTED'
                                                        })
                    logger.info(status_message,
                                extra=self.execution_context.get_context())

                temp_checks_df_main.cache()
                file_df.cache()
                file_df.checkpoint()
                self.execution_context.set_context({'function_status': 'COMPLETED'
                                                    })
                logger.info(status_message,
                            extra=self.execution_context.get_context())
            status_message = 'Unique Check Statred'
            self.execution_context.set_context({'function_status': 'COMPLETED'
                                                })
            logger.info(status_message,
                        extra=self.execution_context.get_context())

            # Getting Tuple For Unique Checks

            for uni in tuple(uni_list):
                uni_column = uni[CommonConstants.COLUMN_NAME]
                uni_column_arr = uni_column.split(',', -1)
                uni_qc_id = uni[CommonConstants.QC_ID]
                app_id = uni['application_id']
                self.app_id = app_id
                dataset_id_fk = str(uni[CommonConstants.DATSET_ID])
                self.dataset_id = dataset_id_fk
                self.qc_id = uni_qc_id
                qc_type = uni[CommonConstants.QC_TYPE]
                param = uni[CommonConstants.QC_PARAM]
                criticality = str(uni[CommonConstants.CRITICALITY])
                qc_status = CommonConstants.STATUS_RUNNING
                self.qc_status = qc_status
                qc_start_time = \
                    datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip()
                create_by = str(uni[CommonConstants.CREATE_BY])
                create_ts = \
                    datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip()
                log_info = {
                    'application_id': app_id,
                    'dataset_id': dataset_id_fk,
                    'batch_id': batch_id,
                    'qc_id': uni_qc_id,
                    'column_name': uni_column,
                    'qc_type': qc_type,
                    'qc_param': param,
                    'criticality': criticality,
                    'error_count': 0,
                    'qc_message': '',
                    'qc_failure_percentage': '',
                    'qc_status': qc_status,
                    'qc_start_time': qc_start_time,
                    'qc_create_by': create_by,
                    'qc_create_date': create_ts,
                }
                CommonUtils().insert_dqm_status(log_info)
                window = Window.partitionBy(uni_column_arr)
                file_df = file_df.select('*', count('*'
                                                    ).over(window).alias('_count'
                                                                         )).withColumn(str(uni_qc_id), when(col('_count'
                                                                                                                ) > 1,
                                                                                                            lit(
                                                                                                                CommonConstants.DQM_FAILED_STATUS)).otherwise(
                    lit(CommonConstants.DQM_PASS_STATUS))).drop('_count'
                                                                )
            self.execution_context.set_context({'function_status': 'COMPLETED'
                                                })
            logger.info(status_message,
                        extra=self.execution_context.get_context())

            status_message = \
                'Completed DQM check , Now Saving Summary And Details'
            self.execution_context.set_context({'function_status': 'COMPLETED'
                                                })
            logger.info(status_message,
                        extra=self.execution_context.get_context())

            # Saving DQM Checks To Error And Summary

            file_df = file_df.join(temp_checks_df_main, 'row_id')
            temp_checks_df_main.unpersist()
            file_df.checkpoint()
            self.save_dqm_error_summary(
                log_info,
                dqm_checks,
                uni_list,
                file_df,
                batch_id,
                spark_context,
                file_field_delimeter,
            )
            status_message = 'Completed DQM Utility For Batch Id : ' \
                             + batch_id
            self.execution_context.set_context({'function_status': 'COMPLETED'
                                                })
            logger.info(status_message,
                        extra=self.execution_context.get_context())
            return True
        except Exception as exception:
            error = 'ERROR in ' \
                    + self.execution_context.get_context_param('current_module'
                                                               ) + ' ERROR MESSAGE: ' + str(traceback.format_exc()
                                                                                            + str(exception))
            self.execution_context.set_context({'function_status': 'FAILED'
                                                   , 'traceback': error})
            logger.error(status_message,
                         extra=self.execution_context.get_context())

            if not self.excptn_in_summary_check_flag:
                self.qc_status = CommonConstants.STATUS_FAILED
                qc_end_time = \
                    datetime.now().strftime(CommonConstants.DATE_TIME_FORMAT).strip()
                log_info['qc_status'] = self.qc_status
                log_info['qc_end_time'] = qc_end_time
                CommonUtils().update_dqm_status(log_info)

            raise exception
        finally:
            status_message = 'In Finally Block Of DQM Check Utility'
            logger.debug(status_message,
                         extra=self.execution_context.get_context())

    def get_date_regex(self, qc_param=None):
        if qc_param == CommonConstants.DATE_TIME_FORAMT_10:
            regex = CommonConstants.DATE_TIME_FORAMT_10_REGEX
        elif qc_param == CommonConstants.DATE_TIME_FORAMT_1:
            regex = CommonConstants.DATE_TIME_FORAMT_1_REGEX
        elif qc_param == CommonConstants.DATE_TIME_FORAMT_2:
            regex = CommonConstants.DATE_TIME_FORAMT_2_REGEX
        elif qc_param == CommonConstants.DATE_TIME_FORAMT_3:
            regex = CommonConstants.DATE_TIME_FORAMT_3_REGEX
        elif qc_param == CommonConstants.DATE_TIME_FORAMT_4:
            regex = CommonConstants.DATE_TIME_FORAMT_4_REGEX
        elif qc_param == CommonConstants.DATE_TIME_FORAMT_5:
            regex = CommonConstants.DATE_TIME_FORAMT_5_REGEX
        elif qc_param == CommonConstants.DATE_TIME_FORAMT_6:
            regex = CommonConstants.DATE_TIME_FORAMT_6_REGEX
        elif qc_param == CommonConstants.DATE_TIME_FORAMT_7:
            regex = CommonConstants.DATE_TIME_FORAMT_7_REGEX
        elif qc_param == CommonConstants.DATE_TIME_FORAMT_8:
            regex = CommonConstants.DATE_TIME_FORAMT_8_REGEX
        elif qc_param == CommonConstants.DATE_TIME_FORAMT_9:
            regex = CommonConstants.DATE_TIME_FORAMT_9_REGEX
        else:
            regex = CommonConstants.DATE_TIME_FORAMT_DEFAULT_REGEX
        return regex

    def filter_column_function(
            self,
            filter_column=None,
            qc_id_pk=None,
            column_name=None,
            file_df=None,
            spark_context=None,
    ):
        temp_table_name = 'table' + '_' + str(qc_id_pk)
        query = \
            'Select *,case when {} then {} else "" end as {} from {}'.format(filter_column,
                                                                             TEMP_DF_FLAG_COLUMN_PASS_VALUE,
                                                                             TEMP_DF_FLAG_COLUMN,
                                                                             temp_table_name)
        file_df.createOrReplaceTempView(temp_table_name)
        filtered_df = spark_context.sql(query)
        return filtered_df
