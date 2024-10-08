import pandas as pd
import sys
import json
import os
import numpy as np

class adjusted_calc(object):

    visit_table = '/clinical_design_center/data_management/cdl_sdo/qa/code/visit_table.csv'
    if os.path.exists(visit_table):
        os.system('rm ' + visit_table)
    os.system('rm -r /clinical_design_center/data_management/cdl_sdo/qa/code/visit_table')
    os.system('hadoop fs -copyToLocal /user/hive/warehouse/visit_table '
          '/clinical_design_center/data_management/cdl_sdo/qa/code/')

    os.system('mv /clinical_design_center/data_management/cdl_sdo/qa/code/visit_table/*.csv '
          '/clinical_design_center/data_management/cdl_sdo/qa/code/visit_table.csv')

    df = pd.read_csv("visit_table.csv")

    df2=df.sort_values(['trial_id','arm_id','procedure_id','row_num'],ascending=[True, True,True, True])

    df2.to_csv("adjusted_burden_temp.csv", index=False)

    df =pd.read_csv("adjusted_burden_temp.csv")

    for i in range(len(df)):

    #print(df.loc[i, 'row_num'])
    #print(df.loc[i, 'visit_label'])
        if df.loc[i, 'row_num'] != 1:

            #print(np.exp(-0.912*(df.loc[i, 'day_visit']-df.loc[i, 'prev_day_visit'])))
            #print(df.loc[(i-1),'adjusted_site_burden']*np.exp(-0.912*(df.loc[i, 'day_visit']-df.loc[i, 'prev_day_visit'])))
            #print(df.loc[i, 'linear_site_burden']+df.loc[(i-1),'adjusted_site_burden']*np.exp(-0.912*(df.loc[i, 'day_visit']-df.loc[i, 'prev_day_visit'])))
            df.loc[i, 'adjusted_site_burden'] = df.loc[i, 'linear_site_burden']+df.loc[(i-1),
                                                                                   'adjusted_site_burden']*np.exp(-0.912*(df.loc[i, 'day_visit']-df.loc[i, 'prev_day_visit']))
            df.loc[i, 'adjusted_patient_burden'] = df.loc[i, 'linear_patient_burden'] + df.loc[(i - 1),
                                                                                           'adjusted_patient_burden'] * np.exp(
            -0.912 * (df.loc[i, 'day_visit'] - df.loc[i, 'prev_day_visit']))

    df.to_csv("adjusted_burden.csv", index=False)

# as a next step copy this csv to a s3 location

    os.system('aws s3 cp adjusted_burden.csv '
          's3://aws-a0036-use1-00-d-s3b-rwcb-chb-data01/clinical-data-lake/uploads/SDO_UPLOADS'
          '/Burden_Score/')

if __name__ == '__main__':

    adj = adjusted_calc()
    
