# optimized code from 1hour to 5-10 minutes
# install gensim library on emr
import pandas as pd
import re
import os
import sys
import json
import time
import logging
from datetime import date, datetime
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

import numpy as np
from sparse_dot_topn import awesome_cossim_topn

from ftfy import fix_text
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer

import warnings
warnings.filterwarnings('ignore')

from gensim.parsing.preprocessing import remove_stopwords, STOPWORDS
from nltk import word_tokenize
import string
import re


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        #logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

initial_time = time.time()


import nltk
nltk.download('punkt')

#all_stopwords=STOPWORDS
## to avoid certain drugs that contain stopwords from being dropped
remove_list={"a", "etc", "in", "de", "is", "he", "my", "i", "same", "via", "me", "for", "get", "at", "on", "it", "km", "as", "co", "am"}
all_stopwords= STOPWORDS.difference(remove_list)


class DrugStandardization(object):

    # add parsers to match decimals not added yet
    def remove_pattern(self, s):

        """
        cleaning drug data based certain pattern matches
        """
        return re.sub('\s+', ' ', re.sub(r'multiple|single|matching|\d+ mg/kg|\d+ ug|administered|\d+mg|daily|week|weeks|day|days|dose|\d+ mg/ml|\d+mcg/kg|\d+ mg/day|\d+mcg|\d+ mcg/kg|\d+ mg|\d+ g|\d+ mg/day/day|\d+ gray/day|\d+ mg/m^2|\d+ mg/ml|\d+ mg/kg|\d+ mtd|\d+ ml/kg|\d+ ml|\d+ week[s]|\d+ hour[s]', ' ', s, flags=re.IGNORECASE)).strip()

    def clean_drug_name(self, s):

        """
        function to return values that do not contain punctuation or stopwords
        """
        #temp_text = remove_stopwords(s)

        tokens=word_tokenize(s)

        filtered = [drug for drug in tokens if drug not in all_stopwords and drug not in string.punctuation and bool(re.search('^[0-9]*$', drug))==False]

        return ' '.join(filtered).strip()


    def process_ctln_data(self, df):

        """
        returns processed citeline dataframe
        """

        #print("Before processing:")
        #print("Number of rows in citeline data: {}\nNumber of columns in citeline data: {}".format(df.shape[0], df.shape[1]))

        # To have unique standard drug and synonym name pairs
        df.drop_duplicates(subset=['drugprimaryname', 'drugnamesynonyms'], inplace=True)

        # No NaN values in standard drug name column
        num_na = df['drugprimaryname'].isna().sum()

        logging.info("Number of NaN values: %d"% num_na)
        if num_na > 0:
            df.dropna(subset=['drugprimaryname'], inplace=True)

        # To avoid errors related to NaN while executing algorithm
        df.dropna(subset=['drugnamesynonyms'], inplace=True)

        ### code to find duplicates in drug primary name ###
        df['drugprimaryname_processed'] = df['drugprimaryname'].apply(lambda x: re.sub('[^\w]+', '', x).lower().strip() )
        df['drugnamesynonyms_processed']=df['drugnamesynonyms'].apply(lambda x: re.sub('[^\w]+','', x ).lower().strip() )

        # To have unique standard drug and synonym name pairs
        df.drop_duplicates(subset=['drugprimaryname_processed', 'drugnamesynonyms_processed'], inplace=True)

        df['drugprimaryname_processed'] = df['drugprimaryname_processed'].apply(self.remove_pattern)
        df['drugprimaryname_processed'] = df['drugprimaryname_processed'].apply(self.clean_drug_name)

        df['drugnamesynonyms_processed'] = df['drugnamesynonyms_processed'].apply(self.remove_pattern)
        df['drugnamesynonyms_processed'] = df['drugnamesynonyms_processed'].apply(self.clean_drug_name)


        df['drugnamesynonyms']=df['drugnamesynonyms'].apply(lambda x : x.strip())
        df['drugprimaryname']=df['drugprimaryname'].apply(lambda x : x.strip() )

        # this id is for left joining ctln df to df after combining drugs.
        # see function merge_combination_to_citeline
        df.insert(0, "temp_id", range(0, 0+len(df)))

        df.rename(columns={'trialids' : 'ctln_trialids'}, inplace=True)

        #print("After processing:")
        #print("Number of rows in citeline data: {}\nNumber of columns in citeline data: {}".format(df.shape[0], df.shape[1]))

        df.fillna('', inplace=True)

        return df


    def process_aact_data(self, df):

        """
        returns processed AACT dataframe
        """

        #print("Before processing:")
        #print("Number of rows in AACT data: {}\nNumber of columns in AACT data: {}".format(df.shape[0], df.shape[1]))

        df['name'] = df['name'].apply(lambda x: x.strip())

        df['processed']=df['name'].apply(lambda x: re.sub('[^\w]+',' ', x ).lower().strip() )

        df.drop_duplicates(subset=['processed'], keep='first', inplace=True)


        df['processed'] = df['processed'].apply(self.remove_pattern)
        df['processed'] = df['processed'].apply(self.clean_drug_name)

        df.rename(columns={'name' : 'aact_drug_name', 'nct_id' : 'aact_trialids'}, inplace=True)

        df.insert(0, 'ID', range(0, 0+len(df)))

        #print("After processing:")
        #print("Number of rows in AACT data: {}\nNumber of columns in AACT data: {}".format(df.shape[0], df.shape[1]))

        df.fillna('', inplace=True)

        return df


    def process_dqs_data(self, df):

        """
        returns processed DQS dataframe
        """

        df.dropna(subset=['drug_name'], inplace=True)

        #print("Before processing:")
        #print("Number of rows in DQS data: {}\nNumber of columns in DQS data: {}".format(df.shape[0], df.shape[1]))

        df['drug_name'] = df['drug_name'].apply(lambda x: x.strip())

        df['processed']=df['drug_name'].apply(lambda x: re.sub('[^\w]+',' ', x ).lower().strip() )

        df.drop_duplicates(subset=['processed'], inplace=True)

        df['processed'] = df['processed'].apply(self.remove_pattern)
        df['processed'] = df['processed'].apply(self.clean_drug_name)


        df.drop(columns=['source_study_ids', 'hash_uid', 'datasource'], inplace=True)
        df.rename(columns={'nct_ids' : 'dqs_trialids', 'drug_name' : 'dqs_drug_name'}, inplace=True)

        df.insert(0, 'ID', range(0, 0+len(df)))

        #print("After processing:")
        #print("Number of rows in DQS data: {}\nNumber of columns in DQS data: {}".format(df.shape[0], df.shape[1]))

        df.fillna('', inplace=True)

        return df


    def combine_primary_syn(self, df):

        """
        Combining ctln_df2 primary name and synonyms into a single column for mapping
        """

        ctln_df2_one = pd.concat([df['drugprimaryname_processed'], df['drugnamesynonyms_processed']], axis=0)

        ctln_df2_one_df = pd.DataFrame(ctln_df2_one, columns=['processed'])

        logging.info("Number of rows after combining data: {}".format(ctln_df2_one_df.shape[0]))

        return ctln_df2_one_df


    def merge_combination_to_citeline(self, df, ctln_df, dqs_flag, dqs_df=None):
        """
        merge input that are combination of multiple drugs to citeline drugs data

        df: aact dataframe
        dqs_df: dqs dataframe
        ctln_df: citeline dataframe
        """

        logging.info("Finding drug combinations in AACT data..")

        keywords_df_aact = df[( (df['aact_drug_name'].str.contains('&')) |
                   (df['aact_drug_name'].str.contains('and')) |
                   (df['aact_drug_name'].str.contains('And')) |
                   (df['aact_drug_name'].str.contains('AND')) |
                   (df['aact_drug_name'].str.contains('\+')) |
                   (df['aact_drug_name'].str.contains('combination')) |
                   (df['aact_drug_name'].str.contains('Combination')) )
                   ]

        comma_df_aact = df[df.apply(lambda x: x['aact_drug_name'].count(',')>=2  , axis=1)]

        logging.info("Combination of drugs found in AACT data: {}".format(len(keywords_df_aact)+len(comma_df_aact)))

        ## this will execute only if dqs flag is set means dqs ds is present
        if dqs_flag == 1:

            logging.info("Finding drug combinations in DQS data..")

            keywords_df_dqs = dqs_df[( (dqs_df['dqs_drug_name'].str.contains('&')) |
                   (dqs_df['dqs_drug_name'].str.contains('and')) |
                   (dqs_df['dqs_drug_name'].str.contains('And')) |
                   (dqs_df['dqs_drug_name'].str.contains('AND')) |
                   (dqs_df['dqs_drug_name'].str.contains('\+')) |
                   (dqs_df['dqs_drug_name'].str.contains('combination')) |
                   (dqs_df['dqs_drug_name'].str.contains('Combination')) )
                   ]

            comma_df_dqs = dqs_df[dqs_df.apply(lambda x: x['dqs_drug_name'].count(',')>=2  , axis=1)]

            logging.info("Combination of drugs found in DQS data: {}".format(len(keywords_df_dqs)+len(comma_df_dqs)))

            df_new = pd.DataFrame(ctln_df['drugprimaryname'].append([keywords_df_aact['aact_drug_name'], comma_df_aact['aact_drug_name'], keywords_df_dqs['dqs_drug_name'], comma_df_dqs['dqs_drug_name']], ignore_index=True))
            df_new.insert(0, 'temp_id', range(0, 0+len(df_new)))


        logging.info("merging combined drugs to standard citeline data..")

        df_new = pd.DataFrame(ctln_df['drugprimaryname'].append([keywords_df_aact['aact_drug_name'], comma_df_aact['aact_drug_name']], ignore_index=True))

        ## dropping duplicates if drug combination already exists in primary name
        #df_new.drop_duplicates(subset=['drugprimaryname'], inplace=True)
        df_new.insert(0, 'temp_id', range(0, 0+len(df_new)))

        # merging added primary names with citeline data
        df_merged = pd.merge(df_new, ctln_df, how='left', on=['temp_id'])

        df_merged.drop(columns=['drugprimaryname', 'drugprimaryname_processed', 'drugnamesynonyms_processed'], inplace=True)
        df_merged.rename(columns={0 : 'drugprimaryname'}, inplace=True)

        df_merged['drugnamesynonyms'].fillna('', inplace=True)

        df_merged['drugprimaryname_processed'] = df_merged['drugprimaryname'].apply(lambda x: re.sub('[^\w]+', ' ', x).lower().strip() )
        df_merged['drugnamesynonyms_processed']= df_merged['drugnamesynonyms'].apply(lambda x: re.sub('[^\w]+',' ', x ).lower().strip() )



        # this index is for creating syn to primary index mapping after we combine into one
        df_merged.insert(0, "idx", range(0, 0+len(df_merged)))

        # syn index col is used to map from synonym to its primary name as we combine both below
        df_merged.insert(1, "syn_index", range(len(df_merged), len(df_merged)+len(df_merged['drugnamesynonyms'])))


        return df_merged


    def ngrams(self, string, n=3):
        # returns 3-grams of input disease after some preprocessing on it
        # string : input disease
        string = fix_text(string) # fix text
        string = string.encode("ascii", errors="ignore").decode() #remove non ascii chars
        string = string.lower()
        chars_to_remove = [")","(",".","|","[","]","{","}","'","=","?", "%"]
        rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
        string = re.sub(rx, '', string)
        string = string.replace('&', 'and')
        string = string.replace(',', ' ')
        string = string.replace('-', ' ')
        string = string.title() # normalise case - capital at start of each word
        string = re.sub(' +',' ',string).strip() # get rid of multiple spaces and replace with a single
        string = ' '+ string +' ' # pad names for ngrams...
        ngrams = zip(*[string[i:] for i in range(n)])
        return [''.join(ngram) for ngram in ngrams]


    def explode_str(self, df, col, sep):
        s = df[col]
        i = np.arange(len(s)).repeat(s.str.count(sep) + 1)
        return df.iloc[i].assign(**{col: sep.join(s).split(sep)})


    def cossim_top(self, A, B, ntop, lower_bound=0):
        return awesome_cossim_topn(A, B ,ntop, lower_bound)


    def get_matches_df(self, sparse_matrix, A, B):
        non_zeros = sparse_matrix.nonzero()

        sparserows = non_zeros[0]
        sparsecols = non_zeros[1]


        nr_matches = sparsecols.size

        nr_matches1 =sparserows.size

        left_side = np.empty([nr_matches], dtype=object)
        right_side = np.empty([nr_matches], dtype=object)
        similarity = np.zeros(nr_matches)
        ctln_idx=np.zeros(nr_matches, dtype=np.int)
        input_idx=np.zeros(nr_matches1, dtype = np.int)

        for index in range(0, nr_matches):
            left_side[index] = A[sparserows[index]]   # input
            right_side[index] = B[sparsecols[index]]
            similarity[index] = sparse_matrix.data[index]
            ctln_idx[index] = sparsecols[index]
            input_idx[index] = sparserows[index]

        return pd.DataFrame({'ctln_idx': ctln_idx,
                             'input_idx': input_idx,
                             'Input Drug': left_side,
                             'Standard Drug': right_side,
                             'similarity': similarity})


    def match(self, df, df_clean, input_col):
        """
        input_col is column from input dataframe

        returns dataframe after calculating similarity between two input columns
        """

        df.rename(columns={input_col : 'processed'}, inplace=True)

        # Creating TF-IDF matrix for input data and standard-synonym data
        vectorizer = TfidfVectorizer(analyzer=self.ngrams)
        tf_idf_matrix_clean = vectorizer.fit_transform(df_clean['processed'])
        tf_idf_matrix_dirty = vectorizer.transform(df['processed'])

        # Calculating the cosine similarity between the two matrices features
        matches = self.cossim_top(tf_idf_matrix_dirty, tf_idf_matrix_clean.transpose(), 1, 0)

        df.reset_index(inplace=True)
        df_clean.reset_index(inplace=True)


        matches_df = self.get_matches_df(matches, df['processed'], df_clean['processed'])

        return matches_df


    # len diff <=1-2 can also be added in condition to check for spelling mistakes
    def rem_dup(self, s, delimiter):

        """
        removes duplicates from delimiter separated values in a column
        """

        all_s = str(s).split(delimiter)
        processed_s = []
        unique_s = []

        for val in all_s:
            temp_s = re.sub('[^\w]+','', val).lower().strip()
            if temp_s not in processed_s:
                processed_s.append(temp_s)
                unique_s.append(val)

        if len(unique_s)>1:
            return delimiter.join(unique_s)
        elif len(unique_s)==1:
            return ''.join(unique_s)
        else:
            return ''


    def syn_prime_ctln_idx(self, df):

        """
        creates a mapping between synonym and primary name index
        """

        map_dict = {}


        for syn_idx, idx in zip(df['syn_index'], df['idx']):
            map_dict[syn_idx] = idx

        return map_dict


    def get_org_cols(self, df, input_df, ctln_df, dqs_flag):

        """
        get original input drug column and ctln df index

        df: drug source similarity dataframe
        input_df: aact or dqs drugs dataframe
        ctln_df: ctln df after merging combination drugs in primary name , merged_ctln

        """
        ## can add here code to trim spaces on indexes columns

        # getting original drugs name as similarity out2 df contains processed drugs
        #df_new = pd.merge(df, input_df[['drug_name', 'ID', 'datasource', 'nct_id']], how='left', left_on=['input_idx'], right_on=['ID'])

        # fixed some missing input drugs value here by keeping input df on left
        df_new = pd.merge(input_df, df, how='left', right_on=['input_idx'], left_on=['ID'])

        map_dict = self.syn_prime_ctln_idx(ctln_df)
        df_new['ctln_prime_idx'] = df_new['ctln_idx'].map(map_dict).fillna(df_new['ctln_idx'])

        final_map1 = pd.merge(ctln_df, df_new, how='left', left_on=['idx'], right_on =['ctln_prime_idx'] )

        final_map1.drop(columns=['idx', 'syn_index', 'temp_id', 'drugprimaryname_processed', 'drugnamesynonyms_processed', 'index', 'ID', 'processed', 'ctln_idx',
           'input_idx', 'Input Drug', 'Standard Drug'], inplace=True)

        #final_map1.fillna('', inplace=True)

        return final_map1
        #return df_new


    def agg_data(self, df_aact, dqs_flag, df_dqs=None):

        """
        returns aggregated aact/dqs dataframe
        """

        if dqs_flag == 1:

            agg_dqs_df = df_dqs.groupby('ctln_prime_idx', as_index=False).agg({

                'drugid' : lambda x: max(str(i) for i in x),
                'drugprimaryname' : lambda x: max(str(i) for i in x),
                'casnumbers' : lambda x: max(str(i) for i in x),
                'drugnamesynonyms' : lambda x: '|'.join([str(i) for i in x]),


                'dqs_trialids' : lambda x: '|'.join(str(i) for i in x),
                'dqs_drug_name': lambda x: '|'.join(str(i) for i in x),
                'similarity': lambda x: '|'.join(str(i) for i in x),

            })


            agg_aact_df = df_aact.groupby('ctln_prime_idx', as_index=False).agg({

            'drugid' : lambda x: max(str(i) for i in x),
            'drugprimaryname' : lambda x: max(str(i) for i in x),
            'casnumbers' : lambda x: max(str(i) for i in x),
            'drugnamesynonyms' : lambda x: '|'.join([str(i) for i in x]),

            'aact_trialids' : lambda x: '|'.join(str(i) for i in x),
            'aact_drug_name': lambda x: '|'.join(str(i) for i in x),
            'similarity': lambda x: '|'.join(str(i) for i in x),

        })

            agg_aact_df['ctln_prime_idx'] = agg_aact_df['ctln_prime_idx'].apply(int)



            return agg_aact_df, agg_dqs_df



        agg_aact_df = df_aact.groupby('ctln_prime_idx', as_index=False).agg({

            'drugid' : lambda x: max(str(i) for i in x),
            'drugprimaryname' : lambda x: max(str(i) for i in x),
            'casnumbers' : lambda x: max(str(i) for i in x),
            'drugnamesynonyms' : lambda x: '|'.join([str(i) for i in x]),

            'aact_trialids' : lambda x: '|'.join(str(i) for i in x),
            'aact_drug_name': lambda x: '|'.join(str(i) for i in x),
            'similarity': lambda x: '|'.join(str(i) for i in x),

        })

        agg_aact_df['ctln_prime_idx'] = agg_aact_df['ctln_prime_idx'].apply(int)

        return agg_aact_df


    def combine_trialids(self, x, dqs_flag):

        """
        this function creates additional column having trialids
        from different data sources combined
        """

        # creating combined trial ids column

        if dqs_flag == 0:

            if x['ctln_trialids']!='' and x['aact_trialids']!='':
                return x['ctln_trialids']+ '|' + x['aact_trialids']

            elif x['aact_trialids']!='':
                return x["aact_trialids"]

            elif x['ctln_trialids']!='':
                return x['ctln_trialids']

            else:
                return ''



        elif dqs_flag == 1:

            # creating combined drugs column
            if x['ctln_trialids']!='' and x['aact_trialids']!='' and x['dqs_trialids']!='':
                return x['ctln_trialids']+ '|' + x['aact_trialids']+ '|' + x['dqs_trialids']

            elif x['ctln_trialids']!='' and x['aact_trialids']!='':
                return x['ctln_trialids']+ '|' + x['aact_trialids']

            elif x['ctln_trialids']!='' and x['dqs_trialids']!='':
                return x['ctln_trialids']+ '|' + x['dqs_trialids']

            elif x['aact_trialids']!='' and x['dqs_trialids']!='':
                return x['aact_trialids']+ '|' + x['dqs_trialids']

            elif x['dqs_trialids']!='':
                return x['dqs_trialids']

            elif x['aact_trialids']!='':
                return x['aact_trialids']

            elif x['ctln_trialids']!='':
                return x['ctln_trialids']

            else:
                return ''



    def combine_syn(self, x, dqs_flag):

        """
        this function creates additional column having drugs synonym
        from different data sources combined
        """

        if dqs_flag == 0:

            if x['drugnamesynonyms']!='' and x['aact_drug_name']!='':
                return x['drugnamesynonyms']+ '|' + x['aact_drug_name']

            elif x['aact_drug_name']!='':
                return x['aact_drug_name']

            elif x['drugnamesynonyms']!='':
                return x['drugnamesynonyms']

            else:
                return ''



        elif dqs_flag == 1:

            # creating combined drugs column
            if x['drugnamesynonyms']!='' and x['aact_drug_name']!='' and x['dqs_drug_name']!='':
                return x['drugnamesynonyms']+ '|' + x['aact_drug_name']+ '|' + x['dqs_drug_name']

            elif x['drugnamesynonyms']!='' and x['aact_drug_name']!='':
                return x['drugnamesynonyms']+ '|' + x['aact_drug_name']

            elif x['drugnamesynonyms']!='' and x['dqs_drug_name']!='':
                return x['drugnamesynonyms']+ '|' + x['dqs_drug_name']

            elif x['aact_drug_name']!='' and x['dqs_drug_name']!='':
                return x['aact_drug_name']+ '|' + x['dqs_drug_name']

            elif x['dqs_drug_name']!='':
                return x['dqs_drug_name']

            elif x['aact_drug_name']!='':
                return x['aact_drug_name']

            elif x['drugnamesynonyms']!='':
                return x['drugnamesynonyms']

            else:
                return ''


    def load_json(self, file_path):

        """
        This function is used to load config json having location info and parameters
        """
        with open(file_path, 'r') as config_file:
            config_data = json.load(config_file)

        return config_data


    def main(self):


        drug_std_config_file_location="drug_standardization_config.json"

        configuration=JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH+'/'+CommonConstants.ENVIRONMENT_CONFIG_FILE)
        s3_bucket_name = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,"s3_bucket_name"])

        '''

        if drug_std_config_file_location is None:
            if len(sys.argv)<2:
                logging.info("config file location not passed as an argument!")
                sys.exit(1)

            drug_std_config_file_location = sys.argv[1]

        '''

       # config_data = drug_std.load_json(drug_std_config_file_location)
        with open(drug_std_config_file_location, 'r') as config_file:
            config_data=json.load(config_file)

        logging.info("config json loaded successfully!")

        """
        dqs_flag:
        - value 1 means to execute function to map DQS drug to main citeline data
        - value 0 means not to execute function to map DQS drug to main citeline data
        """
        dqs_flag = config_data["parameters"]["dqs_flag"]
        logging.info("Setting dqs flag to %d", dqs_flag)
        print("=================================")
        print(CommonConstants.AIRFLOW_CODE_PATH)
        print("=================================")
        #aact_drugs = '/app/clinical_design_center/data_management/sanofi_ctfo/code/aact_drugs.csv'
        aact_drugs = CommonConstants.AIRFLOW_CODE_PATH + '/aact_drugs.csv'

        #citeline_drugs = '/app/clinical_design_center/data_management/sanofi_ctfo/code/' \
        #                'ispharmadrug_true_ctln.csv'
        citeline_drugs = CommonConstants.AIRFLOW_CODE_PATH  + \
                         'ispharmadrug_true_ctln.csv'

        delta_drug_path = CommonConstants.AIRFLOW_CODE_PATH  + \
                         '/delta_drug'
        delta_drug_csv_path = CommonConstants.AIRFLOW_CODE_PATH + \
                          '/delta_drug/*.csv '



        if os.path.exists(aact_drugs):
            os.system('rm ' + aact_drugs)
        os.system('rm -r ' + delta_drug_path)
        os.system('hadoop fs -copyToLocal /user/hive/warehouse/delta_drug '
                  + CommonConstants.AIRFLOW_CODE_PATH)

        #os.system('mv /app/clinical_design_center/data_management/sanofi_ctfo/code/delta_drug/*.csv '
         #         '/app/clinical_design_center/data_management/sanofi_ctfo/code/aact_drugs.csv')
        os.system('mv' + delta_drug_csv_path + ' ' + aact_drugs)



        if dqs_flag == 1:
            os.system('aws s3 cp s3://{}/clinical-data-lake/temp/drug_std/dqs_drugs.csv ./'.format(s3_bucket_name))

        if os.path.exists(citeline_drugs):
            os.system('rm ' + citeline_drugs)
        os.system('rm -r ' + CommonConstants.AIRFLOW_CODE_PATH + '/citeline_drugs')
        os.system('hadoop fs -copyToLocal /user/hive/warehouse/citeline_drugs '
                  + CommonConstants.AIRFLOW_CODE_PATH )

        #os.system('mv /app/clinical_design_center/data_management/sanofi_ctfo/code/citeline_drugs/*.csv '
         #         '/app/clinical_design_center/data_management/sanofi_ctfo/code/ispharmadrug_true_ctln.csv')
        os.system('mv '+ CommonConstants.AIRFLOW_CODE_PATH + '/citeline_drugs/*.csv '
                  + CommonConstants.AIRFLOW_CODE_PATH + '/ispharmadrug_true_ctln.csv')





        """
        THRESHOLD_SCORE:
        - score cut-off to be used to consider drug same as standard data
        """
        THRESHOLD_SCORE= config_data["parameters"]["threshold_score"]
        logging.info("Setting threshold score to %.2f", THRESHOLD_SCORE)

        logging.info("starting main method..")

        logging.info("processing citeline data..")

        try:
            ctln_df=pd.read_csv("ispharmadrug_true_ctln.csv", error_bad_lines=False)

        except Exception as e:
            print(e)

        ctln_df = self.process_ctln_data(ctln_df)


        logging.info("processing AACT data..")

        try:
            aact_df=pd.read_csv("aact_drugs.csv", error_bad_lines=False)

        except Exception as e:
            print(e)

        aact_df = self.process_aact_data(aact_df)



        logging.info("processing DQS data..")

        if dqs_flag  == 1:
            try:
                dqs_df=pd.read_csv("dqs_drugs.csv", error_bad_lines=False)

            except Exception as e:
                print(e)

            dqs_df = self.process_dqs_data(dqs_df)



        logging.info("finding combination drugs and adding them as primary drug name..")

        ## combination of drugs in aact/dqs being added as primary name in citeline df
        if dqs_flag == 1:
            merged_ctln = self.merge_combination_to_citeline(aact_df, ctln_df, dqs_flag, dqs_df)
        elif dqs_flag == 0:
            merged_ctln = self.merge_combination_to_citeline(aact_df, ctln_df, dqs_flag)


        logging.info("preparing input for algo..")

        ## combine drug primary and synonym name into one column for passing to algo
        ctln_df2_one_df = self.combine_primary_syn(merged_ctln)


        logging.info("creating similarity-score dataframe..")

        ## creating a df with similarity scores
        if dqs_flag == 1:
            dqs_ctln_df = self.match(dqs_df, ctln_df2_one_df, "processed") ## processed is column from dqs_df (col we get after processing drug name)


        aact_ctln_df = self.match(aact_df, ctln_df2_one_df, "processed")

        logging.info("Getting required columns from input dataframes..")


        ## get original column names of input and source df
        temp1 = self.get_org_cols(aact_ctln_df, aact_df, merged_ctln, dqs_flag)

        aact_below_threshold = temp1[temp1['similarity']<THRESHOLD_SCORE].copy()
        temp1 = temp1[temp1['similarity']>=THRESHOLD_SCORE].copy()


        if dqs_flag == 1:
            temp2 = self.get_org_cols(dqs_ctln_df, dqs_df, merged_ctln, dqs_flag)
            dqs_below_threshold = temp2[temp2['similarity']<THRESHOLD_SCORE].copy()
            temp2 = temp2[temp2['similarity']>=THRESHOLD_SCORE].copy()


        #current_timestamp = str(datetime.today()).replace(':', '-').split('.')[0]



        if dqs_flag == 1:
            #aact_dqs_cols = temp1.columns.tolist() + temp2.columns.tolist()
            #aact_dqs_df = pd.DataFrame(pd.concat([aact_below_threshold, dqs_below_threshold], axis=1), columns= aact_dqs_cols)

            logging.info("writing not mapped dqs and aact drugs to CSV...")


            dqs_below_threshold.to_csv("not_matched_dqs.csv", index=False)
            aact_below_threshold.to_csv("not_matched_aact.csv", index=False)

        elif dqs_flag == 0:
            logging.info("writing not mapped aact drugs to CSV...")

            aact_below_threshold.to_csv("not_matched_aact.csv", index=False)



        if dqs_flag == 1:
            logging.info("aggregating aact and dqs data..")
            agg_temp1, agg_temp2 = self.agg_data(temp1, dqs_flag, temp2)

        elif dqs_flag == 0:

            logging.info("aggregating aact data..")
            agg_temp1= self.agg_data(temp1, dqs_flag)

        if dqs_flag == 1:
            temp3 = pd.merge(agg_temp1, agg_temp2[['ctln_prime_idx' ,'dqs_trialids', 'dqs_drug_name', 'similarity']], how='outer', on=['ctln_prime_idx'], suffixes= ("_aact", "_dqs"))
            temp3.drop(columns=['drugid', 'drugprimaryname', 'casnumbers', 'drugnamesynonyms'], inplace=True)


        merged_ctln.drop(columns=['syn_index', 'temp_id', 'drugprimaryname_processed', 'drugnamesynonyms_processed'], inplace=True)
        merged_ctln.fillna('', inplace=True)

        agg_merged_ctln = merged_ctln.groupby('idx', as_index=False).agg({
                    'drugprimaryname' : lambda x: max(str(i) for i in x),
                    'drugid' : lambda x: max(str(i) for i in x),
                    'casnumbers' : lambda x: max(str(i) for i in x),
                    'drugnamesynonyms' : lambda x: '|'.join(str(i) for i in x),

                    'ctln_trialids' : lambda x: '|'.join(str(i) for i in x),

        })

        agg_merged_ctln['drugnamesynonyms'] = agg_merged_ctln['drugnamesynonyms'].apply(lambda x: x if x.split('|')[0]!='' else '')
        agg_merged_ctln['ctln_trialids'] = agg_merged_ctln['ctln_trialids'].apply(lambda x: x if x.split('|')[0]!='' else '')

        agg_merged_ctln['idx']=agg_merged_ctln['idx'].apply(int)

        if dqs_flag == 1:
            temp4= pd.merge(agg_merged_ctln, temp3, how='left', left_on=['idx'], right_on=['ctln_prime_idx'])
        elif dqs_flag == 0:
            temp4= pd.merge(agg_merged_ctln, agg_temp1[['aact_trialids', 'aact_drug_name', 'similarity', 'ctln_prime_idx']], how='left', left_on=['idx'], right_on=['ctln_prime_idx'])

        temp4.fillna('', inplace=True)

        logging.info("combining all drugs and trialids into one column...")



        temp4['all_synonyms'] = temp4.apply(lambda x: self.combine_syn(x, dqs_flag), axis=1)
        temp4['all_trialids'] = temp4.apply(lambda x: self.combine_trialids(x, dqs_flag), axis=1)


        ## not removing duplicates from other columns that are '|' separated

        logging.info("removing duplicate drugs after combining all drugs...")

        temp4['all_synonyms'] = temp4['all_synonyms'].apply(lambda x: self.rem_dup(x, '|') )
        temp4['all_trialids'] = temp4['all_trialids'].apply(lambda x: self.rem_dup(x, '|') )


        temp4.drop(columns=['idx', 'ctln_prime_idx'], inplace=True)


        logging.info("writing mapping file to csv (can even mention location here)...")
        #current_timestamp = str(datetime.today()).replace(':', '-').split('.')[0]
        temp4.to_csv("drug_mapping_file.csv", index=False)


        os.system('aws s3 cp drug_mapping_file.csv s3://{}/clinical-data-lake/uploads/DRUG_MAPPING_FILE_LATEST/'.format(s3_bucket_name))
        os.system('aws s3 cp not_matched_aact.csv s3://{}/clinical-data-lake/uploads/NOT_MATCHED_DRUGS/'.format(s3_bucket_name))

        print("Finished!!")

        end_time = time.time()

        logging.info("Executed in {} minutes".format(round((end_time-initial_time)/60,2)))



if __name__ == '__main__':

    drug_std = DrugStandardization()



    drug_std.main()


