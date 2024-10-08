




#!/usr/bin/python
# -*- coding: utf-8 -*-
# __author__ = 'ZS Associates'

######################################################Module Information################################################
#   Module Name         :   DiseaseStandardization
#   Purpose             :   standardization of disease names across all data sources
#   Input Parameters    :
#   Output              :
#   Execution Steps     :
#   Predecessor module  :
#   Successor module    :   NA
#   Last changed on     :   10 Oct 2020
#   Last changed by     :   Ishaan
#   Reason for change   :
########################################################################################################################

import re
import os
import json
import time
import pandas as pd
import numpy as np
import sparse_dot_topn.sparse_dot_topn as ct
import warnings
from datetime import datetime
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

# import icd10
# from icd9cms.icd9 import search
from ftfy import fix_text
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize

# all module level constants are defined here
MODULE_NAME = "DiseaseStandardization"

class DiseaseStandardization(object):

    def __init__(self):

        print("init done")


    def __enter__(self):
        return self

    def __exit__(self):
        pass


    def removeSpecial(self, input_disease):
        # removes special characters from input disease
        chars_to_remove = ["=", "?", ")", "(", ']', '[', '-']
        rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
        input_disease = re.sub(rx, '', input_disease)
        return input_disease

    def removeStopwords(self, disease):


        # Getting stopwords for english language
        stop_words = set(stopwords.words('english'))
        if len(disease) >= 30:
            word_tokens = word_tokenize(disease)
            filtered_sentence = [w for w in word_tokens if not w in stop_words]

            filtered_sentence = []

            for w in word_tokens:
                if w not in stop_words:
                    filtered_sentence.append(w)

            return (' '.join(filtered_sentence))

        return disease

    def ngrams(self, string, n=3):

        """
        returns n-grams of input disease after some preprocessing on it

        Parameters:
        string : input disease
        n : n is value ngrams in which text would be breaken into. n=3 means 3-grams

        """

        string = fix_text(string)  # fix text
        string = string.encode("ascii", errors="ignore").decode()  # remove non ascii chars
        string = string.lower()
        chars_to_remove = [")", "(", ".", "|", "[", "]", "{", "}", "'", "=",
                           "?"]  # specify chars you want to remove here
        rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
        string = re.sub(rx, '', string)
        string = string.replace('&', 'and')
        string = string.replace(',', ' ')
        string = string.replace('-', ' ')
        string = string.title()  # normalise case - capital at start of each word
        string = re.sub(' +', ' ',
                        string).strip()  # get rid of multiple spaces and replace with a single
        string = ' ' + string + ' '  # pad disease names for ngrams
        ngrams = zip(*[string[i:] for i in range(n)])
        return [''.join(ngram) for ngram in ngrams]

    def cossim_top(self, A, B, ntop, lower_bound=0):

        """
        returns the cosine similarity matrix between two matrices

        Parameters:

        A : input matrix 1
        B : input matrix 2 (transpose of A in case mapping against same input)
        ntop : return top n records based on similarity score
        lower_bound : set threshold for scores. values having score less than this would be discarded
        """
        A = A.tocsr()
        B = B.tocsr()
        M, _ = A.shape
        _, N = B.shape

        idx_dtype = np.int32

        nnz_max = M * ntop

        indptr = np.zeros(M + 1, dtype=idx_dtype)
        indices = np.zeros(nnz_max, dtype=idx_dtype)
        data = np.zeros(nnz_max, dtype=A.dtype)

        ct.sparse_dot_topn(
            M, N, np.asarray(A.indptr, dtype=idx_dtype),
            np.asarray(A.indices, dtype=idx_dtype),
            A.data,
            np.asarray(B.indptr, dtype=idx_dtype),
            np.asarray(B.indices, dtype=idx_dtype),
            B.data,
            ntop,
            lower_bound,
            indptr, indices, data)

        return csr_matrix((data, indices, indptr), shape=(M, N))

    def get_matches_df(self, sparse_matrix, input_matrix, std_matrix):

        """
        returns a pandas dataframe with columns input disease, standard disease name and their matching score

        Parameters:

        sparse_matrix : matrix having cosine similarity scores calculated using cossim_top function
        input_matrix : input disease data
        std_matrix : standard disease data

        """
        non_zeros = sparse_matrix.nonzero()

        sparserows = non_zeros[0]
        sparsecols = non_zeros[1]

        nr_matches = sparsecols.size

        left_side = np.empty([nr_matches], dtype=object)
        right_side = np.empty([nr_matches], dtype=object)
        similarity = np.zeros(nr_matches)

        for index in range(0, nr_matches):
            left_side[index] = input_matrix[sparserows[index]]
            right_side[index] = std_matrix[sparsecols[index]]
            similarity[index] = sparse_matrix.data[index]

        return pd.DataFrame({'Input Disease': left_side,
                             'Standard Disease': right_side,
                             'similarity': similarity})

    def getSyn(self, a, b):
        """
        return synonym if input disease is mapped to synonym

        """

        if a != b:
            return a

        return ' '

    def standardize_aact(self,disease_df):

        print("Started mapping AACT diseases data...")



        # Filtering data for aact datasource
        aact_df = disease_df[disease_df['datasource'] == 'aact']

        aact_df.rename(columns={'disease_name': 'disease'}, inplace=True)

        aact_df['processed'] = aact_df['disease'].apply(lambda x: self.removeSpecial(x))

        aact_df['processed'] = aact_df['processed'].apply(lambda x: self.removeStopwords(x))

        df = aact_df.copy()

        df_clean = pd.read_csv('std_syn_one.csv')
        df['processed'] = df['processed'].apply(str)

        # ### Algorithm

        # Creating TF-IDF matrix for input data and standard-synonym data
        vectorizer = TfidfVectorizer(min_df=1, analyzer=self.ngrams)
        tf_idf_matrix_clean = vectorizer.fit_transform(df_clean['std_disease'])
        tf_idf_matrix_dirty = vectorizer.transform(df['processed'])

        # execute this cell if any KeyError in cell having get_matches_df function
        df.reset_index(inplace=True)

        # In[27]:

        # Calculating the cosine similarity between the two matrices features
        # 8-9mins for 550k records
        t1 = time.time()
        # Handling case when there will be no disease data in matrix after cleaning
        try:
            matches = self.cossim_top(tf_idf_matrix_dirty, tf_idf_matrix_clean.transpose(), 1, 0)
        except IndexError as e:
            print("Exception: ", e)
            return []   # returning empty list object
        t = time.time() - t1
        print("Execution time for calculating similarity matrix:", t)

        # In[28]:

        matches_df = self.get_matches_df(matches, df['processed'], df_clean['std_disease'])

        # In[30]:

        # Code to get Standard Name for a particular Input Disease
        matches_df.rename(columns={'Standard Disease': 'std_syn'}, inplace=True)

        df1 = pd.read_csv("exploded.csv")
        std_disease_list = list(set(df1['standard_disease'].tolist()))
        df1['disease'] = df1['disease'].apply(
            str)  # to convert NaN (nan) values to str to avoid type error in process extract 2049 nan values
        df1.drop_duplicates(subset=['disease'], keep='first',
                            inplace=True)  # remove duplicates from synonyms column for below line of code to work (unique values required)
        matches_df['Standard Names'] = matches_df['std_syn'].map(
            df1.set_index('disease')['standard_disease']).fillna(matches_df['std_syn'])
        matches_df['synonym'] = matches_df.apply(lambda x: self.getSyn(x['std_syn'], x['Standard '
                                                                                       'Names']), axis=1)

        test = matches_df.copy()

        test.drop_duplicates(subset=['Input Disease'], keep='first', inplace=True)

        test.rename(columns={'Input Disease': 'processed'}, inplace=True)

        aact_df = test.merge(df, on='processed', how='inner')

        print("Completed standardization of AACT diseases")
        print("Writing output to CSV File..")

        aact_df.to_csv("aact_diseases.csv", index=False)  # choose 0.41 as threshold

        print("Done!!")

        return aact_df


    def standardize_dqs(self, disease_df):
        print("Started mapping for DQS data...")
        # # Mapping for DQS


        dqs_df = disease_df[disease_df['datasource'] == 'ir']

        dqs_df.rename(columns={'disease_name': 'disease'}, inplace=True)

        dqs_df['processed'] = dqs_df['disease'].apply(lambda x: self.removeSpecial(x))

        dqs_df['processed'] = dqs_df['processed'].apply(lambda x: self.removeStopwords(x))

        df = dqs_df.copy()

        df_clean = pd.read_csv('std_syn_one.csv')
        df['processed'] = df['processed'].apply(str)

        # Creating TF-IDF matrix for input data and standard-synonym data
        vectorizer = TfidfVectorizer(min_df=1, analyzer=self.ngrams)
        tf_idf_matrix_clean = vectorizer.fit_transform(df_clean['std_disease'])
        tf_idf_matrix_dirty = vectorizer.transform(df['processed'])

        df.reset_index(inplace=True)

        # Calculating the cosine similarity between the two matrices features
        # 8-9mins for 550k records
        t1 = time.time()
        matches = self.cossim_top(tf_idf_matrix_dirty, tf_idf_matrix_clean.transpose(), 1, 0)
        t = time.time() - t1
        print("Execution time for calculating similarity matrix:", t)

        matches_df = self.get_matches_df(matches, df['processed'], df_clean['std_disease'])

        # Code to get Standard Name for a particular Input Disease
        matches_df.rename(columns={'Standard Disease': 'std_syn'}, inplace=True)

        df1 = pd.read_csv("exploded.csv")
        std_disease_list = list(set(df1['standard_disease'].tolist()))
        df1['disease'] = df1['disease'].apply(
            str)  # to convert NaN (nan) values to str to avoid type error in process extract 2049 nan values
        df1.drop_duplicates(subset=['disease'], keep='first',
                            inplace=True)  # remove duplicates from synonyms column for below line of code to work (unique values required)
        matches_df['Standard Names'] = matches_df['std_syn'].map(
            df1.set_index('disease')['standard_disease']).fillna(matches_df['std_syn'])
        matches_df['synonym'] = matches_df.apply(
            lambda x: self.getSyn(x['std_syn'], x['Standard Names']), axis=1)

        test = matches_df.copy()

        test.drop_duplicates(subset=['Input Disease'], keep='first', inplace=True)

        test.rename(columns={'Input Disease': 'processed'}, inplace=True)

        dqs_df = test.merge(df, on='processed', how='inner')

        print("Completed standardization of DQS diseases")
        print("Writing output to CSV File..")

        dqs_df.to_csv("dqs_diseases.csv", index=False)

        print("Done!!")

        return dqs_df


    def standardize_citeline(self, disease_df):
        print("Started mapping for Citeline diseases data...")


        # # Mapping Citeline diseases
        #print('#######################')

        ctln_df = disease_df[disease_df['datasource'] == 'citeline']
        #print(ctln_df)

        ctln_df.rename(columns={'disease_name': 'disease'}, inplace=True)

        # ###Processing Disease  Data

        ctln_df.dropna(subset=['disease'], inplace=True)


        # #### Citeline data already split on ^ from EMR

        ctln_df['processed'] = ctln_df['disease'].apply(str)

        ctln_df['processed'] = ctln_df['processed'].apply(lambda x: self.removeSpecial(x))

        ctln_df['processed'] = ctln_df['processed'].apply(lambda x: self.removeStopwords(x))

        df = ctln_df.copy()

        df_clean = pd.read_csv('std_syn_one.csv')
        df['processed'] = df['processed'].apply(str)
        #print('df---processes')
        #print(df['processed'])
        # Creating TF-IDF matrix for input data and standard-synonym data
        vectorizer = TfidfVectorizer(min_df=1, analyzer=self.ngrams)
        tf_idf_matrix_clean = vectorizer.fit_transform(df_clean['std_disease'])
        tf_idf_matrix_dirty = vectorizer.transform(df['processed'])

        # execute this cell if any KeyError in cell having get_matches_df function
        df.reset_index(inplace=True)

        # Calculating the cosine similarity between the two matrices features
        # 3-4mins for around 400k records
        # 21mins for 4.2mill records
        t1 = time.time()
        #print('*********************')
        #print(tf_idf_matrix_dirty)
        #print('--------------------')
        #print(tf_idf_matrix_clean)
        try:
            matches = self.cossim_top(tf_idf_matrix_dirty, tf_idf_matrix_clean.transpose(), 1, 0)
        except IndexError as e:
            print("Exception: ", e)
            return []
        t = time.time() - t1
        print("Execution time for calculating similarity matrix:", t)

        matches_df = self.get_matches_df(matches, df['processed'], df_clean['std_disease'])

        # Code to get Standard Name for a particular Input Disease
        matches_df.rename(columns={'Standard Disease': 'std_syn'}, inplace=True)

        df1 = pd.read_csv("exploded.csv")
        std_disease_list = list(set(df1['standard_disease'].tolist()))
        df1['disease'] = df1['disease'].apply(
            str)  # to convert NaN (nan) values to str to avoid type error in process extract 2049 nan values
        df1.drop_duplicates(subset=['disease'], keep='first',
                            inplace=True)  # remove duplicates from synonyms column for below line of code to work (unique values required)
        matches_df['Standard Names'] = matches_df['std_syn'].map(
            df1.set_index('disease')['standard_disease']).fillna(matches_df['std_syn'])
        matches_df['synonym'] = matches_df.apply(
            lambda x: self.getSyn(x['std_syn'], x['Standard Names']), axis=1)

        test = matches_df.copy()

        test.drop_duplicates(subset=['Input Disease'], keep='first', inplace=True)

        test.rename(columns={'Input Disease': 'processed'}, inplace=True)

        citeline_df = test.merge(df, on='processed', how='inner')

        print("Completed standardization of Citeline diseases")
        print("Writing output to CSV File..")

        citeline_df.to_csv("citeline_diseases.csv", index=False)

        print("Done!!")

        return citeline_df

    def standardize_pharma_diseases(self, disease_df):

        print("Started mapping for pharmaprojects drug diseases data...")

        disease_df.rename(columns={'disease_name': 'disease'}, inplace=True)

        pharma_df = disease_df[disease_df['datasource'] == 'pharmaprojects'].copy()

        pharma_df['disease'] = pharma_df['disease'].str.strip()
        pharma_df.drop_duplicates(subset='disease', inplace=True)

        #pharma_df.dropna(inplace=True)

        pharma_df['processed'] = pharma_df['disease'].apply(str)

        pharma_df['processed'] = pharma_df['processed'].apply(lambda x: self.removeSpecial(x))

        pharma_df['processed'] = pharma_df['processed'].apply(lambda x: self.removeStopwords(x))

        df = pharma_df.copy()

        df_clean = pd.read_csv('std_syn_one.csv')
        df['processed'] = df['processed'].apply(str)

        # Creating TF-IDF matrix for input data and standard-synonym data
        vectorizer = TfidfVectorizer(min_df=1, analyzer=self.ngrams)
        tf_idf_matrix_clean = vectorizer.fit_transform(df_clean['std_disease'])
        tf_idf_matrix_dirty = vectorizer.transform(df['processed'])

        # execute this cell if any KeyError in cell having get_matches_df function
        df.reset_index(inplace=True)

        # Calculating the cosine similarity between the two matrices features
        # 3-4mins for around 400k records
        # 21mins for 4.2mill records
        t1 = time.time()
        matches = self.cossim_top(tf_idf_matrix_dirty, tf_idf_matrix_clean.transpose(), 1, 0)
        t = time.time() - t1
        print("Execution time for calculating similarity matrix:", t)

        matches_df = self.get_matches_df(matches, df['processed'], df_clean['std_disease'])

        # Code to get Standard Name for a particular Input Disease
        matches_df.rename(columns={'Standard Disease': 'std_syn'}, inplace=True)

        df1 = pd.read_csv("exploded.csv")
        std_disease_list = list(set(df1['standard_disease'].tolist()))
        df1['disease'] = df1['disease'].apply(
            str)  # to convert NaN (nan) values to str to avoid type error in process extract 2049 nan values
        df1.drop_duplicates(subset=['disease'], keep='first',
                            inplace=True)  # remove duplicates from synonyms column for below line of code to work (unique values required)
        matches_df['Standard Names'] = matches_df['std_syn'].map(
            df1.set_index('disease')['standard_disease']).fillna(matches_df['std_syn'])
        matches_df['synonym'] = matches_df.apply(
            lambda x: self.getSyn(x['std_syn'], x['Standard Names']), axis=1)

        test = matches_df.copy()

        test.drop_duplicates(subset=['Input Disease'], keep='first', inplace=True)

        test.rename(columns={'Input Disease': 'processed'}, inplace=True)

        pharmadrug_disease_df = test.merge(df, on='processed', how='inner')

        print("Completed standardization of pharmaproject diseases diseases")
        print("Writing output to CSV File..")

        pharmadrug_disease_df.to_csv("pharmadrug_diseases.csv", index=False)

        print("Done!!")

        return pharmadrug_disease_df


    def standardize_drg_diseases(self, disease_df):
        print("Started mapping for drg diseases data...")

        disease_df.rename(columns={'disease_name': 'disease'}, inplace=True)

        drg_df = disease_df[disease_df['datasource'] == 'drg'].copy()

        drg_df['disease'] = drg_df['disease'].str.strip()
        drg_df.drop_duplicates(subset='disease', inplace=True)

        #drg_df.dropna(inplace=True)

        print("Number of records in drg: ", drg_df.shape[0])

        drg_df['processed'] = drg_df['disease'].apply(str)

        drg_df['processed'] = drg_df['processed'].apply(lambda x: self.removeSpecial(x))

        drg_df['processed'] = drg_df['processed'].apply(lambda x: self.removeStopwords(x))

        df = drg_df.copy()

        df_clean = pd.read_csv('std_syn_one.csv')
        df['processed'] = df['processed'].apply(str)

        # Creating TF-IDF matrix for input data and standard-synonym data
        vectorizer = TfidfVectorizer(min_df=1, analyzer=self.ngrams)
        tf_idf_matrix_clean = vectorizer.fit_transform(df_clean['std_disease'])
        tf_idf_matrix_dirty = vectorizer.transform(df['processed'])

        # execute this cell if any KeyError in cell having get_matches_df function
        df.reset_index(inplace=True)

        # Calculating the cosine similarity between the two matrices features
        # 3-4mins for around 400k records
        # 21mins for 4.2mill records
        t1 = time.time()
        matches = self.cossim_top(tf_idf_matrix_dirty, tf_idf_matrix_clean.transpose(), 1, 0)
        t = time.time() - t1
        print("Execution time for calculating similarity matrix:", t)

        matches_df = self.get_matches_df(matches, df['processed'], df_clean['std_disease'])


        # Code to get Standard Name for a particular Input Disease
        matches_df.rename(columns={'Standard Disease': 'std_syn'}, inplace=True)

        df1 = pd.read_csv("exploded.csv")
        std_disease_list = list(set(df1['standard_disease'].tolist()))
        df1['disease'] = df1['disease'].apply(
            str)  # to convert NaN (nan) values to str to avoid type error in process extract 2049 nan values
        df1.drop_duplicates(subset=['disease'], keep='first',
                            inplace=True)  # remove duplicates from synonyms column for below line of code to work (unique values required)
        matches_df['Standard Names'] = matches_df['std_syn'].map(
            df1.set_index('disease')['standard_disease']).fillna(matches_df['std_syn'])
        matches_df['synonym'] = matches_df.apply(
            lambda x: self.getSyn(x['std_syn'], x['Standard Names']), axis=1)

        test = matches_df.copy()

        test.drop_duplicates(subset=['Input Disease'], keep='first', inplace=True)

        test.rename(columns={'Input Disease': 'processed'}, inplace=True)

        drg_disease_df = test.merge(df, on='processed', how='inner')

        print("Completed standardization of drg diseases")
        print("Writing output to CSV File..")

        drg_disease_df.to_csv("drg_diseases.csv", index=False)

        print("Done!!")

        return drg_disease_df


    def standardize_data_monitor_diseases(self, disease_df):
        print("Started mapping for dmonprojects data monitor diseases data...")

        disease_df.rename(columns={'disease_name': 'disease'}, inplace=True)

        dmon_df = disease_df[disease_df['datasource'] == 'data monitor'].copy()

        dmon_df['disease'] = dmon_df['disease'].str.strip()
        dmon_df.drop_duplicates(subset='disease', inplace=True)

        #dmon_df.dropna(inplace=True)

        print("Number of records in data monitor: ", dmon_df.shape[0])

        dmon_df['processed'] = dmon_df['disease'].apply(str)

        dmon_df['processed'] = dmon_df['processed'].apply(lambda x: x.replace('+', ' '))

        dmon_df['processed'] = dmon_df['processed'].apply(lambda x: self.removeSpecial(x))

        dmon_df['processed'] = dmon_df['processed'].apply(lambda x: self.removeStopwords(x))

        df = dmon_df.copy()

        df_clean = pd.read_csv('std_syn_one.csv')
        df['processed'] = df['processed'].apply(str)

        # Creating TF-IDF matrix for input data and standard-synonym data
        vectorizer = TfidfVectorizer(min_df=1, analyzer=self.ngrams)
        tf_idf_matrix_clean = vectorizer.fit_transform(df_clean['std_disease'])
        tf_idf_matrix_dirty = vectorizer.transform(df['processed'])

        # execute this cell if any KeyError in cell having get_matches_df function
        df.reset_index(inplace=True)

        # Calculating the cosine similarity between the two matrices features
        # 3-4mins for around 400k records
        # 21mins for 4.2mill records
        t1 = time.time()
        matches = self.cossim_top(tf_idf_matrix_dirty, tf_idf_matrix_clean.transpose(), 1, 0)
        t = time.time() - t1
        print("Execution time for calculating similarity matrix:", t)

        matches_df = self.get_matches_df(matches, df['processed'], df_clean['std_disease'])


        # Code to get Standard Name for a particular Input Disease
        matches_df.rename(columns={'Standard Disease': 'std_syn'}, inplace=True)

        df1 = pd.read_csv("exploded.csv")
        std_disease_list = list(set(df1['standard_disease'].tolist()))
        df1['disease'] = df1['disease'].apply(
            str)  # to convert NaN (nan) values to str to avoid type error in process extract 2049 nan values
        df1.drop_duplicates(subset=['disease'], keep='first',
                            inplace=True)  # remove duplicates from synonyms column for below line of code to work (unique values required)
        matches_df['Standard Names'] = matches_df['std_syn'].map(
            df1.set_index('disease')['standard_disease']).fillna(matches_df['std_syn'])
        matches_df['synonym'] = matches_df.apply(
            lambda x: self.getSyn(x['std_syn'], x['Standard Names']), axis=1)

        test = matches_df.copy()

        test.drop_duplicates(subset=['Input Disease'], keep='first', inplace=True)

        test.rename(columns={'Input Disease': 'processed'}, inplace=True)

        dmon_disease_df = test.merge(df, on='processed', how='inner')

        print("Completed standardization of data monitor diseases")
        print("Writing output to CSV File..")

        dmon_disease_df.to_csv("dmon_diseases.csv", index=False)

        print("Done!!")

        return dmon_disease_df


    def standardize_globocan_diseases(self, disease_df):
        print("Started mapping for ctms diseases data...")

        print("disease df:", disease_df[disease_df['datasource'] == 'ctms'].shape[0] )

        disease_df.rename(columns={'disease_name': 'disease'}, inplace=True)

        globocan_df = disease_df[disease_df['datasource'] == 'ctms'].copy()

        globocan_df['disease'] = globocan_df['disease'].str.strip()
        globocan_df.drop_duplicates(subset='disease', inplace=True)

        #globocan_df.dropna(inplace=True)
        print("Number of records in globocan: ", globocan_df.shape[0])

        globocan_df['processed'] = globocan_df['disease'].apply(str)

        globocan_df['processed'] = globocan_df['processed'].apply(lambda x: x + ' cancer')

        globocan_df['processed'] = globocan_df['processed'].apply(lambda x: self.removeSpecial(x))

        globocan_df['processed'] = globocan_df['processed'].apply(lambda x: self.removeStopwords(x))

        df = globocan_df.copy()

        df_clean = pd.read_csv('std_syn_one.csv')
        df['processed'] = df['processed'].apply(str)

        # Creating TF-IDF matrix for input data and standard-synonym data
        vectorizer = TfidfVectorizer(min_df=1, analyzer=self.ngrams)
        tf_idf_matrix_clean = vectorizer.fit_transform(df_clean['std_disease'])
        tf_idf_matrix_dirty = vectorizer.transform(df['processed'])

        # execute this cell if any KeyError in cell having get_matches_df function
        df.reset_index(inplace=True)

        # Calculating the cosine similarity between the two matrices features
        # 3-4mins for around 400k records
        # 21mins for 4.2mill records
        t1 = time.time()
        matches = self.cossim_top(tf_idf_matrix_dirty, tf_idf_matrix_clean.transpose(), 1, 0)
        t = time.time() - t1
        print("Execution time for calculating similarity matrix:", t)

        matches_df = self.get_matches_df(matches, df['processed'], df_clean['std_disease'])


        # Code to get Standard Name for a particular Input Disease
        matches_df.rename(columns={'Standard Disease': 'std_syn'}, inplace=True)

        df1 = pd.read_csv("exploded.csv")
        std_disease_list = list(set(df1['standard_disease'].tolist()))
        df1['disease'] = df1['disease'].apply(
            str)  # to convert NaN (nan) values to str to avoid type error in process extract 2049 nan values
        df1.drop_duplicates(subset=['disease'], keep='first',
                            inplace=True)  # remove duplicates from synonyms column for below line of code to work (unique values required)
        matches_df['Standard Names'] = matches_df['std_syn'].map(
            df1.set_index('disease')['standard_disease']).fillna(matches_df['std_syn'])
        matches_df['synonym'] = matches_df.apply(
            lambda x: self.getSyn(x['std_syn'], x['Standard Names']), axis=1)

        test = matches_df.copy()

        test.drop_duplicates(subset=['Input Disease'], keep='first', inplace=True)

        test.rename(columns={'Input Disease': 'processed'}, inplace=True)

        globocan_disease_df = test.merge(df, on='processed', how='inner')

        print("Completed standardization of globocan diseases")
        print("Writing output to CSV File..")

        globocan_disease_df.to_csv("globocan_diseases.csv", index=False)

        print("Done!!")

        return globocan_disease_df



    def main(self):
        warnings.filterwarnings('ignore')

        APPLICATION_CONFIG_FILE = "application_config.json"

        configuration = json.load(open(APPLICATION_CONFIG_FILE))

        code_env = configuration["adapter_details"]["generic_config"]["env"]

        initial_time = time.time()

        CURRENT_DATE = datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")
        configuration=JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH+'/'+CommonConstants.ENVIRONMENT_CONFIG_FILE)
        s3_bucket_name = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,"s3_bucket_name"])

        exploded_file = '/app/clinical_design_center/data_management/sanofi_ctfo/code/exploded.csv'
        synonym_file = '/app/clinical_design_center/data_management/sanofi_ctfo/code/std_syn_one.csv'
        delta_file = '/app/clinical_design_center/data_management/sanofi_ctfo/code/delta.csv'

        if os.path.exists(exploded_file):
            os.system('rm ' + exploded_file)
        os.system('aws s3 cp s3://{}/clinical-data-lake/uploads/EXPLODED_FILE/exploded.csv ./'.format(s3_bucket_name))

        if os.path.exists(synonym_file):
            os.system('rm ' + synonym_file)
        os.system('aws s3 cp s3://{}/clinical-data-lake/uploads/SYNONYM_FILE/std_syn_one.csv ./'.format(s3_bucket_name))

        if os.path.exists(delta_file):
            os.system('rm ' + delta_file)
        os.system('rm -r /app/clinical_design_center/data_management/sanofi_ctfo/code/Delta')
        os.system('hadoop fs -copyToLocal /user/hive/warehouse/Delta '
                  '/app/clinical_design_center/data_management/sanofi_ctfo/code/')

        os.system('mv /app/clinical_design_center/data_management/sanofi_ctfo/code/Delta/*.csv '
                  '/app/clinical_design_center/data_management/sanofi_ctfo/code/delta.csv')


        # downloading stopwords
        nltk.download('stopwords')
        nltk.download('punkt')

        # Reading File
        disease_df = pd.read_csv("delta.csv", error_bad_lines=False)
        print("Total number of records: ", disease_df.shape[0])

        df_list = disease_df['datasource'].unique().tolist()
        df_to_combine = []

        if "aact" in df_list:
            aact_df  = self.standardize_aact(disease_df)

            if len(aact_df) > 0:
                aact_df.drop(columns=['std_syn', 'index'], inplace=True)
                df_to_combine.append(aact_df)

        if "ir" in df_list:
            dqs_df = self.standardize_dqs(disease_df)

            if len(dqs_df) > 0:
                dqs_df.drop(columns=['std_syn', 'index'], inplace=True)
                df_to_combine.append(dqs_df)

        if "citeline" in df_list:
            citeline_df = self.standardize_citeline(disease_df)

            if len(citeline_df) > 0:
                citeline_df.drop(columns=['std_syn', 'index'], inplace=True)
                df_to_combine.append(citeline_df)


        if "ctms" in df_list:
            globocan_df = self.standardize_globocan_diseases(disease_df)

            if len(globocan_df) > 0:
                globocan_df.drop(columns=['std_syn', 'index'], inplace=True)
                df_to_combine.append(globocan_df)


        if "drg" in df_list:
            drg_df = self.standardize_drg_diseases(disease_df)

            if len(drg_df) > 0:
                drg_df.drop(columns=['std_syn', 'index'], inplace=True)
                df_to_combine.append(drg_df)

        if "data monitor" in df_list:
            data_monitor_df = self.standardize_data_monitor_diseases(disease_df)

            if len(data_monitor_df) > 0:
                data_monitor_df.drop(columns=['std_syn', 'index'], inplace=True)
                df_to_combine.append(data_monitor_df)

        if "pharmaprojects" in df_list:
            pharma_df = self.standardize_pharma_diseases(disease_df)

            if len(pharma_df) > 0:
                pharma_df.drop(columns=['std_syn', 'index'], inplace=True)
                df_to_combine.append(pharma_df)



        # Merging all data

        #final_data = dqs_df.append([citeline_df, aact_df], ignore_index=True)
        # final_data = pharma_df.append([drg_df, globocan_df, data_monitor_df], ignore_index=True)

        temp_df = pd.DataFrame(columns=['unique_id', 'trial_id', 'datasource', 'disease'])
        #final_data = temp_df.append(df_to_combine)
        flag = 0

        if len(df_to_combine) > 0:
            flag = 1
            final_data = temp_df.append(df_to_combine)
        else:
            final_data = temp_df

        print("Merging data completed!!")

        print("Number of rows in final dataframe = %d" % final_data.shape[0])

        final_data.rename(columns={'disease': 'disease_name'}, inplace=True)
        final_data.rename(columns={'Standard Names': 'Standard_Names'}, inplace=True)

        #final_data.drop(columns=['unique_id', 'processed'], inplace=True)
        #final_data.rename(columns={'synonym': 'Synonyms', 'Standard_Names':'standard_disease', 'disease_name':'disease'}, inplace=True)

        if flag == 1 :
            final_data.drop(columns=['unique_id', 'processed'], inplace=True)
        final_data.rename(columns={'synonym': 'Synonyms', 'Standard_Names':'standard_disease', 'disease_name':'disease'}, inplace=True)

        final_data.to_csv("temp_disease_mapping.csv", index=False)

        s3_mapping_file_location = "s3://{}/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/disease_mapping.csv".format(s3_bucket_name)
        prev_mapping_file_location = "s3://{}/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/Archive/".format(s3_bucket_name)

        # creating backup of existing disease mapping file in archive folder
        file_copy_command = "aws s3 cp {} {} --sse".format(
                    os.path.join(s3_mapping_file_location),
                    os.path.join(prev_mapping_file_location, "disease_mapping_"+CURRENT_DATE+".csv"))
                #logger.info("Command to create backup of file on s3 - {}".format(file_copy_command))
        file_copy_status = os.system(file_copy_command)

        if file_copy_status != 0:
                raise Exception("Failed to create backup of disease mapping file on s3")

        # copying existing disease mapping file from s3 to current path
        os.system("aws s3 cp s3://{}/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/disease_mapping.csv ./".format(s3_bucket_name))


        existing_mapping_df  = pd.read_csv("disease_mapping.csv", error_bad_lines=False)

        # appending newly mapped diseases to existing mapping file
        merge_new_to_s3 = existing_mapping_df.append(final_data)

        if os.path.exists("disease_mapping.csv"):
            os.system('rm ' + "disease_mapping.csv")

        merge_new_to_s3.to_csv("disease_mapping.csv", index=False)
        merge_new_to_s3.drop_duplicates(subset=['Synonyms', 'standard_disease'], inplace=True)


        #final_data[
         #   ['unique_id', 'datasource', 'disease_name', 'processed', 'Standard_Names', 'synonym',
          #   'similarity', 'trial_id']].to_csv("final_disease_mapping_11092020.csv", index=False)

        print(
            "Output has been stored in CSV file disease_mapping.csv in your current working directory")

        print("Uploading latest disease mapping file to S3...")

        #latest_mapping_file_location = "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/"

        #file_copy_command = "aws s3 cp {} {} --sse".format(
         #           os.path.join(os.getcwd(), "disease_mapping_file.csv"),
          #          os.path.join(latest_mapping_file_location))

        #file_copy_status = os.system(file_copy_command)

        #if file_copy_status != 0:
         #       raise Exception("Failed to upload latest disease mapping file on s3")

        os.system('aws s3 cp disease_mapping.csv s3://{}/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/'.format(s3_bucket_name))



        final_time = time.time()

        print("Total Execution time: {} seconds".format(final_time - initial_time))







if __name__ == '__main__':
    Disease_std = DiseaseStandardization()

    Disease_std.main()

