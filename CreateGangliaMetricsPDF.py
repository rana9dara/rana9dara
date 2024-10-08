"""Module for capturing ganglia metric snapshots."""
#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'
#####################################################Module Information#############################
#   Module Name         :   CreateGangliaMetricsPDF
#   Purpose             :   Module for capturing ganglia metric snapshots.
#   Input Parameters    :   cluster_id
#   Output Value        :   NA
#   Execution Steps     :   1. Trigger this module by passing cluster_id
#                           2. Only if either of the SAVE_GANGLIA_PDF_TO_S3 and
# SEND_GANGLIA_PDF_AS_ATTACHMENT flags are configured, capture ganglia
#                              metrics snapshots for each metric in the metric list
#                           3. Put the images in a pdf and save the pdf in local directory
#                           4. If the SAVE_GANGLIA_PDF_TO_S3 flag is configured,copies the pdf to an
#  s3 location
#                           5. If the SEND_GANGLIA_PDF_AS_ATTACHMENT flag is configured, send the
# pdf in an email as attachment
#   Predecessor module  :
#   Successor module    :       NA
#   Pre-requisites      :   The cluster should be running and the EMR version should support ganglia
#   Last changed on     :   10th May 2018
#   Last changed by     :   Neelakshi Kulkarni
#   Reason for change   :   Development for Ganglia PDF generation
#
####################################################################################################

# Library and external modules declaration

import sys
import traceback
import json
import shutil
import os
import glob
import re
from datetime import datetime
import pytz
from pytz import timezone
import boto3
from selenium import webdriver
from fpdf import FPDF
from PyPDF2 import PdfFileWriter, PdfFileReader
from ExecutionContext import ExecutionContext
from MySQLConnectionManager import MySQLConnectionManager
from LogSetup import logger
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from NotificationUtility import NotificationUtility

sys.path.insert(0, os.getcwd())





# Module Level constants are defined here
MODULE_NAME = "Create Ganglia Metrics PDFs"

##################Class CreateGangliaMetricsPDF############
# Class contains all the functions related to creating PDF files containing images for given set of
# Ganglia metrcis
###################################################################################################
class CreateGangliaMetricsPDF(object):
    """Class contains all the functions related to creating PDF files containing images for given
    set of Ganglia metrcis"""

    #Default constructor
    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(
            CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.s3_ganglia_folder_path = CommonConstants.S3_GANGLIA_FOLDER_PATH
        self.s3_ganglia_bucket = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_bucket_name"])
        self.ganglia_pdf_directory = CommonConstants.GANGLIA_PDF_DIRECTORY
        self.app_launcher_code_folder = CommonConstants.AIRFLOW_CODE_PATH
        self.ganglia_metric_list = CommonConstants.GANGLIA_METRIC_LIST
        self.phantomjs_service_log_path = CommonConstants.PHANTOMJS_SERVICE_LOG_PATH
        self.phantomjs_executable_path = CommonConstants.PHANTOMJS_EXECUTABLE_PATH
        self.save_ganglia_pdf_to_s3_flag = CommonConstants.SAVE_GANGLIA_PDF_TO_S3
        self.send_ganglia_pdf_as_attachment_flag = CommonConstants.SEND_GANGLIA_PDF_AS_ATTACHMENT
        self.create_ts = datetime.now(tz=pytz.utc).astimezone(
            timezone("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M:%S")
        self.execution_context = ExecutionContext()
        # Instantiating MySQLConnectionManager to interact with MySQL database
        self.mysql_connector = MySQLConnectionManager()
    ############################################## capture_ganglia_metrics #########################
    # Purpose            :   This method will capture ganglia metrics
    # Input              :
    # Output             :   consolidated pdf on s3 with a screenshot of every ganglia metric
    # captured
    ################################################################################################

    def capture_ganglia_metrics(self, cluster_id, master_node_dns):
        """This method will capture ganglia metrics"""
        result_dictionary = {CommonConstants.STATUS_KEY: CommonConstants.STATUS_SUCCEEDED}
        status_message = ""
        try:
            status_message = "Started function to capture Ganglia metrics"
            logger.debug(status_message, extra=self.execution_context.get_context())
            self.ganglia_pdf_directory = os.path.join(self.ganglia_pdf_directory, str(cluster_id))
            if self.send_ganglia_pdf_as_attachment_flag == "Y" or \
                    self.save_ganglia_pdf_to_s3_flag == "Y":
                os.chdir(self.app_launcher_code_folder)
                if os.path.isdir(self.ganglia_pdf_directory):
                    pass
                else:
                    os.makedirs(self.ganglia_pdf_directory)

                status_message = "Created directory on Launcher, capturing images"
                logger.debug(status_message, extra=self.execution_context.get_context())

                #pdf_process_name = process_name.lower().replace(" ", "")
                pdf_file_name_temp = str(self.create_ts)+str(master_node_dns)+str(cluster_id)
                pdf_file_name = re.sub('[^a-zA-Z0-9\n\.]', '_', pdf_file_name_temp)

                process_pdf_file_dir = self.app_launcher_code_folder.rstrip("/")+ "/" + \
                                       self.ganglia_pdf_directory

                if not os.path.isdir(process_pdf_file_dir):
                    os.makedirs(process_pdf_file_dir)

                os.chdir(process_pdf_file_dir)
                ganglia_metric_list = self.ganglia_metric_list.split(',')

                status_message = "Capturing Ganglia metrics screen shot for configured metrics"
                logger.debug(status_message, extra=self.execution_context.get_context())

                for metric in ganglia_metric_list:
                    metric = metric.strip()
                    metric_addr = "http://"+\
                                  master_node_dns+\
                                  "/ganglia/?r=day&cs=&ce=&/&h=&tab=m&vn=&hide-hf=false&m="+metric+\
                                  "&sh=1&z=small&hc=4&host_regex=&max_graphs=0&s=by+name"

                    status_message = "Metric: "+ metric + ", metric_addr: "+ metric_addr
                    logger.debug(status_message, extra=self.execution_context.get_context())

                    driver = webdriver.PhantomJS(service_log_path=str(
                        self.phantomjs_service_log_path).rstrip(
                            "/")+"/"+CommonConstants.PHANTOMJS_LOG_FILE_NAME,
                                                 executable_path=str(
                                                     self.phantomjs_executable_path).rstrip(
                                                         "/")+"/"+"phantomjs")
                    driver.get(metric_addr)
                    driver.save_screenshot(pdf_file_name + '-' + metric + '.png')
                status_message = "Creating temporary PDF for metrics"
                logger.debug(status_message, extra=self.execution_context.get_context())


                pdf = FPDF(format="A4")
                image_list = glob.glob("*.png")
                for image in image_list:
                    pdf.add_page()
                    pdf.set_font("Arial", size=12)
                    text = "".join(image.split('.')[:-1]).split('-')[-1]
                    text = "Metric: " + text + "  cluster_id: " + str(cluster_id) + "  " + \
                           self.create_ts
                    pdf.cell(10, 10, txt=text, align="L", ln=1)
                    pdf.image(image, w=150, h=250)

                pdf.output("temp.pdf")

                status_message = "Creating final PDF file for Ganglia Metrics"
                logger.debug(status_message, extra=self.execution_context.get_context())


                if os.path.isfile(pdf_file_name + '.pdf'):
                    os.rename(pdf_file_name + '.pdf', "temp_prev.pdf")
                    output = PdfFileWriter()
                    self.append_pdf(PdfFileReader(file("temp_prev.pdf", "rb")), output)
                    self.append_pdf(PdfFileReader(file("temp.pdf", "rb")), output)
                    output.write(file(pdf_file_name + '.pdf', "wb"))
                else:
                    os.rename("temp.pdf", pdf_file_name + '.pdf')

                status_message = "Final PDF created"
                logger.debug(status_message, extra=self.execution_context.get_context())

            if self.save_ganglia_pdf_to_s3_flag == 'Y':

                s3 = boto3.resource('s3')
                source_file_path = str(process_pdf_file_dir)+"/"+str(pdf_file_name)+".pdf"
                target_file_path = str(self.s3_ganglia_folder_path)+"/"+str(pdf_file_name)+".pdf"
                status_message = "Source path: " + source_file_path + " and Target path:" + \
                                 target_file_path
                logger.debug(status_message, extra=self.execution_context.get_context())

                s3.meta.client.upload_file(source_file_path, self.s3_ganglia_bucket,
                                           target_file_path)

                status_message = "PDF Copied to S3, Cleaning temporary directory"
                logger.debug(status_message, extra=self.execution_context.get_context())

                status_message = "Ganglia screen shots captured and stored in S3"
                logger.debug(status_message, extra=self.execution_context.get_context())
                result_dictionary[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCEEDED

            if self.send_ganglia_pdf_as_attachment_flag == 'Y':
                status_message = "Starting to send ganglia report pdf as attachment"
                logger.info(status_message, extra=self.execution_context.get_context())
                os.chdir(process_pdf_file_dir)

                pdf_file_name_with_extn = str(pdf_file_name)+".pdf"

                configuration = JsonConfigUtility(
                    os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                 CommonConstants.ENVIRONMENT_CONFIG_FILE))

                email_types = configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations"])
                email_type = "ganglia_report"
                email_template_path = configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_template_path"])
                status_message = "Email template path retrieved from Configurations is: " + \
                                 str(email_template_path)
                logger.info(status_message, extra=self.execution_context.get_context())

                email_template_name = configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "email_type_configurations",
                     str(email_type), "template_name"])
                status_message = "Email template name retrieved from Configurations is: " +\
                                 str(email_template_name)
                logger.info(status_message, extra=self.execution_context.get_context())

                email_template_path = (email_template_path + "/" +
                                       email_template_name).replace("//", "/")
                status_message = "Email template path is: " + str(email_template_path)
                logger.info(status_message, extra=self.execution_context.get_context())

                if email_types:
                    email_types = list(email_types.keys())
                    if email_type in email_types:
                        status_message = "Email type: " + str(email_type) + " is configured"
                        logger.info(status_message, extra=self.execution_context.get_context())
                    else:
                        status_message = "Email type: " + str(email_type) +\
                                         " is not configured, cannot proceed"
                        logger.info(status_message, extra=self.execution_context.get_context())
                        raise Exception("Email type: " + str(email_type) +
                                        " is not configured, cannot proceed")
                else:
                    raise Exception("No email types configured, cannot proceed")

                email_ses_region = configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "ses_region"])
                status_message = "SES Region retrieved from Configurations is: " +\
                                 str(email_ses_region)
                logger.info(status_message, extra=self.execution_context.get_context())

                email_sender = configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY,
                     "email_type_configurations", str(email_type), "ses_sender"])
                status_message = "Email Sender retrieved from Configurations is: " +\
                                 str(email_sender)
                logger.info(status_message, extra=self.execution_context.get_context())

                email_recipient_list = configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY,
                     "email_type_configurations", str(email_type), "ses_recipient_list"])
                status_message = "Email Recipient retrieved from Configurations is: " +\
                                 str(email_recipient_list)
                logger.info(status_message, extra=self.execution_context.get_context())

                email_subject = configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY,
                     "email_type_configurations", str(email_type), "subject"])
                status_message = "Email Subject retrieved from Configurations is: " +\
                                 str(email_subject)
                logger.info(status_message, extra=self.execution_context.get_context())

                replace_variables = {}
                replace_variables["master_node_dns"] = str(master_node_dns)
                replace_variables["env"] = configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "environment"])
                status_message = "replace_variables: " + str(replace_variables)
                logger.info(status_message, extra=self.execution_context.get_context())
                email_body = ""
                notif_object = NotificationUtility()
                notif_object.send_notif_with_pdf_attachment(
                    email_ses_region, email_subject, email_sender, email_recipient_list,
                    pdf_file_name_with_extn, email_body, email_template_path, replace_variables)
                status_message = "Ganglia screen shots captured and pdf sent as attachment"
                logger.debug(status_message, extra=self.execution_context.get_context())

            #CleanUp
            if self.send_ganglia_pdf_as_attachment_flag == "Y" or \
                    self.save_ganglia_pdf_to_s3_flag == "Y":
                os.chdir(self.app_launcher_code_folder)
                if os.path.isdir(process_pdf_file_dir):
                    shutil.rmtree(process_pdf_file_dir)

            status_message = "Completed function to capture Ganglia metrics"
            logger.debug(status_message, extra=self.execution_context.get_context())
            return result_dictionary
        except Exception as exception:
            print((str(exception)))
            status_message = "Capturing ganglia metrics failed for cluster:" + cluster_id
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            raise exception

    ################################################# append_pdf ###################################
    # Purpose            :   This method will append one pdf to another
    # Input              :   one input pdf
    # Output             :   A new pdf
    ################################################################################################

    def append_pdf(self, input, output):
        """This method will append one pdf to another"""
        try:
            [output.addPage(input.getPage(page_num)) for page_num in range(input.numPages)]

        except Exception as exception:
            print((str(exception)))
            status_message = "Execution failed while appending pdf "
            error = "ERROR in " + self.execution_context.get_context_param("current_module") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc())
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())

    def main(self, cluster_id=None):
        """This method generates ganglia depending on the flag configured."""
        status_message = ""
        result_dictionary = {}
        try:
            status_message = "Starting the main function for CreateGangliaMetricsPDF"
            logger.info(status_message, extra=self.execution_context.get_context())
            result_dictionary[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCEEDED
            if cluster_id is None:
                raise Exception("Cluster id is not provided")
            #Logic to generate ganglia reports depending on the flag configured
            if(self.send_ganglia_pdf_as_attachment_flag == 'Y'
               or self.save_ganglia_pdf_to_s3_flag == 'Y'):
                #getting master_node_dns from log_cluster_dtl table using cluster_id
                ganglia_query = "select master_node_dns from log_cluster_dtl where cluster_id='" + \
                                cluster_id +"' and cluster_status='"+CommonConstants.EMR_WAITING+"'"

                status_message = "Fetching cluster details to capture Ganglia metrics "
                logger.debug(status_message, extra=self.execution_context.get_context())
                ganglia_result = MySQLConnectionManager().execute_query_mysql(ganglia_query)
                logger.debug(ganglia_result, extra=self.execution_context.get_context())
                #if a single row is returned, call function to generate pdfs else give an error
                if len(ganglia_result) == 1:
                    master_node_dns = ganglia_result[0]['master_node_dns']

                    #calling function to capture Ganglia metrics
                    result_dictionary = self.capture_ganglia_metrics(cluster_id, master_node_dns)

                else:
                    status_message = "Found zero or more than one clusters with same details"
                    logger.error(status_message, extra=self.execution_context.get_context())
                    result_dictionary[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED

            if result_dictionary[CommonConstants.STATUS_KEY] == CommonConstants.STATUS_FAILED:
                raise Exception
            status_message = "Completing the main function for CreateGangliaMetricsPDF"
            logger.info(status_message, extra=self.execution_context.get_context())
            return result_dictionary

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as exception:
            error = "ERROR in " + self.execution_context.get_context_param("module_name") + \
                    " ERROR MESSAGE: " + str(traceback.format_exc() + str(exception))
            self.execution_context.set_context({"traceback": error})
            logger.error(status_message, extra=self.execution_context.get_context())
            self.execution_context.set_context({"traceback": ""})
            raise exception

if __name__ == '__main__':
    cluster_id = sys.argv[1]
    create_ganglia_report_obj = CreateGangliaMetricsPDF()
    RESULT_DICT = create_ganglia_report_obj.main(cluster_id)
    STATUS_MSG = "\nCompleted execution for CreateGangliaMetricsPDF Utility with status " + \
                 json.dumps(RESULT_DICT) + "\n"
    sys.stdout.write(STATUS_MSG)

