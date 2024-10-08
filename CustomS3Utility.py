#!/usr/bin/python
# -*- coding: utf-8 -*-
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import json
import traceback
import logging
from os.path import splitext
from io import StringIO
from urllib.parse import urlparse

import boto3
import CommonConstants as CommonConstants

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

MODULE_NAME = "CustomS3Utility"


class CustomS3Utility():

    def __init__(self):
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource('s3')
        self.logger = logging.getLogger(MODULE_NAME)

    def get_folder_size(self, input_path):
        """
        Purpose: Iterate over folders and get the size of the files
        :param input_path: input folder path for the output
        """
        try:
            response = self._s3_list_files(input_path)
            file_list = response[CommonConstants.RESULT_KEY]
            total_size = 0
            for file in file_list:
                # self.logger.info("Started fetching the files in the s3 path: %s" % file)
                url = urlparse(file)
                bucket = url.netloc
                prefix = url.path.lstrip('/').rstrip('/')
                response = self.s3_client.head_object(
                    Bucket=bucket,
                    Key=prefix
                )
                total_size = total_size + response['ContentLength']
            # self.master_dict[input_path] = total_size
            return total_size
        except Exception as ex:
            error = "Failed to process the Json. ERROR: %s" % str(ex)
            self.logger.error(error)
            raise ex

    def get_file_size(self, file_path):
        try:
            url = urlparse(file_path)
            bucket = url.netloc
            prefix = url.path.lstrip('/').rstrip('/')
            response = self.s3_client.head_object(
                Bucket=bucket,
                Key=prefix
            )
            file_size = response['ContentLength']
            return file_size
        except Exception as e:
            self.logger.error(traceback.print_exc())
            raise e

    def delete_all_files_from_folder(self, bucket_name, key):
        try:

            bucket = self.s3_resource.Bucket(bucket_name)
            bucket.objects.filter(Prefix=key).delete()
            self.logger.info("all keys deleted from path {path}".format(path=key))
        except Exception as e:
            self.logger.error(traceback.print_exc())
            raise e

    def _read_json_line_file_from_s3(self, bucket_name, key):
        response = dict()
        try:
            self.logger.info(
                "Started function to read the JSON file from s3 for bucket " + bucket_name + "and key " + key)
            s3_client_object = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            s3_object_data = s3_client_object["Body"].read().decode("utf-8")
            json_line_data_list = [json.loads(json_line) for json_line in s3_object_data.split('\n')]
            self.logger.info("Successfully read the JSON file in s3")
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            response[CommonConstants.RESULT_KEY] = json_line_data_list
            return response
        except Exception as ex:
            error = "Failed to read s3 file. ERROR - " + str(ex)
            self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.ERROR_KEY] = error
            return response

    def _read_config_file_from_s3(self, bucket_name, key):
        response = dict()
        try:
            self.logger.info("Started function to read the JSON file from s3")
            s3_client_object = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            s3_object_data = s3_client_object["Body"].read().decode("utf-8")
            self.logger.info("Successfully read the JSON file in s3")
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            response[CommonConstants.RESULT_KEY] = s3_object_data
            return response
        except Exception as ex:
            error = "Failed to read s3 file. ERROR - " + str(ex)
            self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.ERROR_KEY] = error
            return response

    def _read_json_in_s3(self, bucket_name, key):
        response = dict()
        try:
            self.logger.info("Started function to read the JSON file from s3")
            s3_client_object = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            s3_object_data = s3_client_object["Body"].read().decode("utf-8")
            json_data = json.loads(s3_object_data)
            self.logger.info("Successfully read the JSON file in s3")
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            response[CommonConstants.RESULT_KEY] = json_data
            return response
        except Exception as ex:
            error = "Failed to read s3 file. ERROR - " + str(ex)
            self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.ERROR_KEY] = error
            return response

    def _read_code_file_from_s3(self, bucket_name, key):
        response = dict()
        try:
            self.logger.info("Started function to read the Code file from s3")
            s3_client_object = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            s3_object_data = s3_client_object["Body"].read().decode("utf-8")
            self.logger.info("Successfully read the Code file in s3")
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            response[CommonConstants.RESULT_KEY] = s3_object_data
            return response
        except Exception as ex:
            error = "Failed to read s3 file. ERROR - " + str(ex)
            self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.ERROR_KEY] = error
            return response

    def _write_json_to_s3(self, bucket_name, key, data):
        response = dict()
        try:
            self.logger.info("Started function to upload the JSON to s3")
            self.logger.info("bucket_name===>>>" + str(bucket_name))
            self.logger.info("key====>>" + str(key))
            self.logger.info("data===>>>" + str(data))
            serialized_data = json.dumps(data)
            upload_data_response = self.s3_client.put_object(Bucket=bucket_name,
                                                             Key=key,
                                                             Body=serialized_data)
            if upload_data_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                self.logger.info("Successfully uploaded the file to s3")
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
                response[CommonConstants.RESULT_KEY] = ""
                return response
            else:
                error = "Failed to write the file to s3"
                self.logger.error(error)
                response[CommonConstants.STATUS] = CommonConstants.STATUS_FAILED
                response[CommonConstants.ERROR_KEY] = error
                return response
        except Exception as ex:
            error = "Failed to write dict to s3 file. ERROR - " + str(ex)
            self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.ERROR_KEY] = error
            return response

    def _write_csv_to_s3(self, bucket_name, key, dataframe):
        response = dict()
        try:
            self.logger.info("Started function to write the CSV to s3")
            s3_resource = boto3.resource('s3')
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer, index=False)
            upload_data_response = s3_resource.Object(bucket_name, key).put(Body=csv_buffer.getvalue())
            if upload_data_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                self.logger.info("Successfully uploaded the file to s3")
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
                response[CommonConstants.RESULT_KEY] = ""
                return response
            else:
                error = "Failed to write the file to s3"
                self.logger.error(error)
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
                response[CommonConstants.ERROR_KEY] = error
                return response
        except Exception as ex:
            error = "Failed to write dict to s3 file. ERROR - " + str(ex)
            self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.ERROR_KEY] = error
            return response

    def _s3_list_files(self, file_path, suffix=''):
        response = dict()
        try:
            self.logger.info("Started fetching the files in the s3 path: %s" % file_path)
            url = urlparse(file_path)
            bucket = url.netloc
            prefix = url.path.lstrip('/').rstrip('/')
            response = self.s3_client.list_objects(
                Bucket=bucket,
                Prefix=prefix
            )
            file_names = list()
            if response['Contents']:
                for file in response['Contents']:
                    if file['Key'].endswith("/"):
                        continue
                    if suffix:
                        if file['Key'].endswith(suffix):
                            file_names.append(file['Key'])
                    else:
                        file_names.append(file['Key'])
            if file_names:
                for i in range(len(file_names)):
                    file_names[i] = CommonConstants.S3_PATH_PREFIX + bucket + CommonConstants.SEPARATOR + file_names[i]
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
            response[CommonConstants.RESULT_KEY] = file_names
            return response
        except Exception as ex:
            error = "Failed to list files in the s3 path. ERROR - " + str(ex)
            self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.ERROR_KEY] = error
            return response

    def _get_folder_extensions(self, directory):
        response = dict()
        try:
            ext_list = list()
            for filename in self._s3_list_files(file_path=directory)[CommonConstants.RESULT_KEY]:
                title, ext = splitext(filename)
                ext_list.append(ext.replace('.', ''))
            if len(set(ext_list[1:])) == 1:
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_SUCCESS
                response[CommonConstants.RESULT_KEY] = ext_list.pop()
                return response
            else:
                error = "Cannot process the files as file type is invalid."
                self.logger.error(error)
                response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
                response[CommonConstants.ERROR_KEY] = error
        except Exception as ex:
            error = "Failed to get the file extensions. ERROR - " + str(ex)
            self.logger.error(error)
            response[CommonConstants.STATUS_KEY] = CommonConstants.STATUS_FAILED
            response[CommonConstants.ERROR_KEY] = error
            return response

    def list_s3_objects(self, s3_object_name=None):
        try:
            s3Bucket = s3_object_name.replace("s3://", "").split("/")[0]
            s3Prefix = s3_object_name.replace("s3://", "").replace(s3Bucket, "")[1::]

            response = self.s3_client.list_objects(
                Bucket=s3Bucket,
                Prefix=s3Prefix
            )

            keys = []
            if response.get('Contents'):
                for key in response['Contents']:
                    keys.append(key['Key'])
            return keys

        except Exception as ex:
            error = "Failed to list objects from S3: %s" % str(ex)
            self.logger.error(error)
            raise ex

    def delete_s3_objects_recursively(self, s3_object_name=None):
        try:
            s3Bucket = s3_object_name.replace("s3://", "").split("/")[0]
            s3Key = s3_object_name.replace("s3://", "").replace(s3Bucket, "")[1::]

            sourceFiles = self.list_s3_objects(s3_object_name)
            if len(sourceFiles) != 0:
                for sourceFile in sourceFiles:
                    response = self.s3_client.delete_object(
                        Bucket=s3Bucket,
                        Key=sourceFile
                    )
            else:
                self.logger.info("S3 Location is empty")

            return "Objects Deleted"

        except Exception as ex:
            error = "Failed to delete object from S3: %s" % str(ex)
            self.logger.error(error)
            raise ex

    def copy_s3_to_s3_recursively(self, s3_source_location=None, s3_target_location=None):
        try:
            sourceBucket = s3_source_location.replace("s3://", "").split("/")[0]
            sourceKey = s3_source_location.replace("s3://", "").replace(sourceBucket, "")[1::]
            destinationBucket = s3_target_location.replace("s3://", "").split("/")[0]
            destinationKey = s3_target_location.replace("s3://", "").replace(destinationBucket, "")[1::]

            sourceFiles = self.list_s3_objects(s3_source_location)
            if len(sourceFiles) != 0:
                for sourceFile in sourceFiles:
                    response = self.s3_client.copy_object(
                        Bucket=destinationBucket,
                        # Key=destinationKey + sourceFile[sourceFile.rfind("/")+1:],
                        Key=destinationKey + sourceFile.replace(sourceKey, ""),
                        CopySource={
                            'Bucket': sourceBucket,
                            'Key': sourceFile
                        }
                    )

                return "Objects Copied"
            else:
                self.logger.info("S3 Location is empty")

        except Exception as ex:
            error = "Failed to Copy object from S3 to S3: %s" % str(ex)
            self.logger.error(error)
            raise ex

    def move_s3_to_s3_recursively(self, s3_source_location=None, s3_target_location=None):
        try:
            self.copy_s3_to_s3_recursively(s3_source_location, s3_target_location)
            self.delete_s3_objects_recursively(s3_source_location)
            return "Objects Moved"

        except Exception as ex:
            error = "Failed to move object from S3 to S3: %s" % str(ex)
            self.logger.error(error)
            raise ex
