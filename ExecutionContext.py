#!/usr/bin/python
# -*- coding: utf-8 -*-
""" Module for Execution Context"""
__author__ = 'ZS Associates'

# ################################# Module Information ############################################
#   Module Name         : Execution Context
#   Purpose             : Contains class and functions used for storing and fetching execution
#                         context parameters  in all other python modules
#   How to run          : 1. Initiate an instance of class ExecutionContext
#                         2. Refer to different class components
#   Pre-requisites      : 1. import ExecutionContext class
#   Last changed on     : 12th August 2015
#   Last changed by     : Amol Kavitkar
#   Reason for change   : Removed PEP 8 errors and Pychecker errors
# #################################################################################################

import inspect
import os
import sys
sys.path.insert(0, os.getcwd())
from ConfigUtility import JsonConfigUtility

# all module level constants are defined here
MODULE_NAME = "Context Utility"
ENVIRONMENT_CONFIG_FILE = "log_setup.json"
LOG_MODULE_NAME = "LogSetup"


class ExecutionContext(object):
    """
    Class for Execution Context
    """
    def __init__(self):
        # fields needed for logging purpose
        self.context_dict = {}

        # dictionary required for the purpose of logging; containing key-value for all the fields
        # of the log
        configuration = JsonConfigUtility(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                       ENVIRONMENT_CONFIG_FILE))
        log_formatter_keys = configuration.get_configuration([LOG_MODULE_NAME,
                                                              "log_formatter_keys"])
        log_setup_json = configuration.get_configuration([LOG_MODULE_NAME])

        if log_formatter_keys is not None:
            # if log_formatter_keys exists and its not a list or blank list default formatter will
            # be created
            if len(log_formatter_keys) != 0 and isinstance(log_formatter_keys, list):
                for keys in log_formatter_keys:
                    if keys in log_setup_json:
                        self.context_dict.update({keys: log_setup_json[keys]})
                    else:
                        self.context_dict.update({keys: ""})

    def set_context(self, input_context_dict):
        """
        Method to set context
        :param input_context_dict:
        :return:
        """
        self.context_dict.update(input_context_dict)

    def get_context(self):
        """
        Method to get context
        :return:
        """
        temp_context_dict = {"function_name": inspect.stack()[1][3]}
        self.context_dict.update(temp_context_dict)
        return self.context_dict

    def get_context_param(self, param):
        """
        Method to get context parameters
        :param param:
        :return:
        """
        if param in self.context_dict:
            value = self.context_dict[param]
        else:
            value = ""
        return value
