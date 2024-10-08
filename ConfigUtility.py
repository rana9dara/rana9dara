
"""Module for Configuration Utility"""
#!/usr/bin/python
# -*- coding: utf-8 -*-


__author__ = 'ZS Associates'

# ################################################## Module Information ############################
#   Module Name         :   ConfigUtility
#   Purpose             :   This is used for reading values from configuration file
#   Input Parameters    :   Configuration file, Section in the configuration file and configuration
#                           name.

#   Output Value        :   This utility will return the value corresponding to a configuration
#                           parameter in the configuration file
#   Execution Steps     :   1.Import this class in the class where we need to read values from a
#                           configuration file.
#                           2.Instantiate this class
#                           3.Pass the configuration file as input to get_configuration() method
#   Predecessor module  :   All modules which reads values from configuration files
#   Successor module    :
#   Pre-requisites      :
#   Last changed on     :   22nd January 2019
#   Last changed by     :   Nishant
# Reason for change     :   Python2 to 3
# ##################################################################################################

# Define all module level constants here
import os
import sys
import json
#from functools import reduce
from configparser import SafeConfigParser
from functools import reduce
sys.path.insert(0, os.getcwd())


MODULE_NAME = "ConfigUtility"

# ################################ Class JsonConfigUtility #########################################
# Class contains all the functions related to JsonConfigUtility
# ##################################################################################################

class JsonConfigUtility(object):
    """Class for Json Configuration Utility"""
    # Parametrized constructor with Configuration file as input
    def __init__(self, conf_file=None, conf=None):
        try:
            if conf_file is not None:
                config_fp = open(conf_file)
                self.json_data =  json.load(config_fp)
                self.json_configuration = json.load(config_fp)
            elif conf is not None:
                self.json_configuration = conf
            else:
                self.json_configuration = {}
        except:
            pass

# ############################################### Get Configuration ############################
#   Purpose : This method will read the value of a configuration parameter corresponding to a
#             section in the configuration file
#
#   Input   : Section in the configuration file, Configuration Name
#
#   Output  : Returns the value of the configuration parameter present in the configuration file
# ##############################################################################################
    def get_configuration(self, conf_hierarchy):
        """Method to get the configuration"""
        try:
            return reduce(lambda dictionary, key: dictionary[key], conf_hierarchy,
                          self.json_data)
        except:
            pass

##################Class ConfigUtility#####################################
# Class contains all the functions related to ConfigUtility
###########################################################################

class ConfigUtility(object):
    """ Class for configuration utility"""

    #Instantiating SafeConfigParser()
    parser = SafeConfigParser()

    #Parametrized constructor with Configuration file as input
    def __init__(self, conf_file):
        try:
            self.parser.read(conf_file)
        except:
            pass


################################################# Get Configuration ############################
# Purpose  :  This method will read the value of a configuration parameter corresponding to a
#             section in the configuration file
#
# Input    :  Section in the configuration file, Configuration Name
#
# Output   :  Returns the value of the configuration parameter present in the configuration file
################################################################################################
    def get_configuration(self, conf_section, conf_name):
        """Method to get the configuration"""
        try:
            return self.parser.get(conf_section, conf_name)
        except:
            pass

