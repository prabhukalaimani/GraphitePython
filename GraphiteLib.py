#!/usr/bin/python
################################################
# Author: Prabhu Kalaimani
# Ref: https://graphite.readthedocs.io/en/latest/feeding-carbon.html 
################################################
"""
The purpose of the library is to exercise grafana.
Note: You need to have a proper grafana and graphite setup before you use this code
"""
# Required python imports
import socket
import time
import pickle
import struct


class GraphiteLib(object):
    """
    This class will have all the methods to post data to grafite database.
    The data posted will be used by grafana to
    post in the dashboards.
    """
    def __init__(self, server_address, port_num):
        """
        Initialize the graphite
        """
        self.carbon_server = server_address
        self.carbon_port = port_num
        self.connection_status = True
        self.graphite_soc = ""

        print("Initializing the graphite server {} on port {}".format(self.carbon_server, self.carbon_port))
        # connect to the carbon server and check if the ip is valid
        try:
            self.graphite_soc = socket.socket()
            self.graphite_soc.connect((self.carbon_server, self.carbon_port))
        except socket.gaierror:
            print("Please check the host name / Ip address {}".format(self.carbon_server))
            self.connection_status = False
        except socket.error:
            print("Could not connect to carbon server {}. Check if the carbon server is running".format(self.carbon_server))
            self.connection_status = False

        if self.connection_status:
            print("Successfully connected to carbon server {} @ port number {}".format(self.carbon_server, self.carbon_port))
        else:
            exit(1)

    def format_data_with_timeinfo(self, data):
        """
        This method will add time stamp to the data and returns the formatted string.
        Format : data timestamp
        :param data: data timestamp
        Example : 77 1234423
        :return: String with data and the time
        """
        # Get the time information
        current_time = int(time.time())
        # Get the integer value for the current time
        formated_data = '%s %d' % (data, current_time)
        print("Formated Data: {}".format(formated_data))
        return formated_data

    def create_metric_data(self, metric_str, data):
        """
        This method will join the metric and the data which is required for the graphite database
        :param metric_str: metric string ( example Wham.Jira.Rio.Critical)
        :param data: data to be appended to metric string
        :return: status(Boolean), metric data required by graphite
        example: True, 'Wham.jira.Rio.Critical 40 1539872837'
        """
        status = False
        metric_data = ""
        # Check if metric data is empty
        if metric_str:
            print("Metric string cannot be empty")
        else:
            tmp_data = self.format_data_with_timeinfo(data)
            # add up the metric information and the data together
            metric_data = "%s %s\n" % (metric_str, tmp_data)
            print("metric data: {}".format(metric_data))
            status = True
        return status, metric_data

    def insert_plaintext_data(self, data):
        """
        This method will post plain text results to Graphite.
        Data Format: < Metric path> <Metric value> <Time stamp>
        :param data: Plain data which needs to be posted to graphite server
        :return: None
        """
        # check if the data is empty
        if data:
            # Double check if the socket was established successfully.
            if self.connection_status:
                print("Executing insert plain text with data: {}".format(data))
                self.graphite_soc.sendall(data.encode())
            else:
                print("Connection to the carbon server was not established or lost")
        else:
            print("You cannot send empty data...please check the data your sending")

    def format_pickle_data(self, path, data):
        """
        This function will format the data in picle protocol as show below
        [(path, (timestamp, value)), ...]
        :param path: This is the string of the metric path ( Example: jira.wham.rio)
        :param data: data which needs to be formatted to pickle protocol format
        :return: Tuple containing the pickle  formatted data
        """
        # Make sure the data is not empty
        if data:
            # Create the pickle data
            print("There is no data passed. Please pass data as lists")
            # Get the time using arrow
            curr_time = int(time.time())
            metric_data = [(path, (curr_time, data))]
            print("Metric data = {}".format(metric_data))
            # create a pickle dump(encoding)
            payload = pickle.dumps(metric_data, protocol=2)
            header = struct.pack("!L", len(payload))
            insert_data = header + payload
            # Insert the data to the graphite database
            if self.connection_status:
                print("Executing insert with pickle format data: {}".format(metric_data))
                self.graphite_soc.sendall(insert_data)
            else:
                print("Connection to the carbon server was not established or lost")
        else:
            print("You cannot send empty data...please check the data your sending")



    def insert_pickle_data(self, data):
        """
        This method will insert data using pickle formatting. Pickle is a series of tuple data
        Data Format : [(path,(timestamp, value)),.....(path,(timestamp, value)) ]
        With pickle you can insert multiple data at the same time
        :param data:
        :return:
        """
        #[(path, (timestamp, value)), ...]
        status = False
        print("Calling insert_pickle_data with  {}".format(data))
        return status
