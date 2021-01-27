#!/usr/bin/env python
# coding: utf-8

import httplib2
from googleapiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools
import argparse
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account
import configparser
from googleapiclient.errors import HttpError


class BigQueryGA(object):

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config_bigqueryGA.ini')  # path of your .ini file
        self.profile_id = config.get("DEFAULT", "profile_id")
        credentials = config.get("DEFAULT", "bq_credentials")
        project_id = config.get("DEFAULT", "project_id")
        bq_credentials = service_account.Credentials.from_service_account_file('./{}'.format(credentials))
        self.bigquery_client = bigquery.Client(credentials=bq_credentials, project=project_id)
        self.dataset_id = config.get("DEFAULT", "dataset_id")
        self.dataset_ref = self.bigquery_client.dataset(self.dataset_id)
        client_secrets = config.get("DEFAULT", 'CLIENT_SECRETS')
        token_file_name = config.get("DEFAULT", 'TOKEN_FILE_NAME')
        self.table_id = config.get("table", "table_id")
        self.dimensions = config.get("table", "dimensions")
        self.metric = config.get("table", "metric")
        self.table_ref = self.dataset_ref.table(self.table_id)
        self.TOKEN_FILE_NAME = token_file_name
        self.FLOW = flow_from_clientsecrets(
            client_secrets,
            scope='https://www.googleapis.com/auth/analytics.readonly',
            message='%s is missing' % client_secrets
        )

    @staticmethod
    def retry_if_connection_error(exception):
        """ Specify an exception you need. or just True"""
        # return True
        return isinstance(exception, HttpError)

    # creating bigquery data set
    def bg_create_dataset(self, dataset):
        try:
            self.bigquery_client.get_dataset(dataset)
        except NotFound:
            dataset = bigquery.Dataset(dataset)
            dataset = self.bigquery_client.create_dataset(dataset)
            print('Dataset {} create'.format(self.dataset_id))

    # Create table in bigquery inside dataset
    def bq_create_table(self, table):
        try:
            self.bigquery_client.get_table(table)
        except NotFound:
            table = bigquery.Table(table)
            table = self.bigquery_client.create_table(table)
            print('table {} created.'.format(self.table_id))

    def prepare_credentials(self):
        parser = argparse.ArgumentParser(parents=[tools.argparser])
        # flags = parser.parse_args()
        flags = parser.parse_known_args()
        # Retrieve existing credendials
        storage = Storage(self.TOKEN_FILE_NAME)
        credentials = storage.get()
        # If no credentials exist, we create new ones
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(self.FLOW, storage, flags)
        return credentials

    def initialize_service(self):
        # Creates an http object and authorize it using
        # the function prepare_credentials()
        http = httplib2.Http()
        credentials = self.prepare_credentials()
        http = credentials.authorize(http)
        # Build the Analytics Service Object with the authorized http object
        analytics = build('analytics', 'v3', http=http)
        return analytics

    def bq_load_data_in_bigquery(self, data_frame, dataset_id, table_id, table_ref):
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job_config.autodetect = True
        job_details = self.bigquery_client.load_table_from_dataframe(data_frame, table_ref,
                                                                     job_config=job_config)
        job_details.result()
        print("Loaded {} rows into {}:{}.".format(job_details.output_rows, dataset_id, table_id))


if __name__ == "__main__":
    bigquery_ga = BigQueryGA()
