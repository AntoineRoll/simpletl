from forepaas.dwh.connect import connect
import logging

logger = logging.getLogger(__name__)


def customfunc(event):

    logger.info("Begin function customfunc")
    # do your code here

    logger.info("END function customfunc")

    """
    ## Documentation
    Check-out the product documentation for a more complete presentation !
    * https://docs.forepaas.io/
    * https://docs.forepaas.io/#/en/technical/sdk/dpe/index

    For a complete documentation about the Forepaas-SDK-for-python check-out:
    * https://forepaas-sdk.readthedocs.io/en/latest/

    # A short example
    from forepaas.dwh.connect import connect
    from forepaas.dwh.connector import bulk_insert
    import sys
    import pandas as pd
    import logging


    logger = logging.getLogger(__name__)
    def customfunc(event):
        try:
            # create a connector to handle bucket
            bucket_connector = connect("data_store/my-bucket")

            # list files from bucket
            files = bucket_connector.list_filename(recursive=True)
            logger.info(files)

            # retrieve a file from Data Store bucket to temporary local folder
            bucket_filepath = "daily/tickets.csv"
            local_filepath = "/tmp/tickets.csv"
            bucket_connector.fget(bucket_filepath, local_filepath)

            # read then transform the file as you need
            df = pd.read_csv(local_filepath, sep=';')
            #do some stuff with dataframe ...

            # load the dataframe into a 'raw_ticket' table inside "prim" database
            cn = connect("dwh/data_prim/")
            stats = bulk_insert(cn, "raw_ticket", df)
            logger.info(stats)
            del cn

            # disconnect from datastore
            del bucket_connector
        except Exception as err:
            raise Exception(f"err:{err} L:{sys.exc_info()[2].tb_lineno}")
"""
