#! python3
import pyodbc
import DBUtilities
import pandas
import os
import paramiko
import csv
from pathlib import Path
import posixpath
import os
from dw_logging import prnt as prnt, configure_logging, get_log_file, global_status_log
from datetime import datetime
from Creds import npd_feeds


configure_logging()
NPDdatafeed= npd_feeds()
username = NPDdatafeed.username
hostname = NPDdatafeed.hostname
password = NPDdatafeed.password

#######################################################################################################################


def data_extract_to_csv(query, sqlusername, local_dir_fq, use_pyodbc=True):
    DF = DBUtilities.query_data_return_pandas_df(query, sqlusername , use_pyodbc)
    DF.to_csv(local_dir_fq, header=True,  quotechar='"', quoting=csv.QUOTE_ALL, index =False)
    trailer_record = '3572'+str(datetime.today().strftime('%Y%m%d'))+'ROWCOUNT'+str(len(pandas.read_csv(local_dir_fq,low_memory=False)))+'DOORCOUNT1v6.0|||||||||||||||'
    with open(local_dir_fq, 'a') as fd:
        fd.write(trailer_record)
    prnt("Database file successfully exported")

#######################################################################################################################

def data_transfer_to_sftp_client(username, hostname, password, local_dir_fq, remote_dir_fq):

    with sftp.SFTPCon(username, hostname, password) as sftp_con:
        sftp_con.put(local_dir_fq, remote_dir_fq)
        prnt("Database file successfully exported to SFTP client")

#######################################################################################################################

@DWEmail.email_on_error(_log_fullpath=get_log_file())
@global_status_log()
def main():
    try:
        CUR_DIR = os.path.dirname(os.path.realpath(__file__))
        product_file = 'Product_3572_'+str(datetime.today().strftime('%Y%m%d'))+'.csv'
        location_file='Location_3572_'+str(datetime.today().strftime('%Y%m%d'))+'.csv'
        print(product_file)
        print(location_file)
        local_dir_product_fq = posixpath.join(Path().absolute(), product_file)
        local_dir_location_fq = posixpath.join(Path().absolute(), location_file)
        remote_dir_product_fq = posixpath.join('/data.in', product_file)
        remote_dir_location_fq = posixpath.join('/data.in',location_file)
        print('local_dir_product_fq: ',local_dir_product_fq)
        print('local_dir_location_fq: ',local_dir_location_fq)
        product_query = 'select * from datamart.dw.product_specs_vw'
        location_query = 'select * from datamart.dw.location_vw'
        sqlusername = 'Reporter'
        data_extract_to_csv(product_query, sqlusername,local_dir_product_fq)
        data_extract_to_csv(location_query, sqlusername,local_dir_location_fq)
        data_transfer_to_sftp_client(username, hostname, password, local_dir_product_fq, remote_dir_product_fq)
        data_transfer_to_sftp_client(username, hostname, password, local_dir_location_fq, remote_dir_location_fq)
    except Exception as e:
        raise
    finally:
        try:
            os.remove(local_dir_product_fq)
            os.remove(local_dir_location_fq)
        except  OSError as e:
            pass


#######################################################################################################################


if __name__ == '__main__':
        main()
        prnt("Script completed.")
