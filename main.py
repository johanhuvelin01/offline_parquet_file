import os, yaml

from datetime import datetime
import dateutil

from operator import attrgetter

import boto3


def connection_S3(region_name, aws_access_key_id, aws_secret_access_key):
    """
        This function create connection with S3 buckets 
        
        params:
        - url: url of the S3 buckets
    """
    #check if connection is available or not
    #check our rights
    
    s3 = boto3.resource(
        service_name ='s3',
        region_name = region_name,
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key
    ) 

    return s3

def calculate_bucket_size(bucket_object):
    """
        This function calculate the bucket size

    Returns:
        _type_: _description_
    """
    
    return sum(a.size for a in bucket_object)/1000/1024/1024

    
def check_format_file(bucket_object):
    """
        This function tell the format of the table ['PARQUET', 'DELTA', 'CSV', 'NA']
    """
    if any('_delta_log' in obj.key for obj in bucket_object):
        data_format = 'delta'
    else:
        data_format = 'parquet'
                
    return data_format

def find_last_parquet(bucket_object):
    t =  max(bucket_object, key=attrgetter('last_modified'))
    return t

def filter_bucket_object(bucket_object):
    """_summary_

    Args:
        bucket_object (_type_): _description_

    Returns:
        _type_: _description_
    """
    sample = [x for x in bucket_object if '.parquet' in x.key]
    return sample

def check_partition_year_month(bucket_object):
    """_summary_

    Args:
        bucket_object (_type_): _description_

    Returns:
        _type_: _description_
    """
    partition_year_month = False
    
    if any('year=' in obj.key for obj in bucket_object):
        partition_year_month = True
    
    return partition_year_month


def sample_partition_year_month(table, conf, s3_co, my_bucket):
    """_summary_

    Args:
        table (_type_): _description_
        conf (_type_): _description_
        s3_co (_type_): _description_
        my_bucket (_type_): _description_

    Returns:
        _type_: _description_
    """
    sampling_data = []
    for i in range(0, conf["input"]["params"]["sample"]["nb_month_to_sample"]-1):
        year = (datetime.now() + dateutil.relativedelta.relativedelta(months=-i)).year
        month = (datetime.now() + dateutil.relativedelta.relativedelta(months=-i)).month
        sampling_data = sampling_data + list(s3_co.Bucket(my_bucket['name']).objects.filter(Prefix=table +f'/year={year}/month={month}'))
    return sampling_data

def save_data(data_object, conf, s3_co, my_bucket):
    """
        This function save the parquet
    """
    for object in data_object:
        #Create directory if not exist
        local_folder = os.path.dirname(f'{conf["output"]["folder"]}/{object.key}')
        if not os.path.exists(local_folder):
            os.makedirs(local_folder)
        if os.path.basename(f'{conf["output"]["folder"]}/{object.key}') != '':
            s3_co.Bucket(my_bucket['name']).download_file(object.key, f'{conf["output"]["folder"]}/{object.key}')
    return
                
def load_conf(conf):
    """_summary_

    Args:
        conf (_type_): _description_

    Returns:
        _type_: _description_
    """
    with open(conf) as fh:
        dictionary_data = yaml.safe_load(fh)
    
    return dictionary_data
    
    
#MAIN FUNCTION
def main():

    #TODO unitest
    
    #Get conf
    conf = load_conf(conf=conf_file)
    
    # Get env variable about aws
    region_name = os.environ["AWS_DEFAULT_REGION"]
    aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    
    # Init the S3 connection
    s3_co = connection_S3(region_name, aws_access_key_id, aws_secret_access_key)
    
    for my_bucket in conf['input']['buckets']:
        for table in my_bucket['tables']:
            print(f'table {table} en cours de traitement')

            # Get objects from bucket
            bucket_object = list(s3_co.Bucket(my_bucket['name']).objects.filter(Prefix=table +'/'))
            if len(bucket_object) == 0 :
                print(f'La table {table} n\'est pas présente')
            # Get size of data, will use for sampling or not
            size_giga = calculate_bucket_size(bucket_object)
            # Check if contains partition by year and month
            partition_year = check_partition_year_month(bucket_object)
            
            # Sampling with partition
            if size_giga > conf["input"]["params"]["sample"]["size_max_to_sample"]:
                if partition_year == True:
                    sampling_data = sample_partition_year_month(table=table, conf=conf, s3_co=s3_co, my_bucket=my_bucket)
                    size_giga_sampling = calculate_bucket_size(sampling_data)
                    if size_giga_sampling < conf["input"]["params"]["sample"]["size_max_to_sample"]:
                        save_data(data_object=sampling_data, conf=conf, s3_co=s3_co, my_bucket=my_bucket)
                    else:
                        # Get the last X
                        bucket_object.sort(key=lambda x: x.last_modified, reverse=True)
                        sampling_data = []
                        i = 0
                        while calculate_bucket_size(sampling_data) < conf["input"]["params"]["sample"]["size_max_to_sample"]:
                            sampling_data = sampling_data + [bucket_object[i]]
                            i += 1
                        save_data(data_object=sampling_data, conf=conf, s3_co=s3_co, my_bucket=my_bucket)
                        print(f'Sampling until max')
                else:
                    parquet_file = filter_bucket_object(bucket_object)
                    last_parquet = find_last_parquet(parquet_file)
                    size_giga_sampling = calculate_bucket_size([last_parquet])
                    bucket_object.sort(key=lambda x: x.last_modified, reverse=True)
                    sampling_data = []
                    i = 0
                    while calculate_bucket_size(sampling_data) < conf["input"]["params"]["sample"]["size_max_to_sample"]:
                        sampling_data = sampling_data + [bucket_object[i]]
                        i += 1
                    save_data(data_object=sampling_data, conf=conf, s3_co=s3_co, my_bucket=my_bucket)
                    print(f'Sampling until max')
            else:
                save_data(data_object=bucket_object, conf=conf, s3_co=s3_co, my_bucket=my_bucket)
    return


if __name__ == '__main__':
    
    conf_file = 'configuration.yml'
    
    main()