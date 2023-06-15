@enforce 
def connection_S3(url: string) -> object:
    """
        This function create connection with S3 buckets 
        
        params:
        - url: url of the S3 buckets
    """
    #check if connection is available or not
    #check our rights
    #
    return

def check_format_file(table_name: string):
    """
        This function tell the format of the table ['PARQUET', 'DELTA', 'CSV', 'NA']
    """
    return

def get_parquet_file():
    """
        This function connect and take data from parquet file
    """
    return

def get_delta_file():
    """
        This function connect and take data from delta file
    """
    return

def transform_data_to_duckdb():
    """
        This function transform data to duckdb
    """
    return

def save_duckdb():
    """
        This function save the duckdb database
    """
    return

#MAIN FUNCTION
def main():
    # Yaml file with list of parquet file
    # Connection with S3 buckets
    # Get parquet file
    # Get last 3 month of data inside the parquet table
    # Save it into duckdb into the folder chosen
    
    # env file with token

    # Classe connection S3

    #TODO unitest

    return





if __name__ == '__main__':
    main()