import os
import io
import csv
import boto3
from simple_salesforce import Salesforce
from dotenv import load_dotenv


def execute_sf_query(SF, query):
    return SF.query_all(query)['records']


def get_cases_query():
    sf_sql = """
        SELECT
            CaseNumber,
            CreatedDate,
            ClosedDate,
            Status,
            Priority,
            Organization__c,
            ads_resourceman__Related_Service__c,
            Description
        FROM Case
        WHERE Organization__c = 'Houston Food Bank'
    """
    # WHERE CreatedDate = LAST_N_DAYS:1
    return sf_sql


def build_result_dict(raw_results):
    clean_results = []
    for r in raw_results:
        del r['attributes']
        _keys = r.keys()
        clean_results.append({a: r[a] for a in _keys})
    return clean_results


def list_of_dicts_to_local_csv_file(list_of_dicts, file_path):
    """Writes a list of dicts to a csv file in local filesystem

    Parameters
    ----------
    list_of_dicts : [{}, {}, ...]
        Python list of dicts

    file_path : str
        File path to write to, including filename and extension
    """
    keys = list_of_dicts[0].keys()
    with open(file_path, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)


def list_of_dicts_to_csv_string(list_of_dicts):
    keys = list_of_dicts[0].keys()
    output = io.StringIO()
    dict_writer = csv.DictWriter(output, fieldnames=keys)
    dict_writer.writeheader()
    dict_writer.writerows(list_of_dicts)
    output.seek(0)
    return output.getvalue()


def csv_string_to_s3(csv_str, bucket, path_in_bucket):
    """Writes a csv string to a file in S3.
    Assumes AWS credentials are stored locally in ~/.aws/credentials as described here:
    https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html

    Parameters
    ----------
    csv_str : str
        String of comma separated csv data

    bucket : str
        Name of the bucket to write to

    path_in_bucket : str
        File path within the bucket to write to, including filename and extension
        ex: link2feed/clients.csv
    """
    s3 = boto3.client(service_name='s3', region_name='us-east-1')
    s3.put_object(Bucket=bucket, Key=path_in_bucket, Body=csv_str)
    print(f'CSV uploaded to S3: {bucket}/{path_in_bucket}')


def main():
    load_dotenv()
    credentials = {
        'username': os.environ['USERNAME'],
        'password': os.environ['PASSWORD'],
        'security_token': os.environ['SECURITY_TOKEN'],
    }
    sf = Salesforce(**credentials)
    print('connected to sf')
    raw_results = execute_sf_query(sf, get_cases())
    dict_results = build_result_dict(raw_results)
    print('len of results: {l}'.format(l=len(dict_results)))
    list_of_dicts_to_local_csv_file(dict_results, file_path='./cases.csv')
    # csv_results = list_of_dicts_to_csv_string(dict_results)
    # csv_string_to_s3(csv_results, 'hfb-etl-data', 'cax_test.csv')


main()
