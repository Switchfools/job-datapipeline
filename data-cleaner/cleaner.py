import sys
from datetime import datetime
from time import sleep
import pandas as pd
import numpy as np
import boto3
import re
import requests
from io import BytesIO,StringIO
def extract_job(file_job_string):
    suffix="glassdoor-job-scrapping02-09-2021"
    job_location_string=job[len(suffix)+1:].split("-")
    job_string=" ".join(job_location_string[:-1])
    return job_string
def load_data(bucket):
    counter = 1
    while True:
        try:
            s3 = boto3.resource('s3')
            s3c = boto3.client('s3')
            my_bucket = s3.Bucket(bucket)
            list_of_files=[]
            for my_bucket_object in my_bucket.objects.all():
              if (my_bucket_object.key.endswith('.csv')):
                obj = s3c.get_object(Bucket=bucket, Key=my_bucket_object.key)
                df=pd.read_csv(BytesIO(obj['Body'].read()))
                df["job_position"]=extract_job(my_bucket_object.key)
                list_of_files.append(df)
            jobs=pd.concat(list_of_files, axis=0, ignore_index=True)
            print("[INFO] succesfully loaded data")
            return jobs
        except Exception as e:
            #try to connect 10 times, other wise stops program
            if counter > 5:
                print('[ERROR] {}. Unable to stablish conection with S3 bucket'.format(e))
                sys.exit()
            else:
                print('[ERROR] {} FOUND: retrying S3 client connection'.format(e))
                sleep(5)
                counter += 1

def convert_min(salary,exchange_rates):
    salary=salary.replace("(Employer Est.)", '')
    first_int=re.search(r"\d",salary).start()
    currency=salary[:first_int].replace(u'\xa0', u'')
    conversions={'£':'GBP', 'CA$':'CAD', 'COP':'COP', '$':'USD', '€':'EUR', 'SGD':'SGD', 'CHF':'CHF', 'NOK':'NOK', 'IRR':'IRR'}
    per_hour=False
    if "Per Hour" in salary:
        salary=salary.replace("Per Hour", '')
        per_hour=True
    salary=salary.replace(currency,'')
    salary=salary.replace(",",'')
    salary_int=np.array(salary.split("-")).astype(int)
    if per_hour:
        hours=8
        #we assume a total of 261 working days per year
        days=261
        salary_int=salary_int*hours*days

    return round((salary_int/exchange_rates[conversions[currency]])[0])
def convert_max(salary,exchange_rates):
    salary=salary.replace("(Employer Est.)", '')
    first_int=re.search(r"\d",salary).start()
    currency=salary[:first_int].replace(u'\xa0', u'')
    conversions={'£':'GBP', 'CA$':'CAD', 'COP':'COP', '$':'USD', '€':'EUR', 'SGD':'SGD', 'CHF':'CHF', 'NOK':'NOK', 'IRR':'IRR'}
    per_hour=False
    if "Per Hour" in salary:
        salary=salary.replace("Per Hour", '')
        per_hour=True
    salary=salary.replace(currency,'')
    salary=salary.replace(",",'')
    salary_int=np.array(salary.split("-")).astype(int)
    if per_hour:
        hours=8
        #we assume a total of 261 working days per year
        days=261
        salary_int=salary_int*hours*days

    return round((salary_int/exchange_rates[conversions[currency]])[-1])

def clean_data(jobs, url_exchange_rate):
    counter = 1
    while True:
        try:
            jobs_wo_nan= jobs.loc[jobs.company_name.notna()]
            jobs_clean=jobs_wo_nan
            #first we normalize our strings, lower cases and titles
            jobs_clean["company_rating"]=jobs_wo_nan.company_name.map(lambda x: x[-4:-1] if '★' in x else np.nan)
            jobs_clean["company_name"]=jobs_wo_nan.company_name.map(lambda x: x[:-4] if '★' in x else x)
            jobs_clean["company_name"]=jobs_clean["company_name"].map(str.title)
            jobs_clean["company_offered_role"]=jobs_clean["company_offered_role"].map(lambda x: x.title if x == np.nan else x)
            jobs_clean["company_role_location"]=jobs_clean["company_role_location"].map(lambda x: x.title if x == np.nan else x)
            jobs_clean["job_description"]=jobs_clean["job_description"].map(lambda x: x.lower if x == np.nan else x)
            jobs_clean["job_function"]=jobs_clean["job_function"].map(lambda x: x.lower if x == np.nan else x)
            print("[INFO] succesfully normalize strings")
            #then we convert the company size text into numbers fields
            jobs_clean["company_size_min"]=jobs_clean.company_size.map(lambda x : int(x.replace("Employees", '').replace("+",
                                            ' to 10000').split('to')[0]) if isinstance(x, str) else np.nan)
            jobs_clean["company_size_max"]=jobs_clean.company_size.map(lambda x : int(x.replace("Employees", '').replace("+",
                                            ' to 10000').split('to')[1]) if isinstance(x, str) else np.nan)
            print("[INFO] company size succesfully extracted")
            #Finally we take the stimated salary text, convert it into numbers and the convert them again from local money to USD
            exchange_rates=requests.get(url_exchange_rate).json()["rates"]
            jobs_clean["min_estimated_salary"]=jobs_clean.estimated_salary.dropna().map(lambda x:convert_min(x,exchange_rates))
            jobs_clean["max_estimated_salary"]=jobs_clean.estimated_salary.dropna().map(lambda x:convert_max(x,exchange_rates))
            print("[INFO] estimated salary succesfully extracted")
            return jobs_clean
        except Exception as e:
            #try to connect 10 times, other wise stops program
            if counter > 5:
                print('[ERROR] {}. Unable to clean data'.format(e))
                sys.exit()
            else:
                print('[ERROR] {} retrying cleaning data ...'.format(e))
                sleep(5)
                counter += 1
def save_file_to_aws(file,bucket_filename, bucket):
    counter = 1
    while True:
        #then we write
        try:
            s3 = boto3.resource('s3')
            print("[INFO] Succesfully mounted S3 bucket ")
            parquet_buffer = StringIO()
            file.to_csv(parquet_buffer)
            s3.Object(bucket, bucket_filename).put(Body=parquet_buffer.getvalue())
            print("[INFO] succesfully uploaded file to aws ")
            return "done"
        except Exception as e:
            #try to connect 10 times, other wise stops program
            if counter > 5:
                print('[ERROR] {}. Unable to save file to S3 bucket'.format(e))
                sys.exit()
            else:
                print('[ERROR] {} unable to write file retrying ...'.format(e))
                sleep(5)
                counter += 1

if __name__=="__main__":
    raw_data_bucket = 'job-search-bucket'
    clean_data_bucket = 'clean-data-bucket-jobs-search'
    now = datetime.now() # current date and time
    bucket_filename="glassdoor-job-scrapping-"+now.strftime("%d-%m-%Y")+".csv"
    url_exchange_rate="https://api.exchangerate-api.com/v4/latest/USD"
    jobs = load_data(raw_data_bucket)
    clean_jobs = clean_data(jobs, url_exchange_rate)
    save_file_to_aws(clean_jobs,bucket_filename, clean_data_bucket)
