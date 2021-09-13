import sys
from datetime import datetime
from time import sleep
import pandas as pd
import json
import numpy as np
import boto3
import re
import requests
from io import BytesIO,StringIO
from uuid import uuid5,NAMESPACE_DNS
from collections import Counter
def load_data(bucket,skills_filename):
    counter = 1
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
    while True:
        try:
            s3 = boto3.resource('s3')
            s3c = boto3.client('s3')
            my_bucket = s3.Bucket(bucket)
            objs = s3c.list_objects_v2(Bucket=bucket)['Contents']
            jobs_filename = [obj['Key'] for obj in sorted(objs, key=get_last_modified)][-1]
            jobs_obj = s3c.get_object(Bucket=bucket, Key=jobs_filename)
            jobs=pd.read_csv(BytesIO(jobs_obj['Body'].read()))
            print("[INFO] succesfully loaded jobs data")
            skills_obj = s3c.get_object(Bucket=bucket, Key=skills_filename)
            skills= json.load(BytesIO(skills_obj['Body'].read()))
            print("[INFO] succesfully loaded skills data")
            return jobs,skills
        except Exception as e:
            #try to connect 10 times, other wise stops program
            if counter > 5:
                print('[ERROR] {}. Unable to stablish conection with S3 bucket'.format(e))
                sys.exit()
            else:
                print('[ERROR] {} FOUND: retrying S3 client connection'.format(e))
                sleep(5)
                counter += 1
def extract_top_skills(jobs,skills):
    insertion_data=[]
    for job in jobs.job_position.unique():
        results = Counter()
        job_data=jobs.loc[jobs.job_position.str.contains(job)]
        job_data.job_description.str.lower().str.split().map(lambda x: filter(lambda word: word in skills["skills"] ,x)).apply(results.update)
        insertion_data.append((job,np.array(results.most_common(10))[:,0]))
    return(insertion_data)
def insert_data(insertion_data):
    dynamodb = boto3.resource('dynamodb')
    job_table = dynamodb.Table('Jobs')
    top_10_skills_table=dynamodb.Table('Top_10_skills')
    for data_point in insertion_data:
        job = data_point[0]
        job_id= str(uuid5(NAMESPACE_DNS,job))
        job_table.put_item(Item={'job_id':job_id,'job':job})
        skills_dict={'job_id':job_id}
        for i in range(len(data_point[1])):
            skills_dict["top_skill_n_"+str(i+1)]=data_point[1][i]
            top_10_skills_table.put_item(Item=skills_dict)

if __name__=="__main__":
    clean_data_bucket = 'clean-data-bucket-jobs-search'
    now = datetime.now() # current date and time
    skills_filename ="skills.json"
    jobs,skills=load_data(clean_data_bucket,skills_filename)
    insertion_data=extract_top_skills(jobs,skills)
    insert_data(insertion_data)
