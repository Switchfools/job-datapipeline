# Import necessary libraries
# standard libraries
import json
import os
from datetime import datetime
from time import time
from time import sleep
import csv
import shutil
from io import BytesIO
import argparse
# 3rd-party libraries
import enlighten
import boto3
import pandas as pd
# custom functions
from common import requestAndParse
from page import extract_maximums, extract_listings
from listing import extract_listing


# loads user defined parameters
def load_configs(path, parser):
    with open(path,) as config_file:
        configurations = json.load(config_file)

    base_url_wo_formatting = configurations['base_url']
    target_num = parser['target_num']
    locations=configurations["locations"]
    base_urls=[]
    search_terms_dict={}
    #to modify the url we include
    for search_term in parser['job_type']:
        for location in parser['locations']:
            base_url=base_url_wo_formatting.format(country=location,
                                        search_term=search_term,
                                        country_code=locations[location],
                                        str_length=len(location)+len(search_term)+1)
            base_urls.append(base_url)
            search_terms_dict[base_url]="{}-{}".format(search_term,location)
    return base_urls, target_num,search_terms_dict


# appends list of tuples in specified output csv file
# a tuple is written as a single row in csv file
def dataframe_writer(list_of_tuples, schema, jobs_data):
    new_rows = pd.DataFrame(data=list_of_tuples, columns=schema)
    return pd.concat([jobs_data,new_rows])

# updates url according to the page_index desired
def update_url(prev_url, page_index):
    if page_index == 1:
        prev_substring = ".htm"
        new_substring = "_IP" + str(page_index) + ".htm"
    else:
        prev_substring = "_IP" + str(page_index - 1) + ".htm"
        new_substring = "_IP" + str(page_index) + ".htm"

    new_url = prev_url.replace(prev_substring, new_substring)
    return new_url

def connect_to_aws():
    counter = 1
    while True:
        try:
            s3 = boto3.resource('s3')
            print("[INFO] Succesfully mounted S3 bucket ")
            return s3

        except Exception as e:
            #try to connect 10 times, other wise stops program
            if counter > 1:
                print('[ERROR] {}. Unable to stablish conection with S3 bucket'.format(e))
                os._exit(os.EX_OK)
            else:
                print('[ERROR] {} FOUND: retrying S3 client connection'.format(e))
                sleep(5)
                counter += 1

def save_file_to_aws(file, client, bucket_filename, bucket):
    #then we write
    try:
        parquet_buffer = BytesIO()
        file.to_csv(parquet_buffer)
        client.Object(bucket, bucket_filename+".csv").put(Body=parquet_buffer.getvalue())
        print("[INFO] succesfully uploaded file to aws ")
    except Exception as e:
        print("[ERROR] {} unable to write file ".format(e))

def create_dataframe(schema,data=[]):
    df2 = pd.DataFrame(data=data, columns=schema)
    print("[INFO] Dataframe succesfully created")
    #df2.printSchema()
    return df2
def app_runner(event, context):
    base_urls, target_num,search_terms_dict = load_configs(path="./config.json",parser=event)
    saved_files=[]
    now = datetime.now() # current date and time
    bucket="job-search-bucket"
    bucket_file_name="glassdoor-job-scrapping"+now.strftime("%d-%m-%Y")+"-"
    s3_client=connect_to_aws()
    # initialises enlighten_manager
    schema = ["company_name", "company_rating", "company_offered_role", "company_role_location",\
             "job_description", "requested_url", "compensation_and_benefits", "culture_and_values",\
             "career_opportunities", "work_life_balance", "job_type", "industry", "job_function", \
             "company_size", "estimated_salary"]
    enlighten_manager = enlighten.get_manager()
    progress_total=enlighten_manager.counter(total=len(base_urls), desc="Total progress", unit="url scrapped", color="green", leave=False)
    urls_scrapped=0
    for base_url in base_urls:
        jobs_data=create_dataframe(schema)
        maxJobs, maxPages = extract_maximums(base_url)
        # print("[INFO] Maximum number of jobs in range: {}, number of pages in range: {}".format(maxJobs, maxPages))
        target_num_temp=target_num
        if (target_num_temp >= maxJobs):
            print("[ERROR] Target number larger than maximum number of jobs. Scraping {} jobs instead. \n".format(maxJobs))
            target_num_temp=maxJobs

        progress_outer = enlighten_manager.counter(total=target_num_temp, desc="Pages progress", unit="listings", color="yellow", leave=False)

        # initialise variables
        page_index = 1
        total_listingCount = 0

        # initialises prev_url as base_url
        prev_url = base_url
        start_time = time()
        while total_listingCount < target_num_temp:
            # clean up buffer
            list_returned_tuple = []

            new_url = update_url(prev_url, page_index)
            page_soup,_ = requestAndParse(new_url)
            listings_set, jobCount,listings_dict = extract_listings(page_soup)
            progress_inner = enlighten_manager.counter(total=len(listings_set), desc="Listings scraped from page", unit="listings", color="blue", leave=False)

            print("\n[INFO] Processing page index {}: {}".format(page_index, new_url))
            print("[INFO] Found {} links in page index {}".format(jobCount, page_index))
            for listing_url in listings_set:

                # to implement cache here

                returned_tuple = extract_listing(listing_url)
                if(listing_url in listings_dict.keys()):
                    append_tuple=returned_tuple+(listings_dict[listing_url],)
                else:
                    append_tuple=returned_tuple+(None,)
                list_returned_tuple.append(append_tuple)
                # print(returned_tuple)
                progress_inner.update()

            progress_inner.close()
            #we create the dataframe that stores the info after scrapping the page
            jobs_data = dataframe_writer(list_returned_tuple,schema,jobs_data)
            # done with page, moving onto next page
            total_listingCount = total_listingCount + jobCount
            print("[INFO] Finished processing page index {}; Total number of jobs processed: {}".format(page_index, total_listingCount))
            page_index = page_index + 1
            prev_url = new_url
            progress_outer.update(jobCount)

        #after scraping a whole serch-term we update de file in S3
        save_file_to_aws(jobs_data,s3_client,bucket_file_name+search_terms_dict[base_url],bucket)
        saved_files.append(bucket_file_name+search_terms_dict[base_url]+".csv")
        end_time= time()
        print("[INFO] {} s elapsed to scrappe and save data from {} with {} jobs".format(round(end_time-start_time,2),base_url,total_listingCount))
        progress_outer.close()
        progress_total.update()
    progress_total.close()
    return ("[INFO] Succesfully saved the {} following files :".format(len(saved_files),saved_files))
