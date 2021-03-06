# Import necessary libraries
# standard libraries
import json
import os
from datetime import datetime
from time import time
from time import sleep
import csv
import shutil
# 3rd-party libraries
import enlighten
import boto3
from pyspark.sql.types import StructType,StructField, StringType, DoubleType
from pyspark.sql import SparkSession
# custom functions
from packages.common import requestAndParse
from packages.page import extract_maximums, extract_listings
from packages.listing import extract_listing


# loads user defined parameters
def load_configs(path):
    with open(path,) as config_file:
        configurations = json.load(config_file)

    base_url_wo_formatting = configurations['base_url']
    target_num = int(configurations["target_num"])
    search_terms=configurations["search_terms"]
    locations=configurations["locations"]
    base_urls=[]
    #to modify the url we include
    for search_term in search_terms:
        for location in locations.keys():
            base_url=base_url_wo_formatting.format(country=location,
                                        search_term=search_term,
                                        country_code=locations[location],
                                        str_length=len(location)+len(search_term)+1)
            base_urls.append(base_url)
    return base_urls, target_num


# appends list of tuples in specified output csv file
# a tuple is written as a single row in csv file
def dataframe_writer(listOfTuples, schema, jobs_data):
    newRow = spark.createDataFrame(listOfTuples, schema)
    return jobs_data.union(newRow)

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

def connect_to_aws(credentials_path,spark):
    counter = 1
    while True:
        try:
            credentials = json.load(open(credentials_path))
            s3 = boto3.client('s3',
                    aws_access_key_id= credentials["AWSAccessKeyId"],
                    aws_secret_access_key=credentials["AWSSecretKey"])
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

def save_file(file, filename):
    #then we write
    try:
        file.write.mode('append').parquet(filename)
    except Exception as e:
        print("[ERROR] {} unable to write file ".format(e))
def save_file_to_aws(client, filename, bucket_filename, bucket):
    #then we write
    try:
        filenames = next(os.walk(filename), (None, None, []))[2]
        for file in filenames:
            client.upload_file(filename+"/"+file, bucket, bucket_filename+"/"+file)
    except Exception as e:
        print("[ERROR] {} unable to upload file to aws ".format(e))


def create_dataframe(schema,data=[]):
    df2 = spark.createDataFrame(data, schema)
    print("[INFO] Dataframe succesfully created")
    #df2.printSchema()
    return df2
def delete_file(output_file_name):
    try:
        shutil.rmtree(output_file_name)
        print("[INFO] Local copy succesfully removed")
    except Exception as e:
        print("[ERROR] {} Unable to erase local copy".format(e))
if __name__ == "__main__":
    base_urls, target_num = load_configs(path="./data/config.json")

    # initialises output directory and file
    if not os.path.exists('output'):
        os.makedirs('output')
    now = datetime.now() # current date and time
    #create the spark session
    spark = SparkSession \
        .builder \
        .master("local[*]")\
        .appName("jobScrapper") \
        .getOrCreate()
    output_file_name = "./output/" + now.strftime("%d-%m-%Y") + ".parquet"
    bucket="job-search-bucket"
    bucket_file_name="glassdoor-job-scrapping"+now.strftime("%d-%m-%Y") + ".parquet"
    s3_client=connect_to_aws("../../../credentials.json",spark)
    schema = StructType([StructField("company_name", StringType(), True),
                        StructField("company_rating", DoubleType(), True),
                        StructField("company_offered_role", StringType(), True),
                        StructField("company_role_location", StringType(), True),
                        StructField("job_description", StringType(), True),
                        StructField("requested_url", StringType(), True),
                        StructField("compensation_and_benefits", DoubleType(), True),
                        StructField("culture_and_values", DoubleType(), True),
                        StructField("career_opportunities", DoubleType(), True),
                        StructField("work_life_balance", DoubleType(), True),
                        StructField("job_type", StringType(), True),
                        StructField("industry", StringType(), True),
                        StructField("job_function", StringType(), True),
                        StructField("company_size", StringType(), True),
                        StructField("estimated_salary", StringType(), True)])
    # initialises enlighten_manager
    enlighten_manager = enlighten.get_manager()
    progress_total=enlighten_manager.counter(total=len(base_urls), desc="Total progress", unit="url scrapped", color="green", leave=False)
    urls_scrapped=0
    for base_url in base_urls:
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

        while total_listingCount < target_num_temp:
            # clean up buffer
            list_returnedTuple = []

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
                list_returnedTuple.append(append_tuple)
                # print(returned_tuple)
                progress_inner.update()

            progress_inner.close()
            #we create the dataframe that stores the info after scrapping the page
            jobs_data = create_dataframe(schema, list_returnedTuple)
            # done with page, moving onto next page
            total_listingCount = total_listingCount + jobCount
            print("[INFO] Finished processing page index {}; Total number of jobs processed: {}".format(page_index, total_listingCount))
            page_index = page_index + 1
            prev_url = new_url
            progress_outer.update(jobCount)

        #after scraping a whole serch-term we update de file in S3
        save_file(jobs_data,output_file_name)
        progress_outer.close()
        progress_total.update()
    save_file_to_aws(s3_client,output_file_name, bucket_file_name,bucket)
    delete_file(output_file_name)
    progress_total.close()
