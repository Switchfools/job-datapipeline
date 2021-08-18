# Import necessary libraries
# standard libraries
from time import time
import re
from functools import cache
# custom functions
try:
    from packages.common import requestAndParse
except ModuleNotFoundError:
    from common import requestAndParse


# extracts desired data from listing banner
def extract_listingBanner(listing_soup):
    listing_bannerGroup_valid = False

    try:
        listing_bannerGroup = listing_soup.find("div", class_="css-ur1szg e11nt52q0")
        listing_bannerGroup_valid = True
    except:
        print("[ERROR] Error occurred in function extract_listingBanner")
        companyName = "NaN"
        company_starRating = "NaN"
        company_offeredRole = "NaN"
        company_roleLocation = "NaN"

    if listing_bannerGroup_valid:
        try:
            company_starRating = listing_bannerGroup.find("span", class_="css-1pmc6te e11nt52q4").getText()
        except:
            company_starRating = "NaN"
        if company_starRating != "NaN":
            try:
                companyName = listing_bannerGroup.find("div", class_="css-16nw49e e11nt52q1").getText().replace(company_starRating,'')
            except:
                companyName = "NaN"
            # company_starRating.replace("★", "")
            company_starRating = company_starRating[:-1]
        else:
            try:
                companyName = listing_bannerGroup.find("div", class_="css-16nw49e e11nt52q1").getText()
            except:
                companyName = "NaN"

        try:
            company_offeredRole = listing_bannerGroup.find("div", class_="css-17x2pwl e11nt52q6").getText()
        except:
            company_offeredRole = "NaN"

        try:
            company_roleLocation = listing_bannerGroup.find("div", class_="css-1v5elnn e11nt52q2").getText()
        except:
            company_roleLocation = "NaN"
        try:
            company_salary_estimate = listing_bannerGroup.find("div", class_="css-ur1szg e11nt52q0").getText()
        except:
            company_salary_estimate = "NaN"
    return companyName, company_starRating, company_offeredRole, company_roleLocation


# extracts desired data from listing description
def extract_listingDesc(listing_soup):
    extract_listingDesc_tmpList = []
    listing_jobDesc_raw = None

    try:
        listing_jobDesc_raw = listing_soup.find("div", id="JobDescriptionContainer")
        if type(listing_jobDesc_raw) != type(None):
            JobDescriptionContainer_found = True
        else:
            JobDescriptionContainer_found = False
            listing_jobDesc = "NaN"
    except Exception as e:
        print("[ERROR] {} in extract_listingDesc".format(e))
        JobDescriptionContainer_found = False
        listing_jobDesc = "NaN"

    if JobDescriptionContainer_found:
        jobDesc_items = listing_jobDesc_raw.findAll('li')
        for jobDesc_item in jobDesc_items:
            extract_listingDesc_tmpList.append(jobDesc_item.text)

        listing_jobDesc = " ".join(extract_listingDesc_tmpList)

        if len(listing_jobDesc) <= 10:
            listing_jobDesc = listing_jobDesc_raw.getText()

    return listing_jobDesc
#extracts the value for a matching attribute of the list
def extract_parent_sibling_attr(search_group,attribute):
    finded_attr=False
    for object in search_group:
        if(attribute in object.text):
            finded_attr=True
            break
    if not finded_attr:
        raise Exception('attribute {} not found'.format(attribute))
    if (object.parent.next_sibling.text=="N/A"):
        return "NaN"
    return object.parent.next_sibling.text
#extracts the value for a matching attribute of the list
def extract_sibling_attr(search_group,attribute):
    finded_attr=False
    for object in search_group:
        if(attribute in object.text):
            finded_attr=True
            break
    if not finded_attr:
        raise Exception('attribute {} not found'.format(attribute))
    if (object.next_sibling.text=="N/A"):
        return "NaN"
    elif(object.next_sibling.text==""):
        object=object.next_sibling
    return object.next_sibling.text
# extracts desired higlights of the company from listing description
def extract_listing_highlights(listing_soup):
    listing_highlightsGroup_valid = False
    try:
        listing_highlightsGroup = listing_soup.find_all("div", class_="css-1x772q6 e18tf5om0")
        listing_highlightsGroup_valid = True
    except:
        print("[ERROR] Error occurred in function extract_listing_highlights")
        compensation_and_benefits = "NaN"
        culture_and_values = "NaN"
        career_opportunities = "NaN"
        work_life_balance = "NaN"
        job_type = "NaN"
        industry = "NaN"
        job_function= "NaN"
        company_size="NaN"
    if listing_highlightsGroup_valid :
        list_of_highlights=listing_soup.find_all("span", class_="css-1vg6q84 e18tf5om6")
        try:
            compensation_and_benefits =extract_parent_sibling_attr(list_of_highlights,"Compensation & Benefits")
        except Exception as e:
            compensation_and_benefits = "NaN"
            print("[ERROR] {} in extract_listing_highlights".format(e))
        try:
            culture_and_values =extract_parent_sibling_attr(list_of_highlights,"Culture & Values")
        except Exception as e:
            culture_and_values = "NaN"
            print("[ERROR] {} in extract_listing_highlights".format(e))
        try:
            career_opportunities =extract_parent_sibling_attr(list_of_highlights,"Career Opportunities")
        except Exception as e:
            career_opportunities = "NaN"
            print("[ERROR] {} in extract_listing_highlights".format(e))
        try:
            work_life_balance =extract_parent_sibling_attr(list_of_highlights,"Work/Life Balance")
        except Exception as e:
            work_life_balance = "NaN"
            print("[ERROR] {} in extract_listing_highlights".format(e))
        try:
            job_type = extract_sibling_attr(list_of_highlights,"Job Type")
        except Exception as e:
            job_type = "NaN"
            print("[ERROR] {} in extract_listing_highlights".format(e))
        try:
            industry = extract_sibling_attr(list_of_highlights,"Industry")
        except Exception as e:
            industry = "NaN"
            print("[ERROR] {} in extract_listing_highlights".format(e))
        try:
            job_function= extract_sibling_attr(list_of_highlights,"Job Function")
        except Exception as e:
            job_function= "NaN"
            print("[ERROR] {} in extract_listing_highlights".format(e))
        try:
            company_size = extract_sibling_attr(list_of_highlights,"Size")
            if(company_size == "Unknown" or company_size == "unknown"):
                company_size = "NaN"
        except Exception as e:
            company_size = "NaN"
            print("[ERROR] {} in extract_listing_highlights".format(e))
    return compensation_and_benefits,culture_and_values,career_opportunities,work_life_balance,\
            job_type,industry,job_function,company_size
# extract data from listing
@cache
def extract_listing(url):
    request_success = False
    try:
        listing_soup, requested_url = requestAndParse(url)
        request_success = True
    except Exception as e:
        print("[ERROR] Error occurred in extract_listing, requested url: {} is unavailable.".format(url))
        return ("NaN", "NaN", "NaN", "NaN", "NaN", "NaN","NaN", "NaN", "NaN", "NaN","NaN", "NaN", "NaN", "NaN")

    if request_success:
        companyName, company_starRating, company_offeredRole, company_roleLocation = extract_listingBanner(listing_soup)
        listing_jobDesc = extract_listingDesc(listing_soup)
        compensation_and_benefits,culture_and_values,career_opportunities,work_life_balance,\
                job_type,industry,job_function,company_size= extract_listing_highlights(listing_soup)
        return (companyName, company_starRating, company_offeredRole, company_roleLocation, listing_jobDesc, requested_url, \
                compensation_and_benefits,culture_and_values,career_opportunities,work_life_balance,\
                job_type,industry,job_function,company_size)


if __name__ == "__main__":

    url = "https://www.glassdoor.sg/job-listing/senior-software-engineer-java-scala-nosql-rakuten-asia-pte-JV_KO0,41_KE42,58.htm?jl=1006818844403&pos=104&ao=1110586&s=58&guid=00000179d5112735aff111df641c01be&src=GD_JOB_AD&t=SR&vt=w&ea=1&cs=1_c8e7e727&cb=1622777342179&jobListingId=1006818844403&cpc=AF8BC9077DDDE68D&jrtk=1-1f7ah29sehimi801-1f7ah29t23ogm000-80a84208d187d367&jvt=aHR0cHM6Ly9zZy5pbmRlZWQuY29tL3JjL2dkL3BuZz9hPUh5MlI4ekNxUWl3d19sM3FuaUJHaFh3RlZEYUJyUWlpeldIM2VBR1ZHTUVSeUk5VEo1ZTEzWWl5dU1sLWJWX0NIeGU4NjBDc3o0dE5sV3ZLT2pRTHFIZU5KTHpPLUhLeEFRSERmeE5CdHNUTUc1RV9FSFR2VW5FNldmWWxJQVp5dXIzNFRZZjIzLWNWNXE0NnRhSTF3V1pKeW54dHhNUkxVRlhEekI2djYwMVZGWl9vbGU5andSYjVhX3BvT0cza0JJb0NYQXo0TVZhNWdvUFY4dXY3WVJTYlMySUpZTVpyR252dEc3ZFM1aXlFQ09icHI0YVRKU2ZLUzkzMUxmLXpyQjFlZHZxbHBxbElZMXhpRksxZmdIMEhFLTJBN2pySHRZa1g0aDJCWGRxTzBCdDM0bDNzWlJDLWIxaUlCT0xnZFh6bjg4cnNjZ1N0V1BHdVhNVm5xT3A3Q0s1UEEtb0QxWDl0WFhkY19WM3Fic0dSS0tfZi1oVUZyUUlrc0o2ZV9yVHNjaFpRVkIyV2V1bmRBejNYQWVPcFZNb3lqZFlONWpLUTdVbDUxTlU5LXFVWnZIT19VWlNEWDVtdVYwR3dNbWpXVDFyaHhMM3ZkcUZqcnM4WDZuc3BYYUhYcHg1dXNUVTVJODdzQk12Q2owaXkxTmRjUmhNXzU2TF9KbXNlY0VzajNWWmFOMDQ3QmNSWU5HSGNFNmctcXUzRUV4bHJrdjQxQ3QteW02ZFo5bE45XzBfb3prR2NBVkdqQU9kaS1UNWRwVnllYzA1OU53Q3Aya2QwdHdoRU5kUnU5UzNlTUR5WmJOSFZGb0t3MnR6V1lKbTllaGxuS3hTMEdoMDhLekVBWGg4OW9BblZGR2U2ajRtMUw3T29CSVNvZWVZaC0wRHRoSTV4eUV0ODJCRERkeTV3QlREUVNTUUZ1Mkp3WUEyRE9qZk5udk5xbzQwaVZKRmF0VWFlVDc2TFl6bnIwQTB2RWRGZlNORE41QmlUaHI3VmgyUWs3bkRGaVFibmUzcWlqZE1ZYzR5TmVYZUhnUFFmOHEwc1Q2aHJrX0hPX1RwbWI5M21hd2hxOEd6a2lEaFMtUQ&ctt=1622777391568"
    url ="https://www.glassdoor.com/job-listing/software-engineer-java-js-python-c-html-css-amk-oaktree-consulting-JV_IC3235895_KO0,47_KE48,66.htm?jl=1007214097555&pos=127&ao=1136043&s=58&guid=0000017b55d889fb9e30c1c9dca904e7&src=GD_JOB_AD&t=SR&vt=w&ea=1&cs=1_ca7990ac&cb=1629232860127&jobListingId=1007214097555&jrtk=2-0-1fdath2h0u579801-1fdath2hku1t9800-4ac7ac01ce95db88&ctt=1629233652387"
    start_time = time()
    returned_tuple = extract_listing(url)
    time_taken = time() - start_time
    print(returned_tuple)
    print("[INFO] returned in {} seconds".format(time_taken))
