# custom functions
from common import requestAndParse
from page import extract_maximums, extract_listings
from listing import extract_listing
import sys

def handler(event, context):
    return 'Hello from AWS Lambda using Python' + sys.version + '!'
