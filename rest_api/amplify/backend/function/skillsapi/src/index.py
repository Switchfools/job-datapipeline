import awsgi
import boto3 
import os
from flask_cors import CORS
from flask import Flask, jsonify, request 

#GLOBAL CONSTANTS 
BASE_ROUTE="/skills"
JOBS_TABLE = os.environ.get('STORAGE_JOBS_NAME')
SKILLS_TABLE = os.environ.get('STORAGE_TOP10SKILLS_NAME')
#clients and API calls 
app = Flask(__name__)
CORS(app)
client= boto3.client('dynamodb')

@app.route(BASE_ROUTE + '/<job_id>',methods=['GET'])
def get_skills(job_id):

    job_item=client.get_item(TableName=SKILLS_TABLE, Key={'job_id':{'S':job_id}} )

    return jsonify(data=job_item)

@app.route(BASE_ROUTE,methods=['GET'])
def list_items():
    return jsonify(data=client.scan(TableName=JOBS_TABLE))

def handler(event, context):

    return awsgi.response(app, event, context)