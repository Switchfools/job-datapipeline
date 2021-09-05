import requests

from flask import Flask ,render_template

#let's start with our constants

#this url only allows to make gets 
JOBS_API="https://qh56a8dxx2.execute-api.us-east-2.amazonaws.com/dev/skills"

app = Flask(__name__,static_folder='static')


@app.route('/skills')
def drop_down_menu():
    jobs_json= requests.get(JOBS_API).json()
    jobs=[ (item["job_id"]["S"],item["job"]["S"]) for item in jobs_json["data"]["Items"]]
    return render_template("dropdown.html",jobs = jobs)
@app.route('/skills/<job_id>')
def display_jobs_skilss(job_id):
    skills_json= requests.get(JOBS_API+"/"+job_id).json()
    jobs_json= requests.get(JOBS_API).json()
    job_name=next(filter(lambda x: x["job_id"]["S"]== job_id,jobs_json["data"]["Items"]))["job"]["S"]
    skills=[(i, skills_json["data"]["Item"]["top_skill_n_"+str(i)]["S"]) for i in range(1,11)]
    return render_template("top_skills.html",name = job_name , skills=skills)
@app.route('/')
def dashboard():
    return render_template("home.html")
@app.errorhandler(404)
def page_not_found(e):
    return render_template("page-404.html")

if __name__=="__main__":
    app.run()

