FROM ubuntu:18.04
LABEL maintainer="Nicolas Vergara <switchfools@gmail.com>"
RUN apt-get update -y && \
    apt-get install -y python-pip python-dev
# We copy just the requirements.txt first to leverage Docker cache
COPY ./requirements.txt /app/requirements.txt

WORKDIR /app
RUN pip install -r requirements.txt
COPY . /app
RUM FLASK_APP=skills_interface.py 
RUN FLASK_ENV=development 
CMD [ "flask", "run" ]
