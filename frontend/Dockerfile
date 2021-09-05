FROM ubuntu:18.04
ARG RUNTIME_VERSION="3.9"
LABEL maintainer="Nicolas Vergara <switchfools@gmail.com>"
RUN apt-get update -y && \
    apt-get install -y python3.9-pip python3.9-dev
# We copy just the requirements.txt first to leverage Docker cache
COPY ./requirements.txt /app/requirements.txt

WORKDIR /app
RUN pip install -r requirements.txt
COPY . /app
RUN FLASK_APP=skills_interface.py 
RUN FLASK_ENV=development 
CMD [ "flask", "run" ]
