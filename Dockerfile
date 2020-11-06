# set base image (host OS)
FROM amd64/python:3.7-slim-stretch

# set the working directory in the container
WORKDIR /code

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip3 install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY message_ingester_v2.py .

# command to run on container start
CMD [ "python3", "./message_ingester_v2.py" ]
