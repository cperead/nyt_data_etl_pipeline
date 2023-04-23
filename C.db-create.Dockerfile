# Use the official ubuntu image as the base image
FROM ubuntu:latest

# Set the working directory in the container
WORKDIR /

# Create input_data and output_data directories in the container
RUN mkdir /output_data \
          /etl

# Install any dependencies required by the convert_data.py script
RUN apt-get update && \
    apt-get install -y python3 \
                       python3-pip

# Fetch the requirements for the app and install them
COPY ./etl/requirements_files/create_db_sqlite_requirements.txt /

RUN pip3 install -r create_db_sqlite_requirements.txt

# Copy vars file
COPY ./etl/config_vars.py /etl/config_vars.py

# Copy the  script to the container
COPY ./etl/create_db.py /etl/create_db.py

# Give execute permissions to convert script
RUN chmod +x /etl/create_db.py

# Create an entrypoint with the provided script
ENTRYPOINT ["python3", "/etl/create_db.py"]


