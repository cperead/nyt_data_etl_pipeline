# Use the official MongoDB image as the base image
FROM mongo:latest

# Set the working directory in the container
WORKDIR /

# Install any dependencies required by the convert_data.py script
RUN apt-get update && \
    apt-get install -y python3 \
                       python3-pip

# Fetch the requirements for the app and install them
COPY ./etl/requirements_files/convert_data_mongo_requirements.txt /

RUN pip3 install -r convert_data_mongo_requirements.txt

# Create input_data and output_data directories in the container
RUN mkdir /input_data \
          /output_data \
          /etl

# Copy vars file
COPY ./etl/config_vars.py /etl/config_vars.py

# Copy the convert_data.py script to the container
COPY ./etl/convert_data.py /etl/convert_data.py

# Give execute permissions to convert script
RUN chmod +x /etl/convert_data.py

# Expose the MongoDB default port
EXPOSE 27017
