# Use the official ubuntu image as the base image
FROM ubuntu:latest

# Set the working directory in the container and create needed dirs
RUN mkdir /api \
          /output_data 
          
WORKDIR /api

# Install any dependencies required by the convert_data.py script
RUN apt-get update && \
    apt-get install -y python3 \
                       python3-pip

# Fetch the requirements for the app and install them
COPY ./etl/requirements_files/fastapi_requirements.txt /api

RUN pip3 install -r fastapi_requirements.txt

# Copy the API code
COPY ./0_api/main.py /api

# Give execute permissions to convert script
RUN chmod +x /api/main.py

# Expose port 8000
EXPOSE 8000

# Create an entrypoint with the provided script
# ENTRYPOINT ["python3", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--reload"]


