FROM ubuntu:latest

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /

RUN mkdir /input_data
RUN mkdir /etl

# Copy configuration file to the container
COPY ./etl/config_vars.py /etl

# Copy the script to the container
COPY ./etl/extract_data.sh /etl

# Make the script executable
RUN chmod +x /etl/extract_data.sh

# Run the script
ENTRYPOINT ["/bin/bash", "/etl/extract_data.sh"]
