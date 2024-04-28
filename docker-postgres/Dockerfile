FROM python:3.12.2

# Update package lists and install wget
RUN apt-get update && apt-get install -y wget

# Install Python packages
RUN pip install pandas sqlalchemy psycopg2 pyarrow

# Set working directory
WORKDIR /app

# Copy the Python script into the container
COPY ingest_data.py ingest_data.py

# Set the entry point for the container
ENTRYPOINT ["python", "ingest_data.py"]
