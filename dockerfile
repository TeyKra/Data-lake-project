# ============================================================================
#        Dockerfile for Building the Airflow Image for the Data Lake Project
# ============================================================================

# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.7.1

# Switch to the root user to install system-level dependencies
USER root

# ----------------------------------------------------------------------------
#   INSTALL SYSTEM DEPENDENCIES
# ----------------------------------------------------------------------------
# Update the package lists and install essential build tools and Python development headers.
# The '--no-install-recommends' flag ensures only the necessary packages are installed.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# ----------------------------------------------------------------------------
#   CREATE NECESSARY DIRECTORIES
# ----------------------------------------------------------------------------
# Create directories for build artifacts, raw data, and scripts.
# These directories are used by the Airflow project for various purposes.
RUN mkdir -p /opt/airflow/build /opt/airflow/data/raw /opt/airflow/scripts

# Change the ownership of the created directories to the 'airflow' user and 'root' group.
RUN chown -R airflow:root /opt/airflow/build /opt/airflow/data/raw /opt/airflow/scripts

# Switch back to the 'airflow' user to continue with the image setup
USER airflow

# ----------------------------------------------------------------------------
#   COPY REQUIREMENTS FILE AND INSTALL PYTHON DEPENDENCIES
# ----------------------------------------------------------------------------
# Copy the requirements file from the build context into the build directory inside the container.
COPY build/reqs.txt /opt/airflow/build/reqs.txt

# Install the Python packages listed in the requirements file.
RUN pip install -r /opt/airflow/build/reqs.txt
