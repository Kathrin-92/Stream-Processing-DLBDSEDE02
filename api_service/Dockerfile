FROM python:3.13.2-slim-bullseye AS base

# prevents Python from writing pyc files.
# keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# set the working directory in the container
WORKDIR /usr/src/api_service

# install required system packages and upgrade pip
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libhdf5-dev \
    libfreetype6-dev \
    libpng-dev \
    cron \
    && apt-get install -y curl \
    && apt-get install -y procps \
    && python -m pip install --upgrade pip \
    && rm -rf /var/lib/apt/lists/*

# copy requirements.txt from host to container
COPY requirements.txt .

# install Python dependencies
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --no-cache-dir -r requirements.txt

# copy the project files into the container \
COPY sensor_data/batch_data /usr/src/api_service/sensor_data/batch_data
COPY sensor_data/metadata /usr/src/api_service/sensor_data/metadata
COPY sensor_data/stream_data /usr/src/api_service/sensor_data/stream_data

COPY fetch_data.py /usr/src/api_service/fetch_data.py
COPY stream_simulation.py /usr/src/api_service/stream_simulation.py
COPY main.py /usr/src/api_service/main.py

#CMD ["python3", "/usr/src/api_service/main.py"]

# copy crontab file to the container
COPY crontab /etc/cron.d/batch-cron

# give execution rights to the cron job
RUN chmod 0644 /etc/cron.d/batch-cron

# apply the cron job and start the cron service
RUN crontab /etc/cron.d/batch-cron
CMD ["cron", "-f"]