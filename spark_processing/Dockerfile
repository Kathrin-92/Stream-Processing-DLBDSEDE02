# load base image / preconfigured environment for Apache Spark
FROM apache/spark:3.5.5

# prevents Python from writing pyc files.
# keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# use python 3 when running PySpark jobs
ENV PYSPARK_PYTHON=python3

# configure Python's module search path to include Spark's Python directories
ENV PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/build/:$PYTHONPATH

# specify location of Spark inside the container
ENV SPARK_HOME=/opt/spark

# switch to root user because of required privileges
USER root

# creates directory /usr/src/spark_processing where to store the application (.py file)
# and give spark user permission for it
RUN mkdir -p /usr/src/spark_processing && chown -R spark:spark /usr/src/spark_processing

# fetch JDBC driver into Spark's jars directory
RUN curl -o /opt/spark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# set working directory - all operations will operate relative to this directory
WORKDIR /usr/src/spark_processing

# copy application files and dependencies into container
COPY spark_stream.py /usr/src/spark_processing/spark_stream.py
COPY requirements.txt /usr/src/spark_processing/requirements.txt

# install dependencies
RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install --no-cache-dir -r requirements.txt

# switch back to standard user
USER spark

# container will listen to these default ports
EXPOSE 7077 8080 4040

# run application
CMD ["spark-submit", "/usr/src/spark_processing/spark_stream.py"]