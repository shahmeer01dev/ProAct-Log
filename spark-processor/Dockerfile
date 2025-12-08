# Start with a base image that includes Java, necessary for Spark
FROM eclipse-temurin:11-jre

# Install Spark dependencies
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/spark

RUN apt-get update && apt-get install -y wget python3 python3-pip && rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -O /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C / \
    && mv /spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

# Download PostgreSQL JDBC Driver
ENV POSTGRES_JDBC_VERSION=42.7.3
RUN wget -q "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar" -O ${SPARK_HOME}/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Install PySpark
RUN pip3 install --break-system-packages --no-cache-dir pyspark==${SPARK_VERSION}

WORKDIR /app
# Copy the Python script that runs the processor
COPY realtime_processor.py . 

# Command to run the Spark Streaming Job
CMD ["/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "--conf", "spark.driver.extraClassPath=/spark/jars/postgresql-42.7.3.jar", \
     "realtime_processor.py"]