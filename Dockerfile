FROM apache/airflow:3.0.3

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME dynamically
RUN echo "export JAVA_HOME=\$(readlink -f /usr/bin/java | sed 's:/bin/java::')" >> /etc/environment
ENV JAVA_HOME $(readlink -f /usr/bin/java | sed 's:/bin/java::')

# Alternative: Set it to the standard location
ENV JAVA_HOME /usr/lib/jvm/default-java

# Verify Java installation
RUN java -version
RUN echo $JAVA_HOME

USER airflow
# Install Apache Spark
RUN pip install apache-airflow-providers-common-compat==1.7.2
RUN pip install apache-airflow-providers-apache-spark==5.3.1
RUN pip install pyspark==4.0.1
RUN pip install resend==2.19.0
RUN pip install duckdb==1.1.3
RUN pip install great-expectations==1.3.1
RUN pip install s3fs==2024.10.0
RUN pip install faker==33.1.0
RUN pip install pandas==2.2.3