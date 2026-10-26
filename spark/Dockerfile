FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
USER root
# --no-deps: keep using the Spark install in /opt/spark (so spark-defaults.conf applies),
# not a second pyspark from pip.
RUN pip3 install --no-cache-dir --no-deps delta-spark==3.2.0 \
 && pip3 install --no-cache-dir importlib-metadata pyyaml pytest kafka-python
ENV SPARK_HOME=/opt/spark
