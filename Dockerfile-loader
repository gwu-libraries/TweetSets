FROM python:3.6
MAINTAINER TweetSets <sfm@gwu.edu>

ADD requirements.txt /opt/tweetsets/
WORKDIR /opt/tweetsets
RUN pip install -r requirements.txt
RUN grep elasticsearch-dsl requirements.txt | xargs pip install -t dependencies

RUN set -ex && \
    echo 'deb [check-valid-until=no] http://archive.debian.org/debian jessie-backports main' \
      > /etc/apt/sources.list.d/jessie-backports.list && \
    echo "Acquire::Check-Valid-Until \"false\";" > /etc/apt/apt.conf.d/100disablechecks && \
    apt update -y && \
    apt install -t \
      jessie-backports \
      openjdk-8-jre-headless \
      ca-certificates-java \
      zip -y

WORKDIR /opt/tweetsets/dependencies
RUN zip -r ../dependencies.zip .
WORKDIR /opt/tweetsets

ADD tweetset_loader.py /opt/tweetsets/
ADD models.py /opt/tweetsets/
ADD utils.py /opt/tweetsets/
ADD setup.py /opt/tweetsets/
ADD elasticsearch-hadoop-6.2.2.jar /opt/tweetsets/elasticsearch-hadoop.jar
ADD tweetset_cli.py /opt/tweetsets/

RUN python setup.py bdist_egg

ENV SPARK_LOCAL_IP 0.0.0.0
ENV SPARK_DRIVER_PORT 5001
ENV SPARK_UI_PORT 5002
ENV SPARK_BLOCKMGR_PORT 5003
EXPOSE $SPARK_DRIVER_PORT $SPARK_UI_PORT $SPARK_BLOCKMGR_PORT

CMD /bin/bash
