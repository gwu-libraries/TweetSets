FROM python:3.6
MAINTAINER TweetSets <sfm@gwu.edu>>

ADD requirements.txt /opt/tweetsets/
WORKDIR /opt/tweetsets
RUN pip install -r requirements.txt

ADD tweetset_server.py /opt/tweetsets/
ADD models.py /opt/tweetsets/
ADD utils.py /opt/tweetsets/
ADD stats.py /opt/tweetsets/
ADD tasks.py /opt/tweetsets/

CMD celery worker -A tweetset_server.celery -l ${LOGGING_LEVEL}
