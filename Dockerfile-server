FROM python:3.6
MAINTAINER TweetSets <sfm@gwu.edu>


ADD requirements.txt /opt/tweetsets/
WORKDIR /opt/tweetsets
RUN pip install -r requirements.txt

ADD tweetset_server.py /opt/tweetsets/
ADD models.py /opt/tweetsets/
ADD utils.py /opt/tweetsets/
ADD stats.py /opt/tweetsets/
ADD tasks.py /opt/tweetsets/
ADD templates/ /opt/tweetsets/templates/
ADD static /opt/tweetsets/static/

RUN pip install gunicorn

EXPOSE 8080

CMD gunicorn -w 4 -b 0.0.0.0:8080 -t ${SERVER_TIMEOUT} tweetset_server:app
