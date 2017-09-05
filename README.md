# TweetSets
Twitter datasets for research and archiving.

* Create your own Twitter dataset from existing datasets.
* Conforms with Twitter policies.

TweetSets allows users to (1) select from existing datasets; (2) limit the dataset by querying on keywords, hashtags, 
and other parameters; (3) generate and download dataset derivatives such as the list of tweet ids and mention nodes/edges.

## Modes
TweetSets can be run in different modes. The modes determine which datasets are available and what type of dataset
derivates can be generated.

* public mode: Source datasets that are marked as local only are excluded. Dataset derivates that include the text of the
tweet cannot be generated.
* local mode: All source datasets are included, including those that are marked as local only. All dataset derivatives 
can be generated, including those that include the text of the tweet.
* both mode: For configured network IP ranges, the user is placed in local mode. Otherwise, the user is placed in public
mode.

These modes allow conforming with the Twitter policy that prohibits sharing complete tweets with 3rd parties.

Modes are configured in the `.env` file as described below.

## Installing
### Prerequisites
* Docker
* Docker-compose
* Set `vm_max_map_count` as described in the [ElasticSearch documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html).

### Installation
1. Create data directories on a volume with adequate storage:

        mkdir -p /tweetset_data/redis
        mkdir -p /tweetset_data/datasets
        mkdir -p /tweetset_data/elasticsearch/esdata1
        mkdir -p /tweetset_data/elasticsearch/esdata2
        chown -R 1000:1000 /tweetset_data/elasticsearch

Note:
* Create an `esdata<number>` directory for each ElasticSearch container.
* On OS X, the `redis` and `esdata<number>` directories must be `ugo+rwx`.
2. Clone or download this repository:

        git clone https://github.com/justinlittman/TweetSets.git
        
3. Change to the `docker` directory:
   
        cd docker
4. Copy the example docker files:

        cp example.docker-compose.yml docker-compose.yml
        cp example.env .env

5. Edit `.env`. This file is annotated to help you select appropriate values.
6. Bring up the containers:

        docker-compose up -d

### Cluster installation
#### Master
1. Create data directories on a volume with adequate storage:

        mkdir -p /tweetset_data/redis
        mkdir -p /tweetset_data/datasets
        mkdir -p /tweetset_data/elasticsearch
        chown -R 1000:1000 /tweetset_data/elasticsearch

2. Clone or download this repository:

        git clone https://github.com/justinlittman/TweetSets.git
        
3. Change to the `docker` directory:
   
        cd docker
4. Copy the example docker files:

        cp example.cluster-master.docker-compose.yml docker-compose.yml
        cp example.env .env

5. Edit `.env`. This file is annotated to help you select appropriate values.
6. Bring up the containers:

        docker-compose up -d

#### Worker
1. Create data directories on a volume with adequate storage:

        mkdir -p /tweetset_data/elasticsearch
        chown -R 1000:1000 /tweetset_data/elasticsearch

2. Clone or download this repository:

        git clone https://github.com/justinlittman/TweetSets.git
        
3. Change to the `docker` directory:
   
        cd docker
4. Copy the example docker files:

        cp example.cluster-worker.docker-compose.yml docker-compose.yml
        cp example.cluster-worker.env .env

5. Edit `.env`. This file is annotated to help you select appropriate values.
6. Bring up the containers:

        docker-compose up -d


## Loading a source dataset
### Prepping the source dataset
1. Create a dataset directory.
2. Place tweet files in the directory. The tweet files can be line-oriented JSON (.json) or gzip compressed
line-oriented JSON (.json.gz).
3. Create a dataset description file in the directory named `dataset.json`. See `example.dataset.json` for
the format of the file.

### Loading
1. Point to the dataset directory:

        export DATASET_PATH=/my-dataset
2. Start and connect to a loader container:

        docker-compose run --rm loader /bin/bash
3. Invoke the loader:

        python tweetset_loader.py dataset
    
To see other loader commands:

        python dataset_loader.py

## TODO
* Loading:
  * Hydration of tweet ids lists.
  * Normalize http/https in urls.
* Limiting:
  * Limit by mention user ids, screen names
  * Limit by user ids, screen names
  * Limit by verified users
* Scroll additional sample tweets
* Dataset derivatives:
  * For local users, generate CSV with key fields.
  * Additional top derivatives:
    * URL
    * Quotes/retweets
  * Options to limit top derivatives by:
    * Top number (e.g., top 500)
    * Count greater than (e.g., more than 5 mentions)
  * Additional nodes/edges derivatives:
    * Replies
    * Quotes/retweets
  * Provide nodes/edges in additional formats such as Gephi.
* Separate counts of tweets available for public / local on home page.
