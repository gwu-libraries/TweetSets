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
6. Create `dataset_list_msg.txt`. The contents of this file will be displayed on the dataset list page. It can 
be used to list other datasets that are available, but not yet loaded. If leaving the file empty then:

        touch dataset_list_msg.txt

7. Bring up the containers:

        docker-compose up -d

For HTTPS support, uncomment and configure the nginx-proxy container in `docker-compose.yml`.

### Cluster installation
Clusters must have at least a primary node and two additional nodes.

#### Primary node
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

        cp example.cluster-primary.docker-compose.yml docker-compose.yml
        cp example.env .env

5. Edit `.env`. This file is annotated to help you select appropriate values.
6. Create `dataset_list_msg.txt`. The contents of this file will be displayed on the dataset list page. It can 
be used to list other datasets that are available, but not yet loaded. If leaving the file empty then:

        touch dataset_list_msg.txt

7. Bring up the containers:

        docker-compose up -d

For HTTPS support, uncomment and configure the nginx-proxy container in `docker-compose.yml`.

#### Cluster node
1. Create data directories on a volume with adequate storage:

        mkdir -p /tweetset_data/elasticsearch
        chown -R 1000:1000 /tweetset_data/elasticsearch

2. Clone or download this repository:

        git clone https://github.com/justinlittman/TweetSets.git
        
3. Change to the `docker` directory:
   
        cd docker
4. Copy the example docker files:

        cp example.cluster-node.docker-compose.yml docker-compose.yml
        cp example.cluster-node.env .env

5. Edit `.env`. This file is annotated to help you select appropriate values. Note that 2 cluster nodes
must have `MASTER` set to `true`.
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

        python tweetset_loader.py
        
### Adding to a dataset
Under the hood, each dataset has its own index. Each index is composed of a number of shards and that number is fixed 
when the index is created. TweetSets attempts to select the optimal number of shards based on the number
of tweets. If you add too many tweets to an existing dataset, you may cause sharding problems.

If the total number of tweets in a collection are less than 32.5 million when storing tweets or 138 million
when not storing tweets, then don't worry about sharding. You can use `python tweetset_loader.py tweets` to
add more tweets to an existing collection.

If the total number of tweets in a collection is greater than the above limits, then it is recommended to
delete the collection's index first with `python tweetset_loader truncate` and then reload all tweets with
`python tweetset_loader.py tweets`.

## TODO
* Loading:
  * Hydration of tweet ids lists.
* Limiting:
  * Limit by mention user ids
  * Limit by user ids
  * Limit by verified users
* Scroll additional sample tweets
* Dataset derivatives:
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
