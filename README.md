# TweetSets
Twitter datasets for research and archiving.

* Create your own Twitter dataset from existing, public datasets.
* Conforms with Twitter policies.

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
6. Build the containers:

        docker-compose build
7. Bring up the containers:

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

        python dataset_loader.py dataset
    
To see other loader commands:

        python dataset_loader.py

## TODO
* Write install instructions
* Add hydration to loading
* Save recent datasets in cookie
* Scroll additional sample tweets
* Limit by mention user ids, screen names
* Limit by user ids, screen names
* Limit by verified users
* Export mention counts options (top #, > mentions than)
* Export edges in gephi, additional formats
