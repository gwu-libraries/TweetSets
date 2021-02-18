# TweetSets
[![DOI](https://zenodo.org/badge/91485768.svg)](https://zenodo.org/badge/latestdoi/91485768)

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

### Installation for non-cluster ElasticSearch
1. Create data directories on a volume with adequate storage:

        mkdir -p /tweetset_data/redis
        mkdir -p /tweetset_data/datasets
        mkdir -p /tweetset_data/elasticsearch/esdata1
        mkdir -p /tweetset_data/elasticsearch/esdata2
        chown -R 1000:1000 /tweetset_data/elasticsearch

Note:
* Create an `esdata<number>` directory for each ElasticSearch container.
* On OS X, the `redis` and `esdata<number>` directories must be `ugo+rwx`.

2. Create a directory, to be named as you choose, where tweet data files will be stored for loading.

        mkdir /dataset_loading

2. Clone or download this repository:

        git clone https://github.com/gwu-libraries/TweetSets.git
        
3. Change to the `docker` directory:
   
        cd docker

4. Copy the example docker files:

        cp example.docker-compose.yml docker-compose.yml
        cp example.env .env

5. Edit `.env`. This file is annotated to help you select appropriate values.
6. Create `dataset_list_msg.txt` in the docker directory. The contents of this file will be displayed on the dataset list page. It can 
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
        mkdir -p /tweetset_data/full_datasets
        mkdir -p /tweetset_data/elasticsearch
        chown -R 1000:1000 /tweetset_data/elasticsearch

2. Create a directory, to be named as you choose, where tweet data files will be stored for loading. 

        mkdir /dataset_loading

2. Clone or download this repository:

        git clone https://github.com/gwu-libraries/TweetSets.git
        
3. Change to the `docker` directory:
   
        cd docker

4. Copy the example docker files:

        cp example.cluster-primary.docker-compose.yml docker-compose.yml
        cp example.env .env

5. Edit `.env`. This file is annotated to help you select appropriate values.
6. Create `dataset_list_msg.txt` in the docker directory. The contents of this file will be displayed on the dataset list page. It can 
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

        git clone https://github.com/gwu-libraries/TweetSets.git
        
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
1. Create a dataset directory within the dataset filepath configured in your `.env`.
2. Place tweet files in the directory. The tweet files can be line-oriented JSON (.json) or gzip compressed
line-oriented JSON (.json.gz).
3. Create a dataset description file in the directory named `dataset.json`. See `example.dataset.json` for
the format of the file.

### Loading
Use this method when Elasticsearch is on the same machine as TweetSets (non-cluster option), or for otherwise loading without using Spark.
1. Start and connect to a loader container:

        docker-compose run --rm loader /bin/bash
2. Invoke the loader:

        python tweetset_loader.py create /dataset/path/to
        
To see other loader commands:

        python tweetset_loader.py

Note that tweets are never added to an existing index. When using the `reload` command, a new index is created
for a dataset that replaces the existing index. The new index replaces the old index only after the new index
has been created, so users are not affected by reloading.

### Loading with Apache Spark
When using the Spark loader, the dataset files must be located at the dataset filepath on all nodes 
(e.g., by having separate copies or using a network share such as [NFS](https://www.digitalocean.com/community/tutorials/how-to-set-up-an-nfs-mount-on-ubuntu-16-04)).

In general, using Spark withing Docker is tricky because the Spark driver, Spark master, and Spark
nodes all need to be able to communicate and the ports are dynamically selected. (Some of the ports
can be fixed, but supporting multiple simultaneous loaders requires leaving some dynamic.) This
doesn't play well with Docker's port mapping, since the hostnames and ports that Spark advertises internally
must match what is available through Docker. Further complicating this is that host networking (which is
used to support the dynamic ports) does not work correctly on Mac. Use the regular loader rather than the Spark
loader Elasticsearch is on the same machine as TweetSets (e.g., in a small development environment, not a cluster). 

#### Cluster mode
1. Start and connect to a loader container:

        docker-compose -f loader.docker-compose.yml run --rm loader /bin/bash
2. Invoke the loader:

        spark-submit \
        --jars elasticsearch-hadoop.jar \
        --master spark://$SPARK_MASTER_HOST:7101 \
        --py-files dist/TweetSets-2.1-py3.6.egg,dependencies.zip \
        --conf spark.driver.bindAddress=0.0.0.0 \
        --conf spark.driver.host=$SPARK_DRIVER_HOST \
        tweetset_loader.py spark-create /dataset/path/to

### Reloading an existing set with Apache Spark
1. Start and connect to a loader container:

        docker-compose -f loader.docker-compose.yml run --rm loader /bin/bash
2. Invoke the loader:

        spark-submit \
        --jars elasticsearch-hadoop.jar \
        --master spark://$SPARK_MASTER_HOST:7101 \
        --py-files dist/TweetSets-2.0-py3.6.egg,dependencies.zip \
        --conf spark.driver.bindAddress=0.0.0.0 \
        --conf spark.driver.host=$SPARK_DRIVER_HOST \
        tweetset_loader.py spark-reload dataset-id /dataset/path/to

where `dataset-id` is the id of the dataset, which can be found by viewing the collection's `ID` metadata field via the Tweetsets UI.

Note that running `spark-reload` does *not* re-read `dataset.json` and update the dataset descriptive metadata.
To update the dataset descriptive metadata to match `dataset.json` if it has been changed,
invoke the loader with an `update` command:

        spark-submit \
        --jars elasticsearch-hadoop.jar \
        --master spark://$SPARK_MASTER_HOST:7101 \
        --py-files dist/TweetSets-2.0-py3.6.egg,dependencies.zip \
        --conf spark.driver.bindAddress=0.0.0.0 \
        --conf spark.driver.host=$SPARK_DRIVER_HOST \
        tweetset_loader.py update dataset-id /dataset/path/to

### Creating a manual extract (dataset)
Full extracts of existing datasets can be created from the command line. 

1. Launch a shell session in the server container:
```
docker exec -it ts_server_1 /bin/bash
```
or 
```
docker exec -it ts_server-flaskrun_1 /bin/bash
```

2. Issue the command to create the extract, where `dataset-id` is the id of the dataset, which can be found by viewing the collection's `ID` metadata field via the Tweetsets UI.

```
flask create-extract dataset_id
```

3. Upon completion, an email will be sent to the address in the `ADMIN_EMAIL` field of the `.env` file.


## Kibana
Elastic's [Kibana](https://www.elastic.co/products/kibana) is a general-purpose framework for exploring, 
analyzing, and visualizing data. Since the tweets are already indexed in ElasticSearch, they are ready
to be used from Kibana.

To enable Kibana, uncomment the Kibana service in your `docker-compose.yml`. By default, Kibana will run on
port 5601.

A few notes about Kibana:
* When starting Kibana, the first step you will need to do is select an index pattern. Each index represents
a dataset, where the format of the name of the index is _tweets-<dataset id>_. The dataset id is available
under the dataset details when selecting source datasets in TweetSets.
* The time period of the tweets is controlled by the date picker on the top, right of the Kibana screen. By
default the time period is very short; you will probably want to adjust to cover a longer time period.

## Citing
Please cite TweetSets as:

        Justin Littman, Laura Wrubel, Dan Kerchner, Dolsy Smith, Will Bonnett. (2020). TweetSets. Zenodo. https://doi.org/10.5281/zenodo.1289426

## Development
### Unit tests
  Run outside the container. 
  
  `python -m unittest`

## Kibana TODO
* Consider multiple Kibana users.
* Consider persistence.
* Provide a default dashboard.
* Consider approaches to index patterns.

## TweetSets TODO
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
