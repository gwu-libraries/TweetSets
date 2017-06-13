# TweetSets
Service for creating subsets of existing Twitter research datasets

Must create:
/tweetset_data/redis
/tweetset_data/datasets
/tweetset_data/elasticsearch/esdata1<2, 3, 4 ...>
esdata? and redis and datasets must be ugo+rwx

TODO:
* Dockerize
* Write install instructions
* Add hydration to loading
* Landing page
* Save recent datasets in cookie
* Scroll additional sample tweets
* Change tweet HTML cache to use redis
* Limit by mention user ids, screen names
* Limit by user ids, screen names
* Limit by verified users
* Export mention counts
* Export edges in gephi, additional formats