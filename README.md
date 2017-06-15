# TweetSets
Service for creating subsets of existing Twitter research datasets

Must create:
/tweetset_data/redis
/tweetset_data/datasets
/tweetset_data/elasticsearch/esdata1<2, 3, 4 ...>
esdata? and redis and datasets must be ugo+rwx
On Linux, esdata? must be owned by 1000:1000

From redis:
redis_1            | 1:M 14 Jun 01:53:54.763 # WARNING overcommit_memory is set to 0! Background save may fail under low memory condition. To fix this issue add 'vm.overcommit_memory = 1' to /etc/sysctl.conf and then reboot or run the command 'sysctl vm.overcommit_memory=1' for this to take effect.

Also, document ES prereqs.

TODO:
* Write install instructions
* Add hydration to loading
* Save recent datasets in cookie
* Scroll additional sample tweets
* Limit by mention user ids, screen names
* Limit by user ids, screen names
* Limit by verified users
* Export mention counts options (top #, > mentions than)
* Export edges in gephi, additional formats
