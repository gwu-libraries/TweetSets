import pyspark.sql.types as T
from pyspark.sql.functions import col, explode
import pyspark.sql.functions as F
from pyspark.sql import Row
import json
from twarc import json2csv

def load_schema(path_to_schema):
    '''Load TweetSets Spark DataFrame schema
    :param path_to_schema: path to a Spark schema JSON document on disk'''
    with open(path_to_schema, 'r') as f:
        schema = json.load(f)
        return T.StructType.fromJson(schema)

def load_sql(path_to_sql):
    '''Load Spark SQL code for TweetSets data transform
    :path_to_sql: path to Spark SQL code on disk'''
    with open(path_to_sql, 'r') as f:
        return f.read()

def make_spark_df(spark, schema, sql, path_to_dataset, dataset_id):
    '''Loads a set of JSON tweets and applies the SQL transform.
    
    :param spark: an initialized SparkSession object
    :param schema: a valid Spark DataFrame schema for loading a tweet from JSONL
    :param sql: Spark SQL to execute against the DataFrame
    :param path_to_dataset: a comma-separated list of JSON files to load
    :param dataset_id: a string containing the ID for this dataset'''
    # Read JSON files as Spark DataFrame
    #df = load_rdd(spark, tweet_schema=schema, path_to_tweets=path_to_dataset)
    df = spark.read.schema(schema).json(path_to_dataset)
    # Create a temp view for the SQL ops
    df.createOrReplaceTempView("tweets")
    # Apply SQL transform
    df = spark.sql(sql)
    # Drop temporary columns
    cols_to_drop = [c for c in df.columns if c.endswith('struct') or c.endswith('array') or c.endswith('str')]
    df = df.drop(*cols_to_drop)
    # Add dataset ID column 
    df = df.withColumn('dataset_id', F.lit(dataset_id))
    return df

def extract_tweet_ids(df, path_to_extract):
    '''Saves Tweet ID's from a dataset to the provided path as zipped CSV files.
    :param df: Spark DataFrame
    :parm path_to_extract: string of path to folder for files'''
    # Extract ID column and save as zipped CSV
    df.select('tweet_id').distinct().coalesce(14).write.option("header", "true").csv(path_to_extract, compression='gzip')
    
def extract_tweet_json(df, path_to_extract):
    '''Saves Tweet JSON documents from a dataset to the provided path as zipped JSON files.
    :param df: Spark DataFrame
    :parm path_to_extract: string of path to folder for files'''
    # Extract ID column and save as zipped JSON
    df.select('tweet').write.text(path_to_extract,compression='gzip')
    
def make_column_mapping(df_columns, array_fields):
    '''Creates mapping from TweetSets fields to CSV column headings, using headings derived from twarc.json2csv. Each key is a column name in the DataFrame created from Tweet JSON by SQL transform; each value a tuple: the first element is the name of the CSV column heading, the second element is a Boolean flag indicating whether this field is an array. (Arrays need to be transformed to strings prior to writing to CSV.)
    :param df_columns: list of columns in the transformed Spark DataFrame (includes some fields required by json2csv not indexed in Elasticsearch)
    :param array_fields: list of fields in df_columns stored as arrays'''
    # Map TweetSets fields to their CSV column names
    column_mapping = {'retweet_quoted_status_id': 'retweet_or_quote_id',
                        'retweeted_quoted_screen_name': 'retweet_or_quote_screen_name',
                        'tweet_id': 'id',
                        'user_follower_count': 'user_followers_count',
                        'language': 'lang',
                        'retweeted_quoted_user_id': 'retweet_or_quote_user_id',
                        'hashtags_csv': 'hashtags',
                        'urls_csv': 'urls'
                    }
    # Add remaining fields from the DataFrame if they are used by json2csv
    column_mapping.update({k: k for k in df_columns if k in json2csv.get_headings()})
    # Set array flag for those fields that require it
    column_mapping = {k: (v, True if k in array_fields else False) for k,v in column_mapping.items()}
    return column_mapping

def extract_csv(df):
    '''Creates CSV extract where each row is a Tweet document, using the schema in the twarc.json2csv module.
    :param df: Spark DataFrame'''
    column_mapping = make_column_mapping(df.columns, array_fields=['text'])
    # The hashtags and urls fields are handled differently in the Elasticsearch index and in the CSV (per the twarc.json2csv spec). So we need to drop the ES columns before renaming the CSV-versions of these columns
    df = df.drop('hashtags', 'urls')
    for k, v in column_mapping.items():
        # Need to convert fields stored as arrays
        if v[1]:
            # Concat arrays with whitespace
            df = df.withColumn(k, F.concat_ws(' ', df[k]))
            # rename columns as necessary
        if k != v[0]:
            df = df.withColumnRenamed(k, v[0])
    # We select only the columns identified in json2csv, skipping the user_urls column (which may have been deprecated)
    csv_columns = [c for c in json2csv.get_headings() if c != 'user_urls']
    df_csv = df.select(csv_columns)
    # Remove newlines in the text and user_location fields
    df_csv = df_csv.withColumn('text', F.regexp_replace('text', '\n|\r', ' '))
    df_csv = df_csv.withColumn('user_location', F.regexp_replace('user_location', '\n|\r', ' '))
    # Swap back the date fields so that the created_at field contains the unparsed version  
    data_mapping = {'created_at': 'parsed_created_at',
          'parsed_created_at': 'created_at'}
    df_csv = df_csv.select([F.col(c).alias(data_mapping.get(c, c)) for c in df_csv.columns])
    # Setting the escape character to the double quote. Otherwise, it causes problems for applications reading the CSV.
    # Get rid of duplicate tweets
    df_csv = df_csv.dropDuplicates(['id'])
    return df_csv

def extract_mentions(df, spark):
    '''Creates nodes and edges of full mentions extract.
    :param df: Spark DataFrame (after SQL transform)
    :param spark: SparkSession object'''
    # Create a temp table so that we can use SQL
    df.createOrReplaceTempView("tweets_parsed")
    # SQL for extracting the mention ids, screen_names, and user_ids
    mentions_sql = '''
    select mentions.*,
        user_id
    from (
        select 
            explode(arrays_zip(mention_user_ids, mention_screen_names)) as mentions,
            user_id
        from tweets_parsed
    )
    '''
    mentions_df = spark.sql(mentions_sql)
    mention_edges = mentions_df.select('mention_user_ids', 'mention_screen_names')\
                        .distinct()
    mention_nodes = mentions_df.select('mention_user_ids', 'user_id').distinct()
    return mention_nodes, mention_edges

def agg_mentions(df, spark):
    '''Creates count of Tweets per mentioned user id.
    :param df: Spark DataFrame (after SQL transform)
    :param spark: SparkSession object'''
    df.createOrReplaceTempView("tweets_parsed")
    sql_agg = '''
    select count(distinct tweet_id) as number_mentions,
        mention_user_id as mentioned_user
    from (
        select 
            explode(mention_user_ids) as mention_user_id,
            tweet_id
        from tweets_parsed
    )
    group by mention_user_id
    order by count(distinct tweet_id) desc
    '''
    mentions_agg_df = spark.sql(sql_agg)  
    return mentions_agg_df

def agg_users(df, spark):
    '''Creates count of tweets per user id/screen name.
    :param df: Spark DataFrame (after SQL transform)
    :param spark: SparkSession object'''

    df.createOrReplaceTempView("tweets_parsed")
    sql_agg = '''
    select count(distinct tweet_id) as tweet_count,
        user_id,
        user_screen_name
    from tweets_parsed
    group by user_id, user_screen_name
    order by count(distinct tweet_id) desc
    '''
    return spark.sql(sql_agg)
