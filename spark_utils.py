from itertools import compress
from pyspark.sql.types import *
from pyspark.sql.functions import col, explode
import pyspark.sql.functions as F
import json
from twarc import json2csv

def make_spark_df(spark, schema, sql, path_to_dataset, dataset_id):
    '''Loads a set of JSON Tweets and applies the SQL transform.
    
    :param spark: an initialized SparkSession object
    :param schema: a valid Spark DataFrame schema for loading a Tweet from JSONL
    :param sql: Spark SQL to execute against the DataFrame
    :param path_to_dataset: a comma-separated list of JSON files to load
    :param dataset_id: a string containing the ID for this dataset'''
    # Read JSON files as Spark DataFrame
    df = spark.read.schema(schema).json(path_to_dataset)
    # Add the full Tweet JSON as a separate field
    # Option for Spark v3 to write null fields as nulls (not skip)
    df = df.withColumn("tweet", F.to_json(F.struct([df[x] for x in df.columns]), {'ignoreNullFields': 'false'}))
    df.createOrReplaceTempView("tweets")
    # Apply SQL transform
    df = spark.sql(sql)
    # Drop temporary columns
    cols_to_drop = ['tweet_urls','quoted_status', 'retweeted_status', 'text_str']
    df = df.drop(*cols_to_drop)
    # Add dataset ID column and return
    return df.withColumn('dataset_id', F.lit(dataset_id))

def load_schema(path_to_schema):
    '''Load TweetSets Spark DataFrame schema
    :param path_to_schema: path to a Spark schema JSON document on disk'''
    with open(path_to_schema, 'r') as f:
        schema = json.load(f)
        return StructType.fromJson(schema)

def load_sql(path_to_sql):
    '''Load Spark SQL code for TweetSets data transform
    :path_to_sql: path to Spark SQL code on disk'''
    with open(path_to_sql, 'r') as f:
        return f.read()

def extract_tweet_ids(df, path_to_extract):
    '''Saves Tweet ID's from a dataset to the provided path as zipped CSV files.
    :param df: Spark DataFrame
    :parm path_to_extract: string of path to folder for files'''
    # Extract ID column and save as zipped CSV
    df.select('tweet_id').write.option("header", "true").csv(path_to_extract, compression='gzip')
    
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
                        'retweeted_quoted_user_id': 'retweet_or_quote_user_id'
                    }
    # Add remaining fields from the DataFrame if they are used by json2csv
    column_mapping.update({k: k for k in df_columns if k in json2csv.get_headings()})
    # Set array flag for those fields that require it
    column_mapping = {k: (v, True if k in array_fields else False) for k,v in column_mapping.items()}
    return column_mapping


def extract_csv(df, path_to_extract):
    '''Creates CSV extract where each row is a Tweet document, using the schema in the twarc.json2csv module.
    :param df: Spark DataFrame
    :parm path_to_extract: string of path to folder for files'''
    column_mapping = make_column_mapping(df.columns, ['text', 'hashtags', 'urls'])
    #print('COLUMN MAPPING', column_mapping)
    for k, v in column_mapping.items():
        # Need convert fields stored as arrays
        if v[1]:
            # Concat arrays with whitespace
            df = df.withColumn(k, F.concat_ws(' ', df[k]))
            # rename columns as necessary
        if k != v[0]:
            df = df.withColumnRenamed(k, v[0])
    # We select only the columns identified in json2csv, skipping the user_urls column (which may have been deprecated)
    csv_columns = [c for c in json2csv.get_headings() if c != 'user_urls']
    df_csv = df.select(csv_columns)
    df_csv = df_csv.withColumn('text', F.regexp_replace('text', '\n|\r', ' '))
    df_csv = df_csv.withColumn('user_location', F.regexp_replace('user_location', '\n|\r', ' '))
    # Swap back the date fields so that the created_at field contains the unparsed version  
    data_mapping = {'created_at': 'parsed_created_at',
          'parsed_created_at': 'created_at'}
    df_csv = df_csv.select([F.col(c).alias(data_mapping.get(c, c)) for c in df_csv.columns])
    # Setting the escape character to the double quote. Otherwise, it causes problems for applications reading the CSV.
    df_csv.write.option("header", "true").csv(path_to_extract, compression='gzip', escape='"')

def extract_mentions(df, spark, path_to_extract):
    '''Creates nodes and edges of full mentions extract.
    :param df: Spark DataFrame (after SQL transform)
    :param spark: SparkSession object
    :param path_to_extract: string of path to folder for files'''
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
    mention_nodes.write.option("header", "true").csv(path_to_extract + '/nodes', compression='gzip')
    mention_edges.write.option("header", "true").csv(path_to_extract + '/edges', compression='gzip')
    

def agg_mentions(df, spark, path_to_extract):
    '''Creates count of Tweets per mentioned user id.
    :param df: Spark DataFrame (after SQL transform)
    :param spark: SparkSession object
    :parm path_to_extract: string of path to folder for files'''
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
    '''
    mentions_agg_df = spark.sql(sql_agg)  
    mentions_agg_df.write.option("header", "true").csv(path_to_extract + '/top_mentions', compression='gzip')
  


