
     with tweet_cte as (
         /* SCALAR VALUES */
         select 
            id_str as tweet_id,
            /* Assumes retweeted_status and quoted_status fields will be present only in Tweets of those types. */
            case when isnotnull(in_reply_to_status_id) then 'reply'
                when isnotnull(retweeted_status) then 'retweet'
                when isnotnull(quoted_status) then 'quote'
                else 'original'
            end as tweet_type,
            /* Assumes the full_text field will be populated whenever the extended_tweet field is present. */
            coalesce(extended_tweet.full_text, text) as text_str,
            in_reply_to_user_id_str as in_reply_to_user_id,
            in_reply_to_screen_name,
            in_reply_to_status_id_str as in_reply_to_status_id,
            /* Convert Twitter date string to parsed format for ES */
            date_format(to_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'),
                "yyyy-MM-dd'T'HH:mm:ssX") as created_at,
            user.id_str as user_id,
            user.screen_name as user_screen_name,
            user.followers_count as user_follower_count,
            user.verified as user_verified,
            user.lang as user_language,
            user.utc_offset as user_utc_offset,
            user.time_zone as user_time_zone,
            user.location as user_location,
            retweet_count,
            lang as language,
            /* retweeted_status will be null if not retweet */
            coalesce(retweeted_status.favorite_count, favorite_count) as favorite_count,
            /* STRUCT/ARRAY FIELDS */
            /* Should be null when Tweet is not a quote/retweet */
            quoted_status as quoted_status_struct,
            retweeted_status as retweeted_status_struct,
            /* Assuming the following are either null or populated. There may be more values in the extended_tweet.entities fields than in the root-level (Tested on 40 GB of Tweets). */
            coalesce(extended_tweet.entities.user_mentions, entities.user_mentions) as user_mentions_array,
            coalesce(extended_tweet.entities.hashtags, entities.hashtags) as hashtags_array,
            /* entities.media and extended_tweet.entities.media have different schema, so we can't coalesce */
            case 
                when isnotnull(extended_tweet.entities.media) then transform(extended_tweet.entities.media, x -> x.media_url_https)
                else transform(entities.media, x -> x.media_url_https)                
            end as media_array,
            /* Unlike those above, the URL array fields may be present as empty arrays, so we can't coalesce them. We need to use array intersection to get the URL's present in the fields under root and extended_tweet, since there may be unique values in each */
            case 
                when isnotnull(extended_tweet.entities.urls) 
                then array_union(transform(extended_tweet.entities.urls , x -> x.expanded_url), transform(entities.urls, x -> x.expanded_url)) 
                else transform(entities.urls, x -> x.expanded_url)
            end as urls_array,
            /* These are top level struct fields, so if present, this expression returns true */
            isnotnull(geo) or isnotnull(place) or isnotnull(coordinates) as has_geo,
            isnotnull(entities.media) or isnotnull(extended_tweet.entities.media) as has_media,
            /* CSV ONLY FIELDS */
            'https://twitter.com/' || user.screen_name || '/status/' || id_str as tweet_url,
            /*  This is the raw date; we switch this with the created_at column after loading to ES */
            created_at as parsed_created_at,  
            /* Array field to string */
            concat_ws(' ', coordinates.coordinates) as coordinates,
            place.full_name as place,
            possibly_sensitive,
            source,
            user.created_at as user_created_at,
            user.default_profile_image as user_default_profile_image,
            user.favourites_count as user_favourites_count,
            user.friends_count as user_friends_count,
            user.listed_count as user_listed_count,
            user.statuses_count as user_statuses_count,
            /* Remove newline characters for CSV */
            regexp_replace(user.description, '\n|\r', ' ') as user_description,
            regexp_replace(user.name, '\n|\r', ' ') as user_name,
            /* FULL JSON representation of Tweet */
            tweet
        from tweets)
        select 
            /* Convert text to array for ES indexing */
            case 
                when tweet_type = 'retweet' 
                /* Assumes the retweeted_status.extended_tweet.full_text field will be populated if present (i.e, never an empty string) */
                then array(coalesce(retweeted_status_struct.extended_tweet.full_text, retweeted_status_struct.text))
                else array(text_str)
            end as text,
            /* retweeted_status/quoted_status fields should be null if not populated */
            coalesce(retweeted_status_struct.user.id_str, quoted_status_struct.user.id_str) as retweeted_quoted_user_id,
            coalesce(retweeted_status_struct.user.screen_name, quoted_status_struct.user.screen_name) as retweeted_quoted_screen_name,
            coalesce(retweeted_status_struct.id_str, quoted_status_struct.id_str) as retweet_quoted_status_id,
            /* Extract values from arrays of structs */
            transform(user_mentions_array, x -> x.id_str) as mention_user_ids,
            transform(user_mentions_array, x -> x.screen_name) as mention_screen_names,
            transform(hashtags_array, x -> lower(x.text)) as hashtags,
            transform(urls_array, x -> lower(replace(x, 'https://', 'http://'))) as urls,
            /* Present in CSV only */
            concat_ws(' ', media_array) as media,
            /* CSV fields that differ from their ES representations */
            concat_ws(' ', urls_array) as urls_csv,
            concat_ws(' ', transform(hashtags_array, x -> x.text)) as hashtags_csv,
            *
        from tweet_cte