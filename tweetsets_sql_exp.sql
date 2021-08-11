
     with cte as (
         select id_str as tweet_id,
            case when isnotnull(in_reply_to_status_id) then 'reply'
                when isnotnull(retweeted_status) then 'retweet'
                when isnotnull(quoted_status) then 'quote'
                else 'original'
            end as tweet_type,
            coalesce(extended_tweet.full_text, text) as text_str,
            quoted_status,
            retweeted_status,
            in_reply_to_user_id_str as in_reply_to_user_id,
            in_reply_to_screen_name,
            in_reply_to_status_id_str as in_reply_to_status_id,
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
            transform(coalesce(extended_tweet.entities.user_mentions,
                                entities.user_mentions), x -> x.id_str) as mention_user_ids,
            transform(coalesce(extended_tweet.entities.user_mentions,
                                entities.user_mentions), x -> x.screen_name) as mention_screen_names,
            coalesce(retweeted_status.entities.hashtags, extended_tweet.entities.hashtags, entities.hashtags) as tweet_hashtags,
            coalesce(retweeted_status.favorite_count, favorite_count) as favorite_count,
            retweet_count,
            lang as language,
            isnotnull(entities.media) or isnotnull(extended_tweet.entities.media) 
                    as has_media,
            transform(coalesce(extended_tweet.entities.urls,
                        entities.urls), x -> coalesce(x.expanded_url, x.url)) as tweet_urls,
            isnotnull(geo) or isnotnull(place) or isnotnull(coordinates) as has_geo,
            /* The following are fields used only in the CSV representation of the Tweet */
            'https://twitter.com/' || user.screen_name || '/status/' || id_str as tweet_url,
            created_at as parsed_created_at,  /*  This is the raw date; we can switch this with the created_at column after loading to ES*/
            concat_ws(' ', coordinates.coordinates) as coordinates,
            transform(extended_entities.media, x -> x.media_url_https) as ext_media_urls,
            transform(entities.media, x -> x.media_url_https) as media_urls,
            place.full_name as place,
            possibly_sensitive,
            source,
            user.created_at as user_created_at,
            user.default_profile_image as user_default_profile_image,
            regexp_replace(user.description, '\n|\r', ' ') as user_description,
            user.favourites_count as user_favourites_count,
            user.friends_count as user_friends_count,
            user.listed_count as user_listed_count,
            regexp_replace(user.name, '\n|\r', ' ') as user_name,
            user.statuses_count as user_statuses_count,
            tweet
        from tweets)
        select 
            case when tweet_type = 'quote' then array(text_str, 
                                                    coalesce(quoted_status.extended_tweet.full_text,
                                                            quoted_status.text))
                when tweet_type = 'retweet' then array(coalesce(retweeted_status.extended_tweet.full_text,
                                                            retweeted_status.text))
                else array(text_str)
            end as text,
            coalesce(retweeted_status.user.id_str, quoted_status.user.id_str) as retweeted_quoted_user_id,
            coalesce(retweeted_status.user.screen_name, quoted_status.user.screen_name) as retweeted_quoted_screen_name,
            coalesce(retweeted_status.id_str, quoted_status.id_str) as retweet_quoted_status_id,
            transform(tweet_hashtags, x -> lower(x.text)) as hashtags,
            case when tweet_type = 'quote' then transform(filter(tweet_urls, x -> x not like 'https://twitter.com/%'),
                                                            x -> lower(replace(lower(x), 'https://', 'http://')))
                else transform(tweet_urls, x -> lower(replace(lower(x), 'https://', 'http://')))
            end as urls,
            /* Used by CSV */
            concat_ws(' ', coalesce(ext_media_urls, media_urls)) as media,
            concat_ws(' ', tweet_urls) as urls_csv,
            concat_ws(' ', transform(tweet_hashtags, x -> x.text)) as hashtags_csv,
            *
        from cte

