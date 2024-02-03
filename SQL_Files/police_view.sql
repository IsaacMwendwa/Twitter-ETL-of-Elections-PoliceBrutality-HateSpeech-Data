CREATE VIEW police_topics_view AS 
	SELECT hashtag, retweeted_id, quoted_id,
		CASE	WHEN hashtag ~~* '%PoliceBrutalityke%' THEN 'PoliceBrutalityke'
				WHEN hashtag ~~* '%EngageTheIG%' THEN 'EngageTheIG'
				WHEN hashtag ~~* '%MissingVoicesKE%' THEN 'MissingVoicesKE'
				WHEN hashtag ~~* '%IG_NPS%' THEN 'IG_NPS'
				WHEN hashtag ~~* '%NPSC_KE%' THEN 'NPSC_KE'
				WHEN hashtag ~~* '%NPSOfficial_KE%' THEN 'NPSOfficial_KE'
				WHEN hashtag ~~* '%DCI_Kenya%' THEN 'DCI_Kenya'
				WHEN hashtag ~~* '%APSKenya%' THEN 'APSKenya'
				WHEN hashtag ~~* '%WakoWapi%' THEN 'WakoWapi'
				WHEN hashtag ~~* '%CrimeWatch254%' THEN 'CrimeWatch254'
				WHEN hashtag ~~* '%Ministry of Interior%' THEN 'Ministry of Interior'
				WHEN hashtag ~~* '%ODPP_KE%' THEN 'ODPP_KE'
				WHEN hashtag ~~* '%PrisonsKe%' THEN 'PrisonsKe'
				WHEN hashtag ~~* '%Kenya Prisons Service%' THEN 'Kenya Prisons Service'
				ELSE NULL END AS police_topic,
		CASE 	WHEN quoted_id IS NOT NULL THEN True
				ELSE False END AS if_quoted,
		CASE 	WHEN retweeted_id IS NOT NULL THEN True
			ELSE False END AS if_retweeted
	FROM(
		SELECT *
		FROM base_tweets
		JOIN hashtags
		ON base_tweets.tweet_id = hashtags.tweet_id) AS base_hash
	LEFT JOIN retweeted_tweet
		ON base_hash.retweeted_tweet_id = retweeted_tweet.retweeted_id
	LEFT JOIN quoted_tweets
		ON base_hash.quoted_tweets_id = quoted_tweets.quoted_id;



SELECT COUNT(*) FROM police_topics_view WHERE hashtag LIKE '%EngageTheIG%';
SELECT COUNT(*) FROM police_topics_view WHERE hashtag LIKE '%PoliceBrutalityke%';
SELECT COUNT(*) FROM police_topics_view WHERE hashtag LIKE '%@IG_NPS%';
