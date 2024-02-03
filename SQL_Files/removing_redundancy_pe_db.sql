SELECT COUNT(*) FROM base_tweets;

SELECT COUNT(*) FROM base_tweets;

/* #### Tweet User Table 2 #### */

/* Checking count and count distinct */
SELECT (SELECT COUNT(*) FROM tweet_user) AS tweet_user_count, (SELECT COUNT(DISTINCT id) FROM tweet_user) AS tweet_user_distinct;

/* Checking duplicates count */
SELECT COUNT(*) FROM (SELECT id, COUNT(id) FROM tweet_user GROUP BY id HAVING COUNT(id)>1) subquery;

ALTER TABLE base_tweets ADD COLUMN tweet_user_id bigint;

UPDATE base_tweets SET tweet_user_id = tweet_user.id FROM tweet_user WHERE base_tweets.tweet_id = tweet_user.tweet_id;

/* Deleting duplicates by retaining latest*/
DELETE FROM tweet_user a USING tweet_user b WHERE a.tweet_id < b.tweet_id AND a.id = b.id;

/* Delete all Duplicates*/
DELETE FROM tweet_user a USING (SELECT MIN(ctid) AS ctid, tweet_id FROM tweet_user GROUP BY tweet_id HAVING COUNT(*) > 1) b WHERE a.tweet_id = b.tweet_id  AND a.ctid <> b.ctid;

/* Confirming duplicates are all removed */
SELECT (SELECT COUNT(*) FROM tweet_user) AS tweet_user_count, (SELECT COUNT(DISTINCT id) FROM tweet_user) AS tweet_user_distinct;

/* Creating relationships */
ALTER TABLE tweet_user ADD CONSTRAINT tweet_user_pk PRIMARY KEY (id);
ALTER TABLE base_tweets ADD CONSTRAINT base_user_fk FOREIGN KEY (tweet_user_id) REFERENCES tweet_user (id);


/*    ##### Quoted User Table 3 ##### */

SELECT (SELECT COUNT(*) FROM quoted_user) AS quoted_user_count, (SELECT COUNT(DISTINCT id) FROM quoted_user) AS quoted_user_distinct;

/* Checking duplicates count */
SELECT COUNT(*) FROM (SELECT id, COUNT(id) FROM quoted_user GROUP BY id HAVING COUNT(id)>1) subquery;

ALTER TABLE base_tweets ADD COLUMN quoted_user_id bigint;

UPDATE base_tweets SET quoted_user_id = quoted_user.id FROM quoted_user WHERE base_tweets.tweet_id = quoted_user.tweet_id;

DELETE FROM quoted_user a USING quoted_user b WHERE a.tweet_id < b.tweet_id AND a.id = b.id;

delete from quoted_user a using (select min(ctid) as ctid, tweet_id from quoted_user group by tweet_id having count(*) > 1) b where a.tweet_id = b.tweet_id  and a.ctid <> b.ctid;

ALTER TABLE quoted_user ADD CONSTRAINT quoted_user_pk PRIMARY KEY (id);

ALTER TABLE base_tweets ADD CONSTRAINT quoted_user_fk FOREIGN KEY (quoted_user_id) REFERENCES quoted_user (id);


/* #### Retweeted User: Table 4  #### */

/* Checking count and count distinct */
SELECT (SELECT COUNT(*) FROM retweeted_user) AS retweeted_user_count, (SELECT COUNT(DISTINCT id) FROM retweeted_user) AS retweeted_user_distinct;

/* Checking duplicates count */
SELECT COUNT(*) FROM (SELECT id, COUNT(id) FROM retweeted_user GROUP BY id HAVING COUNT(id)>1) subquery;

ALTER TABLE base_tweets ADD COLUMN retweeted_user_id bigint;

UPDATE base_tweets SET retweeted_user_id = retweeted_user.id FROM retweeted_user WHERE base_tweets.tweet_id = retweeted_user.tweet_id;

DELETE FROM retweeted_user a USING retweeted_user b WHERE a.tweet_id < b.tweet_id AND a.id = b.id;

SELECT id, COUNT(id) FROM retweeted_user GROUP BY id HAVING COUNT(id)>1;

delete from retweeted_user a using (select min(ctid) as ctid, tweet_id from retweeted_user group by tweet_id having count(*) > 1) b where a.tweet_id = b.tweet_id  and a.ctid <> b.ctid;

ALTER TABLE retweeted_user ADD CONSTRAINT retweeted_user_pk PRIMARY KEY (id);

ALTER TABLE base_tweets ADD CONSTRAINT retweeted_user_fk FOREIGN KEY (retweeted_user_id) REFERENCES retweeted_user (id);


/* #### Retweeted Tweet: Table 5 #### */

/* Checking count and count distinct */
SELECT (SELECT COUNT(*) FROM retweeted_tweet) AS retweeted_tweet_count, (SELECT COUNT(DISTINCT tweet_id) FROM retweeted_tweet) AS retweeted_tweet_distinct;

/* Checking duplicates count */
SELECT COUNT(*) FROM (SELECT tweet_id, COUNT(tweet_id) FROM retweeted_tweet GROUP BY tweet_id HAVING COUNT(tweet_id)>1) subquery;

ALTER TABLE base_tweets ADD COLUMN retweeted_tweet_id bigint;

UPDATE base_tweets SET retweeted_tweet_id = retweeted_tweet.tweet_id FROM retweeted_tweet WHERE base_tweets.tweet_id = retweeted_tweet.tweet_id;

SELECT tweet_id, COUNT(tweet_id) FROM retweeted_tweet GROUP BY tweet_id HAVING COUNT(tweet_id)>1;

delete from retweeted_tweet a using (select min(ctid) as ctid, tweet_id from retweeted_tweet group by tweet_id having count(*) > 1) b where a.tweet_id = b.tweet_id  and a.ctid <> b.ctid;

ALTER TABLE retweeted_tweet ADD CONSTRAINT retweeted_tweet_pk PRIMARY KEY (tweet_id);

ALTER TABLE base_tweets ADD CONSTRAINT retweeted_tweet_fk FOREIGN KEY (retweeted_tweet_id) REFERENCES retweeted_tweet (tweet_id);


/* #### Extended Tweets: Table 6 #### */

/* Checking count and count distinct */
SELECT (SELECT COUNT(*) FROM extended_tweets) AS extended_tweets_count, (SELECT COUNT(DISTINCT tweet_id) FROM extended_tweets) AS extended_tweets_distinct;

/* Checking duplicates count */
SELECT COUNT(*) FROM (SELECT tweet_id, COUNT(tweet_id) FROM extended_tweets GROUP BY tweet_id HAVING COUNT(tweet_id)>1) subquery;

ALTER TABLE base_tweets ADD COLUMN extended_tweets_id bigint;

UPDATE base_tweets SET extended_tweets_id = extended_tweets.tweet_id FROM extended_tweets WHERE base_tweets.tweet_id = extended_tweets.tweet_id;

SELECT tweet_id, COUNT(tweet_id) FROM extended_tweets GROUP BY tweet_id HAVING COUNT(tweet_id)>1;

delete from extended_tweets a using (select min(ctid) as ctid, tweet_id from extended_tweets group by tweet_id having count(*) > 1) b where a.tweet_id = b.tweet_id  and a.ctid <> b.ctid;

ALTER TABLE extended_tweets ADD CONSTRAINT extended_tweets_pk PRIMARY KEY (tweet_id);

ALTER TABLE base_tweets ADD CONSTRAINT extended_tweets_fk FOREIGN KEY (extended_tweets_id) REFERENCES extended_tweets (tweet_id);


/* #### Coordinates: Table 7 #### */

/* Checking count and count distinct */
SELECT (SELECT COUNT(*) FROM coordinates) AS coordinates_count, (SELECT COUNT(DISTINCT tweet_id) FROM coordinates) AS coordinates_distinct;

/* Checking duplicates count */
SELECT COUNT(*) FROM (SELECT tweet_id, COUNT(tweet_id) FROM coordinates GROUP BY tweet_id HAVING COUNT(tweet_id)>1) subquery;

ALTER TABLE base_tweets ADD COLUMN coordinates_id bigint;

UPDATE base_tweets SET coordinates_id = coordinates.tweet_id FROM coordinates WHERE base_tweets.tweet_id = coordinates.tweet_id;

SELECT tweet_id, COUNT(tweet_id) FROM coordinates GROUP BY tweet_id HAVING COUNT(tweet_id)>1;

delete from coordinates a using (select min(ctid) as ctid, tweet_id from coordinates group by tweet_id having count(*) > 1) b where a.tweet_id = b.tweet_id  and a.ctid <> b.ctid;

ALTER TABLE coordinates ADD CONSTRAINT coordinates_pk PRIMARY KEY (tweet_id);

ALTER TABLE base_tweets ADD CONSTRAINT coordinates_fk FOREIGN KEY (coordinates_id) REFERENCES coordinates (tweet_id);


/* #### Quoted Tweets: Table 8 #### */

/* Checking count and count distinct */
SELECT (SELECT COUNT(*) FROM quoted_tweets) AS quoted_tweets_count, (SELECT COUNT(DISTINCT tweet_id) FROM quoted_tweets) AS quoted_tweets_distinct;

/* Checking duplicates count */
SELECT COUNT(*) FROM (SELECT tweet_id, COUNT(tweet_id) FROM quoted_tweets GROUP BY tweet_id HAVING COUNT(tweet_id)>1) subquery;

ALTER TABLE base_tweets ADD COLUMN quoted_tweets_id bigint;

UPDATE base_tweets SET quoted_tweets_id = quoted_tweets.tweet_id FROM quoted_tweets WHERE base_tweets.tweet_id = quoted_tweets.tweet_id;

SELECT tweet_id, COUNT(tweet_id) FROM quoted_tweets GROUP BY tweet_id HAVING COUNT(tweet_id)>1;

delete from quoted_tweets a using (select min(ctid) as ctid, tweet_id from quoted_tweets group by tweet_id having count(*) > 1) b where a.tweet_id = b.tweet_id  and a.ctid <> b.ctid;

ALTER TABLE quoted_tweets ADD CONSTRAINT quoted_tweets_pk PRIMARY KEY (tweet_id);

ALTER TABLE base_tweets ADD CONSTRAINT quoted_tweets_fk FOREIGN KEY (quoted_tweets_id) REFERENCES quoted_tweets (tweet_id);


/* #### Hashtags: Table 9 */

/* Checking count and count distinct */
SELECT (SELECT COUNT(*) FROM hashtags) AS hashtags_count, (SELECT COUNT(DISTINCT tweet_id) FROM hashtags) AS hashtags_distinct;

/* Checking duplicates count */
SELECT COUNT(*) FROM (SELECT tweet_id, COUNT(tweet_id) FROM hashtags GROUP BY tweet_id HAVING COUNT(tweet_id)>1) subquery;

ALTER TABLE base_tweets ADD COLUMN hashtags_id bigint;

UPDATE base_tweets SET hashtags_id = hashtags.tweet_id FROM hashtags WHERE base_tweets.tweet_id = hashtags.tweet_id;

SELECT tweet_id, COUNT(tweet_id) FROM hashtags GROUP BY tweet_id HAVING COUNT(tweet_id)>1;

delete from hashtags a using (select min(ctid) as ctid, tweet_id from hashtags group by tweet_id having count(*) > 1) b where a.tweet_id = b.tweet_id  and a.ctid <> b.ctid;

ALTER TABLE hashtags ADD CONSTRAINT hashtags_pk PRIMARY KEY (tweet_id);

ALTER TABLE base_tweets ADD CONSTRAINT hashtags_fk FOREIGN KEY (hashtags_id) REFERENCES hashtags (tweet_id);


/* #### User Mentions: Table 10 #### */

/* Checking count and count distinct */
SELECT (SELECT COUNT(*) FROM user_mentions) AS user_mentions_count, (SELECT COUNT(DISTINCT id) FROM user_mentions) AS user_mentions_distinct;

/* Checking duplicates count */
SELECT COUNT(*) FROM (SELECT id, COUNT(id) FROM user_mentions GROUP BY id HAVING COUNT(id)>1) subquery;

ALTER TABLE base_tweets ADD COLUMN user_mentions_id character varying;

/*
ALTER TABLE base_tweets DROP COLUMN user_mentions_id;
SELECT * FROM user_mentions LIMIT 100;
*/

UPDATE base_tweets SET user_mentions_id = user_mentions.id FROM user_mentions WHERE base_tweets.tweet_id = user_mentions.tweet_id;

DELETE FROM user_mentions a USING user_mentions b WHERE a.tweet_id < b.tweet_id AND a.id = b.id;

SELECT id, COUNT(id) FROM user_mentions GROUP BY id HAVING COUNT(id)>1;

delete from user_mentions a using (select min(ctid) as ctid, tweet_id from user_mentions group by tweet_id having count(*) > 1) b where a.tweet_id = b.tweet_id  and a.ctid <> b.ctid;

ALTER TABLE user_mentions ADD CONSTRAINT user_mentions_pk PRIMARY KEY (id);

ALTER TABLE base_tweets ADD CONSTRAINT user_mentions_fk FOREIGN KEY (user_mentions_id) REFERENCES user_mentions (id);



/* #### Place: Table 11 #### */

/* Checking count and count distinct */
SELECT (SELECT COUNT(*) FROM place) AS place_count, (SELECT COUNT(DISTINCT id) FROM place) AS place_distinct;

/* Checking duplicates count */
SELECT COUNT(*) FROM (SELECT id, COUNT(id) FROM place GROUP BY id HAVING COUNT(id)>1) subquery;

SELECT * FROM place LIMIT 100;

ALTER TABLE base_tweets ADD COLUMN place_id character varying;

UPDATE base_tweets SET place_id = place.id FROM place WHERE base_tweets.tweet_id = place.tweet_id;

DELETE FROM place a USING place b WHERE a.tweet_id < b.tweet_id AND a.id = b.id;

SELECT id, COUNT(id) FROM place GROUP BY id HAVING COUNT(id)>1;

/*delete from place a using (select min(ctid) as ctid, tweet_id from place group by tweet_id having count(*) > 1) b where a.tweet_id = b.tweet_id  and a.ctid <> b.ctid; */

ALTER TABLE place ADD CONSTRAINT place_pk PRIMARY KEY (id);

ALTER TABLE base_tweets ADD CONSTRAINT place_fk FOREIGN KEY (place_id) REFERENCES place (id);


/* Base Tweets Table */
/* Checking count and count distinct */
SELECT (SELECT COUNT(*) FROM base_tweets) AS base_tweets_count, (SELECT COUNT(DISTINCT tweet_id) FROM base_tweets) AS base_tweets_distinct;

/* Checking duplicates count */
SELECT COUNT(*) FROM (SELECT tweet_id, COUNT(tweet_id) FROM base_tweets GROUP BY tweet_id HAVING COUNT(tweet_id)>1) subquery;
/* Deleting string duplicates in Postgresql */



