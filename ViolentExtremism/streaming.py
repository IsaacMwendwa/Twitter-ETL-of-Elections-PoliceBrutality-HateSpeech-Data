from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream
from slistener import SListener
from urllib3.exceptions import ProtocolError


# consumer (API) key authentication
consumer_key = 'YOUR CONSUMER KEY HERE'
consumer_secret = 'YOUR CONSUMER SECRET HERE'

auth = OAuthHandler(consumer_key, consumer_secret)


# access key authentication
access_token = 'YOUR ACCESS TOKEN HERE'
access_token_secret = 'YOUR ACCESS TOKEN SECRET HERE'

auth.set_access_token(access_token, access_token_secret)

#print(auth.access_token)


# set up the API with the authentication handler
api = API(auth)


# set up words to hear
keywords_to_hear = [
                    '#Al-Shabaab', '#Al-Shabab', '#jihadi',
                    '#Harakat al-Shabaab al-Mujahideen',
                    'al-Mujahideen',
                    '#Al-Qaeda',
                    '#ISIS',
                    '#Islamic State',
                    '#Daesh'
                    '#dci',
                    '#Kenya',
                    '#Somalia',
                    '#Ethiopia',
                    '#Uganda',
                    '#Tanzania',
                    '#South Sudan',
                    '#Mozambique',
                    '#Rwanda',
                    '#Horn of Africa',
                    '#jihad',
                    '#bombing',
                    '#bomb',
                    '#conflict',
                    '#conflict events',
                    '#counter-terrorism',
                    '#terrorism',
                    '#radicalization',
                    '#terror attacks',
                    '#terrorist attacks',
                    '#terrorism narratives',
                    '#Hizbu Tahreer',
                    '#Aboud Rogo', '#dying as shahid',
                    '#NONE TO BE WORSHIPPED EXCEPT ALLAH',
                    '#Al-ahli media',
                    '#Al Misbah', '#Al Hidaya', '#Gaidi Mtaani',
                    '#Ummu Shahl',
                    '#Al-Taubah', '#Al-Baqarah', '#Surah ya Chuma',
                    '#Al Bidaya wal Nihaya',
                    '#Ummul Sahl',
                    '#Al Bidaya wal Nihaya', '#Abdul Wahab',
                    '#Tazkiya',
                    '#Asha wa Maratib', '#Mama Totti', '#wife ya zuberi', '#Ali aka Don', '#Ahmed Belarus', '#Mama Ogada',
                    '#AMS Al Shabab Media site',
                    '#Majengo Muslim Youth Center', '#Musa aka Bamee',
                    '#Juma Osau', '#Juma Ayub',
                    '#Tahfeedh', '#Salim Kipofu'
                    ]

# instantiate the SListener object
listen = SListener(api)

# instantiate the stream object
stream = Stream(auth, listen)
#stream = Stream(consumer_key, consumer_secret, access_token, access_token_secret)


# begin collecting data
while True:
    # maintain connection unless interrupted
    try:
        stream.filter(track=keywords_to_hear)
    # reconnect automatically if error arise
    # due to unstable network connection
    except (ProtocolError, AttributeError):
        continue
