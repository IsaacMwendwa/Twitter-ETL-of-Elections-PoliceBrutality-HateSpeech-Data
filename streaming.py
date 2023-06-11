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


# Search Terms for Elections
keywords_to_hear = [
                    '#2022 general elections',
                    '#2022 presidential elections',
                    '#NCIC_Kenya',
                    '#ElectionsBilaNoma',
                    '#luo', '#luos',
                    '#kikuyu', '#kikuyus',
                    '#luhya', '#luhyas',
                    '#kalenjin', '#kalenjins',
                    '#kamba', '#kambas',
                    '#Ruto', '#Raila',
                    '#Kalonzo', '#Uhuru Kenyatta',
                    '#Moses Kuria', '#Aden Duale',
                    '#Miguna', '#Babu Owino',
                    '#Oscar Sudi', '#Aisha Jumwa',
                    '#Kipchumba Murkomen', '#Johnson Sakaja',
                    '#Millie Odhiambo', '#Hassan Joho',
                    '#Ndindi Nyoro', '#Charles Njagua',
                    '#kieleweke', '#Tanga Tanga',
                    '#AzimioLaUmoja',
                    '#Azimio', '#Azimio La Umoja',
                    '#Ufisadi Daima Alliance', '#Sheikh Ngao',
                    '#Juma Ngao',
                    '#PoliceBrutalityke',
                    '#EngageTheIG',
                    '#MissingVoicesKE',
                    '#IG_NPS',
                    '#NPSC_KE',
                    '#NPSOfficial_KE',
                    '#DCI_Kenya',
                    '#APSKenya',
                    '#WakoWapi',
                    '#CrimeWatch254',
                    '#Ministry of Interior', '#ODPP_KE',
                    '#PrisonsKe', '#Kenya Prisons Service'
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
