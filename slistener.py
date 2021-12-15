# import packages
from tweepy.streaming import StreamListener
from tweepy import API
import json
import time
import sys

# inherit from StreamListener class
class SListener(StreamListener):
    
    def __init__(self, api = None, fprefix = 'police-and-elections-tweets'):
       #path of streamed  files
        path_json_files = '/home/imwendwa/analytics/policeAndElectionsTwitterScraping/streaming_json_files/'

        # define the filename with time as prefix
        self.api = api or API()
        self.counter = 0
        self.fprefix = fprefix
        self.output  = open(path_json_files + '%s-%s.json' % (self.fprefix, time.strftime('%Y%m%d-%H%M%S')), 'w')

    def on_data(self, data):
        if  'in_reply_to_status' in data:
            self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print("WARNING: %s" % warning['message'])
            return

    def on_status(self, status):
        path_json_files = '/home/imwendwa/analytics/policeAndElectionsTwitterScraping/streaming_json_files/'
        # if the number of tweets reaches 500
        # create a new file
        self.output.write(status)
        self.counter += 1
        if self.counter >= 500:
            self.output.close()
            self.output  = open(path_json_files + '%s-%s.json' % (self.fprefix, time.strftime('%Y%m%d-%H%M%S')), 'w')
            self.counter = 0
        return

    def on_delete(self, status_id, user_id):
        print(time.strftime('%Y%m%d-%H%M%S') + ":  Delete notice")
        return


    def on_limit(self, track):
        print(time.strftime('%Y%m%d-%H%M%S') + ":  WARNING: Limitation notice received, tweets missed: %d" % track)
        return


    def on_error(self, status_code):
        print(time.strftime('%Y%m%d-%H%M%S') + ':  Encountered error with status code:', status_code)
        return 


    def on_timeout(self):
        print(time.strftime('%Y%m%d-%H%M%S') + ":  Timeout, sleeping for 60 seconds...")
        time.sleep(60)
        return 