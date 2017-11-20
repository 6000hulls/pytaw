import googleapiclient.discovery


class YouTube(object):

    def __init__(self, key):
        self.key = key
        self.youtube = googleapiclient.discovery.build(
            'youtube',
            'v3',
            developerKey=self.key,
            cache_discovery=False
        )
