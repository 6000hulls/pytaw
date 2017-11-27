import math
import logging
import googleapiclient.discovery

from .utils import datetime_to_string, string_to_datetime


logger = logging.getLogger(__name__)

MAX_SEARCH_RESULTS = 1000


class YouTube(object):

    def __init__(self, key):
        self.key = key
        self.build = googleapiclient.discovery.build(
            'youtube',
            'v3',
            developerKey=self.key,
            cache_discovery=False,
        )

    def __repr__(self):
        return "<YouTube object>"

    def search(self, search_string=None, type_=None, per_page=None, after=None, extra_kwargs=None):
        kwargs = {
            'part': 'id,snippet',
        }
        if search_string:
            kwargs['q'] = search_string
        if type_:
            kwargs['type'] = type_
        if per_page:
            if per_page > 50:
                logger.warning("cannot fetch more than 50 results per page.")
                per_page = 50
            kwargs['maxResults'] = per_page
        if after:
            kwargs['publishedAfter'] = datetime_to_string(after)
        if extra_kwargs:
            kwargs.update(extra_kwargs)

        query = Query(self, 'search', kwargs)
        return ListResponse(query)

    def video(self, id_, extra_kwargs=None):
        kwargs = {
            'part': 'id,snippet',
            'id': id_,
        }
        if extra_kwargs:
            kwargs.update(extra_kwargs)

        query = Query(self, 'videos', kwargs)
        return ListResponse(query).first()

    def channel(self, id_, extra_kwargs=None):
        kwargs = {
            'part': 'id,snippet',
            'id': id_,
        }
        if extra_kwargs:
            kwargs.update(extra_kwargs)

        query = Query(self, 'channels', kwargs)
        return ListResponse(query).first()


class Query(object):

    def __init__(self, youtube, endpoint, kwargs=None):
        self.youtube = youtube
        self.endpoint = endpoint
        self.kwargs = kwargs or dict()

        if not 'part' in kwargs:
            kwargs['part'] = 'id'

        endpoint_func_mapping = {
            'search': self.youtube.build.search().list,
            'videos': self.youtube.build.videos().list,
            'channels': self.youtube.build.channels().list,
        }

        try:
            self.query_func = endpoint_func_mapping[self.endpoint]
        except KeyError:
            raise ValueError(f"youtube api endpoint '{self.endpoint}' not recognised.")

    def __repr__(self):
        return "<Query '{}' kwargs={}>".format(self.endpoint, self.kwargs)

    def execute(self, kwargs=None):
        if kwargs is not None:
            query_kwargs = self.kwargs.copy()
            query_kwargs.update(kwargs)
        else:
            query_kwargs = self.kwargs

        return self.query_func(**query_kwargs).execute()


class ListResponse(object):

    def __init__(self, query):
        # execute query to get raw api response dictionary
        self.query = query
        raw = self.query.execute()

        # store basic response info
        self.kind = raw.get('kind').replace("youtube#", "")
        self.next_page_token = raw.get('nextPageToken')
        page_info = raw.get('pageInfo', {})
        self.total_results = page_info.get('totalResults')
        self.results_per_page = page_info.get('resultsPerPage')

        # keep items on the first page in raw format
        self._first_page = raw.get('items')

    def __repr__(self):
        return "<ListResponse '{}': n={}, per_page={}>".format(
            self.query.endpoint, self.total_results, self.results_per_page
        )

    def first(self):
        if self._first_page:
            return convert_item(self._first_page[0])

    def first_page(self):
        if self._first_page:
            return [convert_item(item) for item in self._first_page]

    def all(self, limit=MAX_SEARCH_RESULTS):
        page_items = self._first_page
        next_page_token = self.next_page_token
        items_yielded = 0
        page_no = 1
        while True:
            if page_no > 1:
                raw = self.query.execute({'pageToken': next_page_token})
                page_items = raw.get('items')
                next_page_token = raw.get('nextPageToken')

            for item in page_items:
                yield convert_item(item)
                items_yielded += 1
                if items_yielded >= limit:
                    return

            if next_page_token is None:
                break

            page_no += 1


class Video(object):

    def __init__(self, id_, snippet=None):
        self.id_ = id_
        self.snippet = snippet or dict()

        # store data (or None if no snippet is given)
        self.title = self.snippet.get('title')
        self.description = self.snippet.get('description')
        self.published_at = string_to_datetime(self.snippet.get('publishedAt'))
        self.tags = self.snippet.get('tags')
        self.channel_id = self.snippet.get('channelId')
        self.channel_title = self.snippet.get('channelTitle')

    def __repr__(self):
        if self.snippet:
            return "<Video {}: \"{:.32}\" by {}>".format(self.id_, self.title, self.channel_title)
        else:
            return "<Video {}>".format(self.id_)


class Channel(object):

    def __init__(self, id_, snippet=None):
        self.id_ = id_
        self.snippet = snippet or dict()

        # store data (or None if no snippet is given)
        self.title = self.snippet.get('title')
        self.description = self.snippet.get('description')
        self.published_at = string_to_datetime(self.snippet.get('publishedAt'))
        self.country = self.snippet.get('channelId')

    def __repr__(self):
        if self.snippet:
            return "<Channel {}: {}>".format(self.id_, self.title)
        else:
            return "<Channel {}>".format(self.id_)


def convert_item(item):
    kind = item['kind'].replace('youtube#', '')

    if kind == 'searchResult':
        kind = item['id']['kind'].replace('youtube#', '')
        id_label = kind + 'Id'
        id_ = item['id'][id_label]
    else:
        id_ = item['id']

    if kind == 'video':
        return Video(id_, item.get('snippet'))
    elif kind == 'channel':
        return Channel(id_, item.get('snippet'))
    else:
        NotImplementedError(f"can't deal with resource kind {kind} yet.")
