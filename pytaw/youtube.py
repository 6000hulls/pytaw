import math
import logging
import googleapiclient.discovery

from .utils import datetime_to_string, string_to_datetime


logger = logging.getLogger(__name__)


class YouTube(object):

    def __init__(self, key):
        self.key = key
        self.build = googleapiclient.discovery.build(
            'youtube',
            'v3',
            developerKey=self.key,
            cache_discovery=False,
        )

    def search(self, search_string=None, type_=None, per_page=None, after=None):
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

        query = Query(self.build, 'search', kwargs)
        return ListResponse(query)

    def video(self, id_):
        kwargs = {
            'part': 'id,snippet',
            'id': id_,
        }
        query = Query(self.build, 'videos', kwargs)
        return ListResponse(query).first()


class Query(object):

    def __init__(self, build, endpoint, kwargs=None):
        self.build = build
        self.endpoint = endpoint
        self.kwargs = kwargs or dict()

        if not 'part' in kwargs:
            kwargs['part'] = 'id'

        endpoint_func_mapping = {
            'search': self.build.search().list,
            'videos': self.build.videos().list,
        }

        try:
            self.query_func = endpoint_func_mapping[self.endpoint]
        except KeyError:
            raise ValueError(f"youtube api endpoint '{self.endpoint}' not recognised.")

    def execute(self, kwargs=None):
        if kwargs is not None:
            query_kwargs = self.kwargs.copy()
            query_kwargs.update(kwargs)
        else:
            query_kwargs = self.kwargs

        return self.query_func(**query_kwargs).execute()


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


class ListResponse(object):

    def __init__(self, query):
        self.query = query
        raw = self.query.execute()

        self.kind = raw.get('kind')
        self.next_page_token = raw.get('nextPageToken')

        page_info = raw.get('pageInfo', {})
        self.total_results = page_info.get('totalResults')
        self.results_per_page = page_info.get('resultsPerPage')

        self._first_page = raw.get('items')

    def first(self):
        if self._first_page:
            return convert_item(self._first_page[0])
        else:
            return None

    def all(self, limit=None):
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
        snippet = snippet or dict()

        # convert date to datetime object
        published_at = snippet.get('publishedAt')
        if published_at:
            published_at = string_to_datetime(published_at)

        # store data (or None if no snippet is given)
        self.published_at = published_at
        self.channel_id = snippet.get('channelId')
        self.title = snippet.get('title')
        self.description = snippet.get('description')
        self.channel_title = snippet.get('channelTitle')
        self.tags = snippet.get('tags')
