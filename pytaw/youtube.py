import math
import logging

import googleapiclient.discovery

from .models import Video, Channel
from .utils import datetime_to_string


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

    def search(self, query=None, type_=None, after=None):
        kwargs = {
            'part': 'id,snippet',
        }
        if query:
            kwargs['q'] = query
        if type_:
            kwargs['type'] = type_
        if after:
            kwargs['publishedAfter'] = datetime_to_string(after)

        query = Query(self.build, 'search', kwargs)
        return SearchListResponse(query)


class Query(object):

    def __init__(self, build, endpoint, kwargs=None):
        self.build = build
        self.endpoint = endpoint
        self.kwargs = kwargs or dict()

        if not 'part' in kwargs:
            kwargs['part'] = 'id'

        endpoint_func_mapping = {
            'search': self.build.search().list
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


class Response(object):

    def __init__(self, query):
        # execute a minimal query to check it works and get no. of results etc.
        self.query = query
        raw = self.query.execute(kwargs={
            'part': 'id',
            'maxResults': 0,
        })

        self.kind = raw.get('kind')
        self.etag = raw.get('etag')
        self.total_results = raw.get('pageInfo', {}).get('totalResults')
        self.current_page = 1
        self.next_page_token = ''

    def process_item(self, item):
        raise NotImplementedError("you must implement process_item() for this class.")

    def page(self):
        if self.next_page_token is None:
            return []

        kwargs = {'pageToken': self.next_page_token} if self.next_page_token else None
        raw = self.query.execute(kwargs)

        self.next_page_token = raw.get('nextPageToken')
        page_items = raw.get('items')

        for item in page_items:
            yield item

        if self.next_page_token is not None:
            self.current_page += 1
        else:
            self.current_page = None


    def all(self):
        self.current_page = 1
        self.next_page_token = ''
        while self.next_page_token is not None:
            yield from self.page()


class SearchListResponse(Response):

    resource_type = 'search'

    def process_item(self, item):
        kind = item.get('kind')
        if kind != "youtube#searchResult":
            logger.info("search result kind is '{kind}', expected 'youtube#searchResult'.")

        resource_kind = item.get('id', {}).get('kind')
        if resource_kind == "youtube#video":
            kwargs = {
                'id_': item.get('id', {}).get('videoId'),
                'title': item.get('snippet', {}).get('title'),
                'published_at': item.get('snippet', {}).get('publishedAt'),
            }
            return Video(**kwargs)
        elif resource_kind == "youtube#channel":
            kwargs = {
                'id_': item.get('id', {}).get('channelId'),
                'title': item.get('snippet', {}).get('title'),
                'published_at': item.get('snippet', {}).get('publishedAt'),
            }
            return Channel(**kwargs)
        else:
            logger.warning(f"unrecognised resource kind '{resource_kind}'.")
            return None