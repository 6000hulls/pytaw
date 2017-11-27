import math
import os
import logging
import configparser
import googleapiclient.discovery

from .utils import datetime_to_string, string_to_datetime
from .resources import Video, Channel


logger = logging.getLogger(__name__)

MAX_SEARCH_RESULTS = 1000
CONFIG_FILE_PATH = "config.ini"


class YouTube(object):

    def __init__(self, key=None, part=None):
        if key is None:
            if os.path.exists(CONFIG_FILE_PATH):
                config = configparser.ConfigParser()
                config.read(CONFIG_FILE_PATH)
                self.key = config['youtube']['developer_key']
            else:
                raise ValueError("api key not provided.")
        else:
            self.key = key

        if part is None:
            self.part = "id,snippet,contentDetails"
        else:
            self.part = part

        self.build = googleapiclient.discovery.build(
            'youtube',
            'v3',
            developerKey=self.key,
            cache_discovery=False,      # supress a warning
        )

    def __repr__(self):
        return "<YouTube object>"

    def search(self, search_string=None, type_=None, per_page=None, after=None, extra_kwargs=None):
        kwargs = {
            'part': self.part,
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

    def video(self, id, extra_kwargs=None):
        kwargs = {
            'part': self.part,
            'id': id,
        }
        if extra_kwargs:
            kwargs.update(extra_kwargs)

        query = Query(self, 'videos', kwargs)
        return ListResponse(query).first()

    def channel(self, id, extra_kwargs=None):
        kwargs = {
            'part': self.part,
            'id': id,
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
            kwargs['part'] = self.youtube.part

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
            return create_resource_from_api_response(self._first_page[0])

    def first_page(self):
        if self._first_page:
            return [create_resource_from_api_response(item) for item in self._first_page]

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
                yield create_resource_from_api_response(item)
                items_yielded += 1
                if items_yielded >= limit:
                    return

            if next_page_token is None:
                break

            page_no += 1


def create_resource_from_api_response(item):
        kind = item['kind'].replace('youtube#', '')

        if kind == 'searchResult':
            kind = item['id']['kind'].replace('youtube#', '')
            id_label = kind + 'Id'
            id = item['id'][id_label]
        else:
            id = item['id']

        if kind == 'video':
            return Video(id, item)
        elif kind == 'channel':
            return Channel(id, item)
        else:
            NotImplementedError(f"can't deal with resource kind {kind} yet.")
