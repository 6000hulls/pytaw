import math
import logging

from googleapiclient.discovery import build

from .resources import Video, Channel
from .utils import datetime_to_string


logger = logging.getLogger(__name__)


class Response(object):

    def __init__(self, raw, kwargs):
        self.kwargs = kwargs

        self.kind = raw.get('kind')
        self.etag = raw.get('etag')
        self.next_page_token = raw.get('nextPageToken')
        self.total_results = raw.get('pageInfo', {}).get('totalResults')
        self.results_per_page = raw.get('pageInfo', {}).get('resultsPerPage')

        self.page_items = raw.get('items')
        self.n_pages = math.ceil(self.total_results/self.results_per_page)
        self.current_page = 1

    def process_item(self, item):
        raise NotImplementedError("you must implement process_item() for this class.")

    def page(self):
        for item in self.page_items:
            yield self.process_item(item)
        self.next_page()

    def next_page(self):
        # self.page_items =
        if self.current_page < self.n_pages:
            self.current_page += 1
        else:
            self.current_page = None

    def all(self):
        pass


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


class YouTube(object):

    def __init__(self, key):
        self.key = key
        self.youtube = build('youtube', 'v3', developerKey=self.key, cache_discovery=False)


    def query_(self, resource='search', items_limit=50, kwargs=None, return_total=False):
        # get the function we need to call for this resource type
        resource_functions = {
            'search': self.youtube.search().list,
        }
        try:
            resource_func = resource_functions[resource]
        except KeyError:
            raise ValueError(f"youtube api resource '{resource}' not recognised.")

        # work out how many pages to get, and how many results on each page
        youtube_max_per_page = 50
        if items_limit < 1:
            return []
        else:
            kwargs['maxResults'] = youtube_max_per_page
            page_limit = math.ceil(items_limit/youtube_max_per_page)
            max_results_last_page = (items_limit % youtube_max_per_page) or youtube_max_per_page

        # make sure kwargs is defined and missing values are filled in
        if kwargs is None:
            kwargs = {}
        if not 'part' in kwargs:
            kwargs['part'] = 'id'

        # fetch the results
        results = []
        n_total_results = None
        next_page = ''
        pc = 0
        while next_page is not None and pc < page_limit:
            if pc == page_limit - 1:
                kwargs['maxResults'] = max_results_last_page

            kwargs['pageToken'] = next_page
            response = resource_func(**kwargs).execute()
            items = response.get('items', [])
            next_page = response.get('nextPageToken', None)
            n_total_results = response.get('pageInfo', {}).get('totalResults')

            results += items
            pc += 1

        if return_total:
            return results, n_total_results
        else:
            return results


    def raw_query(self, resource='search', kwargs=None):
        # get the function we need to call for this resource type
        resource_functions = {
            'search': self.youtube.search().list,
        }
        try:
            resource_func = resource_functions[resource.lower()]
        except KeyError:
            raise ValueError(f"youtube api resource '{resource}' not recognised.")

        # make sure kwargs is defined and missing values are filled in
        if kwargs is None:
            kwargs = {}
        if not 'part' in kwargs:
            kwargs['part'] = 'id'

        # fetch the raw response from youtube
        return resource_func(**kwargs).execute()


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

        raw = self.raw_query('search', kwargs)
        return SearchListResponse(raw, kwargs)