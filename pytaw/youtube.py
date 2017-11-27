from abc import ABC, abstractmethod
from datetime import timedelta
import os
import logging
import configparser
import googleapiclient.discovery

from .utils import datetime_to_string, string_to_datetime, youtube_duration_to_seconds


logger = logging.getLogger(__name__)

MAX_SEARCH_RESULTS = 1000
CONFIG_FILE_PATH = "config.ini"


class YouTube(object):
    """The interface to the YouTube API.

    Connects to the API by passing a developer api key, and provides some high-level methods for
    querying it.

    """

    def __init__(self, key=None, part=None):
        """Initialise the YouTube class.

        :param key: developer api key (you need to get this from google)
        :param part: default part string for api queries.  default: 'id,snippet,contentDetails'.

        """
        # developer api key may be specified at initialisation, or in a config file
        if key is None:
            if os.path.exists(CONFIG_FILE_PATH):
                config = configparser.ConfigParser()
                config.read(CONFIG_FILE_PATH)
                self.key = config['youtube']['developer_key']
            else:
                raise ValueError("api key not provided.")
        else:
            self.key = key

        # the default 'part' string to use for requests to the api.
        # this effects how much data a query returns, as well as the quota cost of that query.
        if part is None:
            self.part = "id"
        else:
            self.part = part

        self.build = googleapiclient.discovery.build(
            'youtube',
            'v3',
            developerKey=self.key,
            cache_discovery=False,      # suppress a warning
        )

    def __repr__(self):
        return "<YouTube object>"

    def search(self, search_string=None, type_=None, per_page=None, after=None, extra_kwargs=None):
        """Search YouTube, returning an instance of `ListResponse`.

        :param search_string: search query
        :param type_: type of resource to search (by default a search will contain many
            different resource types, including videos, channels, playlists etc.)
        :param per_page: how many results to return per request (max. 50)
        :param after: limit results to resources published after this date by providing a
            datetime instance
        :param extra_kwargs: extra keyword arguments to pass to the search function
        :return: ListResponse object containing the requested resource instances

        """
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
        """Fetch a Video instance.

        :param id: youtube video id e.g. 'jNQXAC9IVRw'
        :param extra_kwargs: extra keyword arguments to send with the query
        :return: Video instance if video is found, else None

        """
        kwargs = {
            'part': self.part,
            'id': id,
        }
        if extra_kwargs:
            kwargs.update(extra_kwargs)

        query = Query(self, 'videos', kwargs)
        return ListResponse(query).first()

    def channel(self, id, extra_kwargs=None):
        """Fetch a Channel instance.

        :param id: youtube channel id e.g. 'UCMDQxm7cUx3yXkfeHa5zJIQ'
        :param extra_kwargs: extra keyword arguments to send with the query
        :return: Channel instance if channel is found, else None

        """
        kwargs = {
            'part': self.part,
            'id': id,
        }
        if extra_kwargs:
            kwargs.update(extra_kwargs)

        query = Query(self, 'channels', kwargs)
        return ListResponse(query).first()


class Query(object):
    """Everything we need to execute a query and retrieve the raw response dictionary."""

    def __init__(self, youtube, endpoint, kwargs=None):
        """Initialise the query.

        :param youtube: YouTube instance
        :param endpoint: string giving the endpoing to query, e.g. 'videos', 'search'...
        :param kwargs: keyword arguments to send with the query

        """
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
        """Execute the query.

        :param kwargs: extra keyword arguments to send with the query.
        :return: api response dictionary

        """
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
            return create_resource_from_api_response(self.query.youtube, self._first_page[0])

    def first_page(self):
        if self._first_page:
            return [create_resource_from_api_response(self.query.youtube, item)
                    for item in self._first_page]

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


def create_resource_from_api_response(youtube, item):
        kind = item['kind'].replace('youtube#', '')

        if kind == 'searchResult':
            kind = item['id']['kind'].replace('youtube#', '')
            id_label = kind + 'Id'
            id = item['id'][id_label]
        else:
            id = item['id']

        if kind == 'video':
            return Video(youtube, id, item)
        elif kind == 'channel':
            return Channel(youtube, id, item)
        else:
            NotImplementedError(f"can't deal with resource kind {kind} yet.")


class Resource(ABC):
    """Abtract base class for YouTube resource classes, e.g. Video, Channel etc."""

    resource_type = None
    resource_endpoint = None
    attribute_lookup = None

    def __init__(self, youtube, id, item):
        # if we need to query again for more data we'll need access to the youtube instance
        self.youtube = youtube

        # every resource has a unique id, it may be a different format for each resource type though
        self.id = id

        # this is the api response item for the resource.  it's a dictionary with 'kind',
        # 'etag' and 'id' keys, at least.  it may also have a 'snippet', 'contentDetails' etc.
        # containing more detailed info.  in theroy, this dictionary could be access directly,
        # but we'll make the data accessible via class attributes where possible.
        self._item = item

    def _get(self, *keys):
        """Get a data attribute from the stored item response, if it exists.

        If it doesn't, return None.  This could be because the necessary information was not
        included in the 'part' argument in the original query, or simply because the data
        attribute doesn't exist in the response.

        :param *keys: one or more dictionary keys.  if there's more than one, we'll query
            them recursively, so _get('snippet', 'title') will return
            self._items['snippet']['title']
        :return: the data attribute

        """
        param = self._item
        for key in keys:
            param = param.get(key, None)
            if param is None:
                return None

        return param

    def _get_or_query(self, part, attribute):
        value = self._get(part, attribute)
        if value is not None:
            return value

        self._update_item(part)
        value = self._get(part, attribute)
        if value is None:
            raise ValueError(f"can't find attribute {attribute} in part {part}")

        return value

    def _update_item(self, part):
        part_string = f"id,{part}"
        response = Query(
            youtube=self.youtube,
            endpoint=self.resource_endpoint,
            kwargs={'part': part_string, 'id': self.id}
        ).execute()

        item = response['items'][0]
        self._item.update(item)

    def __getattr__(self, item):
        return self._get_or_query(*self.attribute_lookup[item])


class Video(Resource):
    resource_type = 'video'
    resource_endpoint = 'videos'
    attribute_lookup = {
        'title':            ('snippet', 'title'),
        'description':      ('snippet', 'description'),
        'tags':             ('snippet', 'tags'),
        'channel_id':       ('snippet', 'channelId'),
        'channel_title':    ('snippet', 'channelTitle'),
        'status':           ('status', 'license'),
        'n_views':          ('statistics', 'viewCount'),
        'n_likes':          ('statistics', 'likeCount'),
        'n_dislikes':       ('statistics', 'dislikeCount'),
        'n_favorites':      ('statistics', 'favoriteCount'),
        'n_comments':       ('statistics', 'commentCount'),
    }

    def __repr__(self):
        if self.title:
            return "<Video {}: {}>".format(self.id, self.title)
        else:
            return "<Video {}>".format(self.id)

    @property
    def published_at(self):
        published_at = self._get_or_query('snippet', 'publishedAt')
        return string_to_datetime(published_at)

    @property
    def duration(self):
        duration_iso8601 = self._get_or_query('contentDetails', 'duration')
        return timedelta(seconds=youtube_duration_to_seconds(duration_iso8601))

    def __getattr__(self, item):
        attribute = super().__getattr__(item)
        if len(item) > 2 and item[:2] == 'n_':
            attribute = int(attribute)
        return attribute



class Channel(Resource):
    resource_type = 'channel'
    resource_endpoint = 'channels'
    attribute_lookup = {
        'title': ('snippet', 'title'),
    }

    @property
    def published_at(self):
        published_at = self._get_or_query('snippet', 'publishedAt')
        return string_to_datetime(published_at)

    def __repr__(self):
        if self.title:
            return "<Channel {}: {}>".format(self.id, self.title)
        else:
            return "<Channel {}>".format(self.id)