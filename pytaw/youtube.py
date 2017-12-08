from abc import ABC, abstractmethod
from enum import Enum
from datetime import timedelta
import os
import logging
import configparser
import googleapiclient.discovery

from .utils import datetime_to_string, string_to_datetime, youtube_duration_to_seconds


logger = logging.getLogger(__name__)

CONFIG_FILE_PATH = "config.ini"


class YouTube(object):
    """The interface to the YouTube API.

    Connects to the API by passing a developer api key, and provides some high-level methods for
    querying it.

    """

    def __init__(self, key=None):
        """Initialise the YouTube class.

        :param key: developer api key (you need to get this from google)

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

        self.build = googleapiclient.discovery.build(
            'youtube',
            'v3',
            developerKey=self.key,
            cache_discovery=False,      # suppress a warning
        )

    def __repr__(self):
        return "<YouTube object>"

    def search(self, q=None, type_=None, before=None, after=None, api_params=None):
        """Search YouTube, returning an instance of `ListResponse`.

        :param q: search query
        :param type_: type of resource to search (by default a search will contain many
            different resource types, including videos, channels, playlists etc.)
        :param before: limit results to resources published before this date by providing a
            datetime instance
        :param after: limit results to resources published after this date by providing a
            datetime instance
        :param api_params: dict of additional api parameters to pass to the search function
        :return: ListResponse object containing the requested resource instances

        """
        params = {
            'part': 'id',
            'maxResults': 50,
        }
        if q:
            params['q'] = q
        if type_:
            params['type'] = type_
        if before:
            params['publishedBefore'] = datetime_to_string(before)
        if after:
            params['publishedAfter'] = datetime_to_string(after)
        if api_params:
            params.update(api_params)

        query = Query(self, 'search', params)
        return ListResponse(query)

    def video(self, id, api_params=None):
        """Fetch a Video instance.

        :param id: youtube video id e.g. 'jNQXAC9IVRw'
        :param api_params: additional api parameters to send with the query
        :return: Video instance if video is found, else None

        """
        params = {
            'part': 'id',
            'id': id,
        }
        if api_params:
            params.update(api_params)

        query = Query(self, 'videos', params)
        return ListResponse(query).first()

    def channel(self, id, api_params=None):
        """Fetch a Channel instance.

        :param id: youtube channel id e.g. 'UCMDQxm7cUx3yXkfeHa5zJIQ'
        :param api_params: extra keyword arguments to send with the query
        :return: Channel instance if channel is found, else None

        """
        params = {
            'part': 'id',
            'id': id,
        }
        if api_params:
            params.update(api_params)

        query = Query(self, 'channels', params)
        return ListResponse(query).first()


class Query(object):
    """Everything we need to execute a query and retrieve the raw response dictionary."""

    def __init__(self, youtube, endpoint, api_params=None):
        """Initialise the query.

        :param youtube: YouTube instance
        :param endpoint: string giving the api endpoint to query, e.g. 'videos', 'search'...
        :param api_params: dict of keyword parameters to send (directly) to the api

        """
        self.youtube = youtube
        self.endpoint = endpoint
        self.api_params = api_params or dict()

        if not 'part' in api_params:
            api_params['part'] = 'id'

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
        return "<Query '{}' api_params={}>".format(self.endpoint, self.api_params)

    def execute(self, api_params=None):
        """Execute the query.

        :param api_params: extra api parameters to send with the query.
        :return: api response dictionary

        """
        if api_params is not None:
            # update only for this query execution
            query_params = self.api_params.copy()
            query_params.update(api_params)
        else:
            query_params = self.api_params

        return self.query_func(**query_params).execute()


class ListResponse(object):
    """Executes a query and turns the response into a list of Resource instances."""

    MAX_RESULTS = 100000

    def __init__(self, query):
        self.youtube = query.youtube
        self.query = query

        self.kind = None
        self.total_results = None
        self.results_per_page = None

        self._reset()

    def _reset(self):
        self._listing = None            # internal storage for current page listing
        self._list_index = None         # index of item within current listing
        self._exhausted = False         # flagged when we reach the end of the available results
        self._page_counter = 0          # no. of pages processed
        self._item_counter = 0          # total no. of items yielded
        self._next_page_token = None    # api page token required for the next page of results

    def __repr__(self):
        return "<ListResponse endpoint='{}', n={}, per_page={}>".format(
            self.query.endpoint, self.total_results, self.results_per_page
        )
        
    def __iter__(self):
        """Allow this object to act as an iterator."""
        return self

    def __next__(self):
        if self._item_counter > self.MAX_RESULTS:
            logger.warning(f"ListResponse results limit reached.  if you really need more than "
                           f"{self.MAX_RESULTS} then increase self.MAX_RESULTS.")
            raise StopIteration()

        if self._listing is None or self._list_index >= len(self._listing):
            self._fetch_next()

        try:
            item = self._listing[self._list_index]
        except IndexError:
            raise StopIteration()

        self._list_index += 1
        self._item_counter += 1
        return create_resource_from_api_response(self.youtube, item)

    def __getitem__(self, index):
        self._reset()
        if isinstance(index, slice):
            start = index.start or 0
            stop = index.stop or self.MAX_RESULTS
            step = index.step or None

            if step not in (1, None):
                raise ValueError("don't use the slice step!")

            if start is not None:
                for _ in range(start):
                    self.__next__()

            items = []
            for _ in range(start, stop):
                items.append(self.__next__())

            return items
        else:
            for _ in range(index.start):
                self.__next__()
            return self.__next__()

    def _fetch_next(self):
        if self._exhausted:
            raise StopIteration()

        # execute query to get raw api response dictionary
        params = dict()
        if self._next_page_token:
            params['pageToken'] = self._next_page_token
        raw = self.query.execute(api_params=params)

        # store basic response info
        self.kind = raw.get('kind').replace("youtube#", "")
        self._next_page_token = raw.get('nextPageToken', None)
        if self._next_page_token is None:
            self._exhausted = True

        page_info = raw.get('pageInfo', {})
        self.total_results = int(page_info.get('totalResults'))
        self.results_per_page = int(page_info.get('resultsPerPage'))

        # add items on this page in raw format
        self._listing = raw.get('items')
        self._list_index = 0

    def first(self):
        self._reset()
        return self.__next__()

    def first_page(self):
        self._reset()
        return [self.__next__() for _ in range(self.results_per_page)]


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
        elif kind == 'playlist':
            return Playlist(youtube, id, item)
        else:
            raise NotImplementedError(f"can't deal with resource kind '{kind}'")


class Resource(object):
    """Base class for YouTube resource classes, e.g. Video, Channel etc."""

    ENDPOINT = ''
    ATTRIBUTE_DEFS = {}

    def __init__(self, youtube, id, data=None):
        """Initialise a Resource object.

        Need the YouTube instance, in case further queries are required, the resource id,
        and (optionally) some data in the form of an API response.

        """
        # if we need to query again for more data we'll need access to the youtube instance
        self.youtube = youtube

        # every resource has a unique id, it may be a different format for each resource type though
        self.id = id

        # this is the api response item for the resource.  it's a dictionary with 'kind',
        # 'etag' and 'id' keys, at least.  it may also have a 'snippet', 'contentDetails' etc.
        # containing more detailed info.  in theory, this dictionary could be accessed directly,
        # but we'll make the data accessible via class attributes where possible so that we can
        # do type conversion etc.
        self._data = data or {}

        # update attributes with whatever we've been given as data
        self._update_attributes()

    def _get(self, *keys):
        """Get a data attribute from the stored item response, if it exists.

        If it doesn't, return None.  This could be because the necessary information was not
        included in the 'part' argument in the original query, or simply because the data
        attribute doesn't exist in the response.

        :param *keys: one or more dictionary keys.  if there's more than one, we'll query
            them recursively, so _get('a', 'b', 'c') will return
            self._items['a']['b']['c']
        :return: the data attribute

        """
        param = self._data
        for key in keys:
            param = param.get(key, None)
            if param is None:
                return None

        return param

    def _fetch(self, part):
        """Query the API for a specific data part.

        Build a query and execute it.  Update internal storage to reflect the new data.  Note:
        access to the data via attributes will not update until _update_attributes() is called.

        :param part: part string for the API query.

        """
        part_string = f"id,{part}"

        # get a raw listResponse from youtube
        response = Query(
            youtube=self.youtube,
            endpoint=self.ENDPOINT,
            api_params={'part': part_string, 'id': self.id}
        ).execute()

        # get the first resource item and update the internal data storage
        item = response['items'][0]
        self._data.update(item)


    def _update_attributes(self):
        """Take internally stored raw data and creates attributes with right types etc.

        Attributes defined in ATTRIBUTE_DEFS will be added as attributes, if they exist in
        internal data storage.

        """
        for attr_name, attr_def in self.ATTRIBUTE_DEFS.items():
            # get the value, if it exists in the data store
            if isinstance(attr_def.name, str):
                raw_value = self._get(attr_def.part, attr_def.name)
            else:
                raw_value = self._get(attr_def.part, *attr_def.name)

            if raw_value is None:
                if self._data.get(attr_def.part) is not None:
                    raw_value = ''
                else:
                    continue

            if attr_def.type_ is None:
                value = raw_value
            elif attr_def.type_ in ('str', 'string'):
                value = str(raw_value)
            elif attr_def.type_ in ('int', 'integer'):
                value = int(raw_value)
            elif attr_def.type_ == 'float':
                value = float(raw_value)
            elif attr_def.type_ == 'list':
                value = list(raw_value)
            elif attr_def.type_ == 'datetime':
                value = string_to_datetime(raw_value)
            elif attr_def.type_ == 'duration':
                value = timedelta(seconds=youtube_duration_to_seconds(raw_value))
            else:
                raise TypeError(f"type '{attr_def.type_}' not recognised.")

            setattr(self, attr_name, value)

    def __getattr__(self, item):
        """If an attribute hasn't been set, this function tries to fetch and add it.

        Note: the __getattr__ method is only ever called when an attribute can't be found,
        therefore there is no need to check if the attribute already exists within this function.

        If the attribute isn't present in ATTRIBUTE_DEFS, raise AttributeError.

        :param item: attribute name
        :return: attribute value

        """
        if item in self.ATTRIBUTE_DEFS:
            self._fetch(part=self.ATTRIBUTE_DEFS[item].part)
            self._update_attributes()
            return getattr(self, item)

        raise AttributeError(f"attribute '{item}' not recognised for resource type "
                             f"'{type(self).__name__}'")


class AttributeDef(object):
    """Defines a Resource attribute.

    To make the API data available as attributes on Resource objects we need to know
        1. where to find the data in the API response, and
        2. what data type the attribute should have.

    This class defines the 'part' (in API terminology) that the attribute can be found in,
    and it's name (the dictionary key within the 'part'), so that it can be found in the API
    response.

    The data type should also be given as a string ('str', 'int', 'datetime' etc), so that we can
    convert it when we add the data as an attribute to the Resource instance.  If not given or
    None, no type conversion is performed.

    """
    def __init__(self, part, name, type_=None):
        self.part = part
        self.name = name
        self.type_ = type_


class Video(Resource):
    """A single YouTube video."""

    ENDPOINT = 'videos'
    ATTRIBUTE_DEFS = {
        #
        # snippet
        'title': AttributeDef('snippet', 'title', type_='str'),
        'description': AttributeDef('snippet', 'description', type_='str'),
        'published_at': AttributeDef('snippet', 'publishedAt', type_='datetime'),
        'tags': AttributeDef('snippet', 'tags', type_='list'),
        'channel_id': AttributeDef('snippet', 'channelId', type_='str'),
        'channel_title': AttributeDef('snippet', 'channelTitle', type_='str'),
        #
        # contentDetails
        'duration': AttributeDef('contentDetails', 'duration', type_='duration'),
        #
        # status
        'license': AttributeDef('status', 'license', type_='str'),
        #
        # statistics
        'n_views': AttributeDef('statistics', 'viewCount', type_='int'),
        'n_likes': AttributeDef('statistics', 'likeCount', type_='int'),
        'n_dislikes': AttributeDef('statistics', 'dislikeCount', type_='int'),
        'n_favorites': AttributeDef('statistics', 'favoriteCount', type_='int'),
        'n_comments': AttributeDef('statistics', 'commentCount', type_='int'),
    }

    @property
    def is_cc(self):
        return self.license == 'creativeCommon'


class Channel(Resource):
    """A single YouTube channel."""

    ENDPOINT = 'channels'
    ATTRIBUTE_DEFS = {
        #
        # snippet
        'title': AttributeDef('snippet', 'title'),
        'description': AttributeDef('snippet', 'description'),
        'published_at': AttributeDef('snippet', 'publishedAt', type_='datetime'),
        'thumbnail_url': AttributeDef('snippet', ['thumbnails', 'default', 'url'], type_='str'),
        'country': AttributeDef('snippet', 'country', type_='str'),
        #
        # statistics
        'n_videos': AttributeDef('statistics', 'videoCount', type_='int'),
        'n_subscribers': AttributeDef('statistics', 'subscriberCount', type_='int'),
        'n_views': AttributeDef('statistics', 'viewCount', type_='int'),
        'n_comments': AttributeDef('statistics', 'commentCount', type_='int'),
    }

    def most_recent_upload(self):
        response = self.most_recent_uploads(n=1)
        return response[0]

    def most_recent_uploads(self, n=50):
        if n > 50:
            raise ValueError(f"n must be less than 50, not {n}")

        kwargs = {
            'part': 'id',
            'channelId': self.id,
            'maxResults': n,
            'order': 'date',
            'type': 'video',
        }
        response = self.youtube.search(api_params=kwargs)
        return response[:n]


class Playlist(Resource):
    """A single YouTube playlist."""

    ENDPOINT = 'playlists'
    ATTRIBUTE_DEFS = {
        #
        # snippet
        'title': AttributeDef('snippet', 'title'),
        'description': AttributeDef('snippet', 'description'),
        'published_at': AttributeDef('snippet', 'publishedAt', type_='datetime'),
    }