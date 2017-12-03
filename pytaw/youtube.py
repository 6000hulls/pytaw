from abc import ABC, abstractmethod
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

    def search(self, q=None, type_=None, per_page=None, after=None, extra_kwargs=None):
        """Search YouTube, returning an instance of `ListResponse`.

        :param q: search query
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
        if q:
            kwargs['q'] = q
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
    """Executes a query and turns the response into a list of Resource instances."""

    MAX_RESULTS = 500

    def __init__(self, query):
        self.query = query
        self._reset()

    def __repr__(self):
        return "<ListResponse endpoint='{}', n={}, per_page={}>".format(
            self.query.endpoint, self.total_results, self.results_per_page
        )

    def _reset(self):
        self.kind = None
        self.next_page_token = None
        self.total_results = None
        self.results_per_page = None
        self._listing = None
        self._listing_index = None      # index of item within current listing
        self._n_yielded = 0             # total no. of items yielded so far
        self._exhausted = False
        
    def __iter__(self):
        """Allow this object to act as an iterator."""
        return self

    def __next__(self):
        if self._n_yielded > self.MAX_RESULTS:
            raise StopIteration()

        if self._listing is None or self._listing_index >= len(self._listing):
            self._fetch_next()

        item = self._listing[self._listing_index]
        self._listing_index += 1
        self._n_yielded += 1
        return create_resource_from_api_response(self.query.youtube, item)

    def _fetch_next(self):
        if self._exhausted:
            raise StopIteration()

        # execute query to get raw api response dictionary
        params = dict()
        if self.next_page_token:
            params['pageToken'] = self.next_page_token
        raw = self.query.execute(kwargs=params)

        # store basic response info
        self.kind = raw.get('kind').replace("youtube#", "")
        self.next_page_token = raw.get('nextPageToken', None)
        if self.next_page_token is None:
            self._exhausted = True

        page_info = raw.get('pageInfo', {})
        self.total_results = int(page_info.get('totalResults'))
        self.results_per_page = int(page_info.get('resultsPerPage'))

        # keep items on the first page in raw format
        self._listing = raw.get('items')
        self._listing_index = 0

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
            kwargs={'part': part_string, 'id': self.id}
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
            raw_value = self._get(attr_def.part, attr_def.name)
            if raw_value is None:
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
        'published_at': AttributeDef('snippet', 'publishedAt', type_='datetime'),
    }