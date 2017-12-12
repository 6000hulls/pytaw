from datetime import timedelta
import os
import logging
import configparser
import collections
import itertools
from pprint import pprint
import googleapiclient.discovery

from .utils import datetime_to_string, string_to_datetime, youtube_duration_to_seconds


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

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
        return ListResponse(query)[0]

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
        return ListResponse(query)[0]


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

        if 'part' not in api_params:
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

        log.debug("executing query with the following parameters:")
        log.debug(pprint(query_params))
        return self.query_func(**query_params).execute()


class ListResponse(collections.Iterator):
    """Executes a query and creates a data structure containing Resource instances.

    When iterated over, this object behaves like an iterator, paging through the results and
    creating Resource instances (Video, Channel, Playlist...) as they are required.

    When indexed with an integer n, returns the nth Resource.

    When sliced, returns a list of Resource instances.

    Due to limitations in the API, you'll never get more than ~500 from a search result -
    definitely for the 'search' endoint and probably others as well. Also, the value given in
    pageInfo.totalResults for how many results are returned is pretty worthless.  It may be an
    estimate of total numbers of results _before filtering_, and it'll never be more than a
    million.  See this issue for more details: https://issuetracker.google.com/issues/35171641

    """
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
        self._no_more_pages = False     # flagged when we reach the end of the available results
        self._page_count = 0            # no. of pages processed
        self._item_count = 0            # total no. of items yielded
        self._next_page_token = None    # api page token required for the next page of results

    def __repr__(self):
        return "<ListResponse endpoint='{}', n={}, per_page={}>".format(
            self.query.endpoint, self.total_results, self.results_per_page
        )

    def __iter__(self):
        """Allow this object to act as an iterator."""
        return self

    def __next__(self):
        """Get the next resource.

        This method allows the list reponse to be iterated over.  First we fetch a page of search
        results, load the response into memory and and return each resource in turn.  If we're at
        the end of a page we fetch a new one, replacing the old page in memory.

        """
        # fetch the next page of items if we haven't fetched the first page yet, or alternatively
        #  if we've run out of results on this page.  this check relies on results_per_page being
        #  set if _listing is not None (which of course it should be).
        if self._listing is None or self._list_index >= self.results_per_page:
            self._fetch_next()

        # get the next item.  if this fails now we must be out of results.
        # note: often you'll still get a next page token, even if the results end on this page,
        # meaning the _no_more_pages flag will not be set.
        # in this case, the items list on the _next_ page should be empty, but we don't check this.
        try:
            item = self._listing[self._list_index]
        except IndexError:
            log.debug(f"exhausted all results at item {self._item_count} "
                      f"(item {self._list_index + 1} on page {self._page_count})")
            self._no_more_pages = True      # unnecessary but true
            raise StopIteration()

        self._list_index += 1
        self._item_count += 1
        return create_resource_from_api_response(self.youtube, item)

    def __getitem__(self, index):
        """Get a specific resource or list of resources.

        This method handles indexing by integer or slice, e.g.:
            listresponse[n]     returns the nth Resource instance
            listresponse[:n]    returns the first n Resources as a list

        We do this by just repeatedly calling the __next__() method until we have the items we're
        looking for, which is a pretty dumb way of doing it but it'll do for now.

        Before finding an item or items, we call _reset() so that if this response has been used
        as an iterator we go back and start again.  After the requested item or items have been
        found we _reset() again so that the response can still be iterated over.

        """
        if isinstance(index, int):
            # if an integer is used we just return a single item.  we'll just __next__()
            # along until we're there.  this is a bit silly because we're creating a resource for
            #  each call and only returning the final one, but it'll do for now.
            self._reset()
            try:
                for _ in range(index):
                    self.__next__()
            except StopIteration:
                raise IndexError("list index out of range")

            # store item to be returned
            item = self.__next__()

            # reset so that this object can still be used as a generator
            self._reset()

            return item

        elif isinstance(index, slice):
            # if a slice is used we want to return a list (not a generator).  we'll use
            # __next__() to build up the list.
            start = 0 if index.start is None else index.start
            stop = index.stop
            step = index.step

            if step not in (1, None):
                raise NotImplementedError("can't use a slice step other than one")

            if start < 0 or (stop is not None and stop < 0):
                raise NotImplementedError("can't use negative numbers in slices")

            # ok if all that worked let's reset so that __next__() gives the first item in the
            # list response
            self._reset()

            if start > 0:
                # move to start position
                try:
                    for _ in range(start):
                        self.__next__()
                except StopIteration:
                    # if the slice start is greater than the total length you usually get an empty
                    # list
                    return []

            if stop is not None:
                # iterate over the range provided by the slice
                range_ = range(start, stop)
            else:
                # make the for loop iterate until StopIteration is raised
                range_ = itertools.count()

            items = []
            for _ in range_:
                try:
                    items.append(self.__next__())
                except StopIteration:
                    # if the slice end is greater than the total length you usually get a
                    # truncated list
                    break

            self._reset()
            return items

        else:
            raise KeyError(f"you can't index a ListResponse with '{index}'")

    def _fetch_next(self):
        """Fetch the next page of the API response and load into memory."""
        if self._no_more_pages:
            # we should only get here if results stop at a page boundary
            log.debug(f"exhausted all results at item {self._item_count} at page boundary "
                      f"(item {self._list_index + 1} on page {self._page_count})")
            raise StopIteration()

        # pass the next page token if this is not the first page we're fetching
        params = dict()
        if self._next_page_token:
            params['pageToken'] = self._next_page_token

        # execute query to get raw response dictionary
        raw = self.query.execute(api_params=params)

        # the following data shouldn't change, so store only if it's not been set yet
        # (i.e. this is the first fetch)
        if None in (self.kind, self.total_results, self.results_per_page):
            # don't use get() because if this data doesn't exist in the api response something
            # has gone wrong and we'd like an exception
            self.kind = raw['kind'].replace('youtube#', '')
            self.total_results = int(raw['pageInfo']['totalResults'])
            self.results_per_page = int(raw['pageInfo']['resultsPerPage'])

        # whereever we are in the list response we need the next page token.  if it's not there,
        # set a flag so that we know there's no more to be fetched (note _next_page_token is also
        #  None at initialisation so we can't check it that way).
        self._next_page_token = raw.get('nextPageToken', None)
        if self._next_page_token is None:
            self._no_more_pages = True

        # store items in raw format for processing by __next__()
        self._listing = raw['items']    # would like a KeyError if this fails (it shouldn't)
        self._list_index = 0
        self._page_count += 1


def create_resource_from_api_response(youtube, item):
    """Given a raw item from an API response, return the appropriate Resource instance."""
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

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__

        # if they're different classes return NotImplemented instead of False so that we fallback
        #  to the default comparison method
        return NotImplemented

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))

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
