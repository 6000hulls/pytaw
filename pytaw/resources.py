from datetime import timedelta

from .utils import string_to_datetime, youtube_duration_to_seconds


class Resource(object):

    resource_type = 'resource'

    def __init__(self, id, item):
        # every resource has a unique id, it'll be a different format for each resource type though
        self.id = id

        # this is the api response item for the resource.  it's a dictionary with 'kind',
        # 'etag' and 'id' keys, at least.  it may also have a 'snippet', 'contentDetails' etc.
        # containing more detailed info.  in theroy, this dictionary could be access directly,
        # but we'll make the data accessible via class attributes where possible.
        self._item = item

        # store key information from the above item response as attributes.  if those attributes
        # don't exist (for example, we didn't specify the right part in initial query) they'll be
        #  set to None.
        self._set_attributes()

    def _get(self, *keys):
        """Get an attribute from the stored item response, if it exists.

        If it doesn't, return None.

        """
        param = self._item
        for key in keys:
            param = param.get(key, None)
            if param is None:
                return None

        return param

    def _set_attributes(self):
        raise NotImplementedError("method should be overridden by subclass.")


class Video(Resource):

    resource_type = 'video'

    def _set_attributes(self):
        part = 'snippet'
        self.title = self._get(part, 'title')
        self.description = self._get(part, 'description')
        self.published_at = string_to_datetime(self._get(part, 'publishedAt'))
        self.tags = self._get(part, 'tags')
        self.channel_id = self._get(part, 'channelId')
        self.channel_title = self._get(part, 'channelTitle')

        part = 'contentDetails'
        duration_iso8601 = self._get(part, 'duration')
        if duration_iso8601 is not None:
            self.duration = timedelta(seconds=youtube_duration_to_seconds(duration_iso8601))
        else:
            self.duration = None

        part = 'status'
        self.license = self._get(part, 'license')

        part = 'statistics'
        self.n_views = self._get(part, 'viewCount')
        self.n_likes = self._get(part, 'likeCount')
        self.n_dislikes = self._get(part, 'dislikeCount')
        self.n_favorites = self._get(part, 'favoriteCount')
        self.n_comments = self._get(part, 'commentCount')

    def __repr__(self):
        if self.title:
            return "<Video {}: \"{:.32}\" by {}>".format(self.id, self.title, self.channel_title)
        else:
            return "<Video {}>".format(self.id)


class Channel(Resource):

    resource_type = 'channel'

    def _set_attributes(self):
        part = 'snippet'
        self.title = self._get(part, 'title')
        self.description = self._get(part, 'description')
        self.published_at = string_to_datetime(self._get(part, 'publishedAt'))
        self.country = self._get(part, 'channelId')

    def __repr__(self):
        if self.title:
            return "<Channel {}: {}>".format(self.id, self.title)
        else:
            return "<Channel {}>".format(self.id)