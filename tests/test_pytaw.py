import pytest

from pytaw import YouTube


@pytest.fixture
def youtube():
    """A YouTube instance initialised with a developer key loaded from config.ini"""
    return YouTube()


@pytest.fixture
def video(youtube):
    """A Video instance for the classic video 'Me at the zoo'"""
    return youtube.video(id='jNQXAC9IVRw')


@pytest.fixture
def channel(youtube):
    """A Channel instance for the 'YouTube Help' channel"""
    return youtube.channel(id='UCMDQxm7cUx3yXkfeHa5zJIQ')


class TestVideo(object):

    def test_video_title(self, video):
        assert video.title == "Me at the zoo"

    def test_video_published_at(self, video):
        assert video.published_at.isoformat() == '2005-04-24T03:31:52+00:00'

    def test_video_n_views(self, video):
        assert video.n_views > int(40e6)

class TestChannel(object):

    def test_channel_title(self, channel):
        assert channel.title == "YouTube Help"