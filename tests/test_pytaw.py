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


@pytest.fixture
def video_search(youtube):
    """A ListResponse instance corresponding to a video search for the query 'python'"""
    return youtube.search(search_string='python', type_='video')


class TestVideo:

    def test_title(self, video):
        assert video.title == "Me at the zoo"

    def test_published_at(self, video):
        assert video.published_at.isoformat() == '2005-04-24T03:31:52+00:00'

    def test_n_views(self, video):
        assert video.n_views > int(40e6)

    def test_tags(self, video):
        assert video.tags == ['jawed', 'karim', 'elephant', 'zoo', 'youtube', 'first', 'video']


class TestChannel:

    def test_title(self, channel):
        assert channel.title == "YouTube Help"


class TestSearch:

    def test_video_search_returns_a_video(self, video, video_search):
        assert type(video) == type(video_search.first())

    def test_video_search_has_many_results(self, video_search):
        assert video_search.total_results > 10000
