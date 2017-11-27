from pytaw import YouTube
import pytest


@pytest.fixture
def youtube():
    return YouTube()


def test_video_title(youtube):
    video = youtube.video(id_='jNQXAC9IVRw')
    assert video.title == 'Me at the zoo'