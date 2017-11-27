# PYTAW: Python YouTube API Wrapper

```python
>>> from pytaw import YouTube
>>> youtube = YouTube(key='your_api_key')
>>> video = youtube.video(id='jNQXAC9IVRw')
>>> video.title
'Me at the zoo'
>>> video.duration
datetime.timedelta(0, 19)
```