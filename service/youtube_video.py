from service.youtube_base import YoutubeBase


class YoutubeVideo(YoutubeBase):
    def __init__(self, id_video: str) -> None:
        self._path_url = 'videos'
        self._params = {
            'part': 'statistics,contentDetails,id,snippet,status',
            'id': id_video,
            'relevanceLanguage': 'pt',
            'regionCode': 'BR'

        }
        super().__init__(params=self._params, path_url=self._path_url)
