from typing import Dict
from service.youtube_base import YoutubeBase


class YoutubeVideo(YoutubeBase):
    def __init__(self, params: Dict) -> None:
        self._path_url = 'videos'
        self._params = {
            'part': 'snippet',
            'regionCode': 'BR',
            'relevanceLanguage': 'pt',
            'maxResults': '50',
            'q': assunto,
            'publishedAfter': data_hora_busca,

        }
        super().__init__(params, )
