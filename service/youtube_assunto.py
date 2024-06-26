from typing import Dict
from datetime import datetime
import requests
from service.youtube_base import YoutubeBase


class YoutubeAssunto(YoutubeBase):

    def __init__(self, assunto: str, data_hora_busca: str) -> None:

        self._path_url = 'search'
        self._params = {
            'part': 'snippet',
            'regionCode': 'BR',
            'relevanceLanguage': 'pt',
            'maxResults': '50',
            'q': assunto,
            'publishedAfter': data_hora_busca,

        }
        super().__init__(params=self._params, path_url=self._path_url)
