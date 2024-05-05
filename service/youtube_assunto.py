from typing import Dict
from datetime import datetime
import requests
from service.youtube_base import YoutubeBase


class YoutubeAssunto(YoutubeBase):
    def __init__(self, assunto: str, data_hora_busca: str) -> None:

        self.__path_url = 'search'
        self.__params = {
            'part': 'snippet',
            'regionCode': 'BR',
            'relevanceLanguage': 'pt',
            'maxResults': '50',
            'q': assunto,
            'publishedAfter': data_hora_busca,

        }
        super().__init__(params=self.__params)

    def conectar_api(self) -> Dict:
        req = requests.get(url=self._url_base +
                           self.__path_url, params=self.__params, timeout=10)
        req = req.json()
        req['data_extracao'] = datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S'
        )

        return req
