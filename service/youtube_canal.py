from typing import Dict
from datetime import datetime
import requests
from service.youtube_base import YoutubeBase

class YoutubeCanal(YoutubeBase):
    def __init__(self, params: Dict, id_canal: str) -> None:
        self.__path_url = 'channels'
        self._id_canal = id_canal
        self._params = {
            'part': 'snippet,statistics',
            'id': self._id_canal
        }
        super().__init__(params=self._params)

    def conectar_api(self) -> Dict:
        req = requests.get(url=self._url_base +
                           self.__path_url, params=self._params, timeout=10)
        req = req.json()
        req['data_extracao'] = datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S'
        )

        return req
