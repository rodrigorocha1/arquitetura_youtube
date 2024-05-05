from typing import Dict, List
from datetime import datetime
import requests
from service.youtube_base import YoutubeBase


class YoutubeCanal(YoutubeBase):
    def __init__(self, id_canal: str) -> None:
        self.__path_url = 'channels'
        self._id_canal = id_canal
        self._params = {
            'part': 'snippet,statistics',
            'id': self._id_canal,
            'maxResults': '100'
        }
        super().__init__(params=self._params)

    def __verificar_idioma(self, req: Dict) -> bool:

        try:
            flag = req['items'][0]['snippet']['country']
            if flag == 'BR':
                return True
            return False
        except:
            return False

    def listar_canais(self) -> bool:
        req = self.conectar_api()
        flag = self.__verificar_idioma(req=req)
        return flag

    def conectar_api(self) -> Dict:
        req = requests.get(url=self._url_base +
                           self.__path_url, params=self._params, timeout=10)
        req = req.json()
        req['data_extracao'] = datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S'
        )

        return req
