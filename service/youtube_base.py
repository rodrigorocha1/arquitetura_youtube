from abc import ABC
from dotenv import load_dotenv
import requests
from typing import Dict, Iterable
import os
from datetime import datetime
load_dotenv()


class YoutubeBase(ABC):
    def __init__(self, requisicao: Dict) -> None:
        self._url_base = os.environ['url']
        self._KEY = requisicao

    def __conectar_api(self, params: Dict[str, str]) -> Dict:
        req = requests.get(url=self.criar_url, params=params)
        return req.json()

    def executar_paginacao(self,  param: Dict) -> Iterable[Dict]:

        i = 1
        next_token = ''

        while next_token is not None:
            response = self.__conectar_api(param)
            if response:
                json_response = response.json()
                json_response['data_extracao'] = datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'
                )
                yield json_response
                try:
                    next_token = json_response['nextPageToken']
                    param['pageToken'] = next_token
                except KeyError:
                    break
