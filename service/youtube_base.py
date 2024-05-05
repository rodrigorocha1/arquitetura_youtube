try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from typing import Dict, Iterable
from abc import ABC
import requests
from dotenv import load_dotenv
from datetime import datetime
from variaveis.variaveis import chave

load_dotenv()


class YoutubeBase(ABC):
    def __init__(self, params: Dict, path_url) -> None:
        self._url_base = os.environ['url_youtube']
        self._path_url = path_url
        self._KEY = chave
        self._params = params
        self._params['key'] = self._KEY

    def conectar_api(self) -> Dict:
        req = requests.get(url=self._url_base +
                           self._path_url, params=self._params, timeout=10)
        req = req.json()
        req['data_extracao'] = datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S'
        )

        return req

    def executar_paginacao(self,) -> Iterable[Dict]:
        next_token = ''
        while next_token is not None:
            response = self.conectar_api()
            if response:
                yield response
                try:
                    next_token = response['nextPageToken']
                    self._params['pageToken'] = next_token
                except KeyError:
                    break
