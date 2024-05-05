try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from abc import ABC, abstractmethod
from typing import Dict, Iterable
from dotenv import load_dotenv
from variaveis.variaveis import chave
load_dotenv()


class YoutubeBase(ABC):
    def __init__(self, params: Dict) -> None:
        self._url_base = os.environ['url_youtube']
        self._KEY = chave
        self._params = params
        self._params['key'] = self._KEY

    @abstractmethod
    def conectar_api(self) -> Dict:
        pass

    def executar_paginacao(self,) -> Iterable[Dict]:
        next_token = ''
        while next_token is not None:
            response = self.conectar_api()
            if response:
                yield response
                try:
                    next_token = response['nextPageToken']
                    self._params['pageToken'] = next_token
                    print('próximo token', next_token)
                except KeyError:
                    break
