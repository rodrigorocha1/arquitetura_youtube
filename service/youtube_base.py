from abc import ABC, abstractmethod
from dotenv import load_dotenv
import os
import requests
from typing import List, Dict, Iterable
from datetime import datetime
load_dotenv()


class YoutubeBase(ABC):
    def __init__(self) -> None:
        self._url = os.environ['url']
        self._KEY = os.environ['key']

    @abstractmethod
    def criar_url(self) -> str:
        pass

    def __conectar_api(self, params: Dict[str, str]) -> Dict:
        req = requests.get(url=self.criar_url, params=params)
        return req.json()

    def _executar_paginacao(self,  params: List[Dict]) -> Iterable[Dict]:

        i = 1
        next_token = ''
        for param in params:
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
                else:
                    break
