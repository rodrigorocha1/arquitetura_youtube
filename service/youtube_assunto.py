from service.youtube_base import YoutubeBase
from typing import List


class YoutubeAssunto(YoutubeBase):
    def __init__(self, assuntos: List[str], data_inicio: str) -> None:
        self._assuntos = assuntos
        self._data_inicio = data_inicio
        super().__init__()

    def criar_url(self):
        return self._url + '/search'

    @property
    def params(self):
        return [
            {
                'part':  'snippet',
                'key': self._KEY,
                'regionCode': 'BR',
                'relevanceLanguage': 'pt',
                'maxResults': '50',
                'publishedAfter': self._data_inicio,
                'q': assunto,
                'pageToken': ''
            } for assunto in self._assuntos
        ]
