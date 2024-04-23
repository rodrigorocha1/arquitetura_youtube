from service.youtube_base import YoutubeBase
from typing import Dict


class YoutubeAssunto(YoutubeBase):
    def __init__(self, assunto: str, data_inicio: str) -> None:
        self._assunto = assunto
        self._data_inicio = data_inicio
        super().__init__()

    def criar_url(self) -> str:
        return self._url + '/search'

    @property
    def params(self) -> Dict[str, str]:
        return {
            'part':  'snippet',
            'key': self._KEY,
            'regionCode': 'BR',
            'relevanceLanguage': 'pt',
            'maxResults': '50',
            'publishedAfter': self._data_inicio,
            'q': self._assunto,
            'pageToken': ''
        }
