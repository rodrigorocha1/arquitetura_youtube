from service.youtube_base import YoutubeBase
from typing import Dict


class YoutubeAssunto(YoutubeBase):
    def __init__(self, assunto: str, data_inicio: str, requisicao: Dict) -> None:
        self._assunto = assunto
        self._data_inicio = data_inicio
        self._url_chamada = self._url_base + 'search'
        super().__init__(requisicao=requisicao)
