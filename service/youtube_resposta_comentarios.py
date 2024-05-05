from typing import Dict
from service.youtube_base import YoutubeBase


class YoutubeRespostaComentario(YoutubeBase):
    def __init__(self, id_resposta_comentario: str) -> None:
        self._path_url = 'comments'
        self._id_resposta_comentario = id_resposta_comentario
        self._params = {
            'part': 'snippet',
            'parentId': self._id_resposta_comentario
        }
        super().__init__(params=self._params, path_url=self._path_url)
