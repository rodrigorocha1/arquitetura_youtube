from typing import Dict, List
from service.youtube_base import YoutubeBase


class YoutubeComentario(YoutubeBase):
    def __init__(self, id_video: str) -> None:
        self._path_url = 'commentThreads'
        self._id_video = id_video
        self._params = {
            'part': 'snippet,statistics',
            'id': self._id_video,
            'maxResults': '100'
        }
        super().__init__(params=self._params, path_url=self._path_url)

    def obter_resposta_comentarios(self, req: Dict):
        try:
            lista_resposta_comentarios = [
                (item['id'], item['snippet']['totalReplyCount'])
                for item in req['items']
            ]
        except Exception:
            return None
        return lista_resposta_comentarios
