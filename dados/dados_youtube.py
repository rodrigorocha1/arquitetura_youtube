try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from typing import Dict, List
import requests
import variaveis.variaveis as v


class DadosYoutube():

    @classmethod
    def verificar_idioma_canal(cls, id_canal: str) -> bool:
        """Método para verificar se o canal é brasileiro

        Args:
            id_canal (str): id do canal

        Returns:
            bool: verdadeiro ou falso
        """
        try:
            params = {
                'part': 'snippet,contentDetails, id',
                'key': v.chave,
                'id': id_canal,
                'maxResults': '100'
            }
            url = v.url + 'channels/'
            response = requests.get(url=url, params=params)
            req = response.json()
            flag = req['items'][0]['snippet']['country']
            if flag == 'BR':
                return True, req
            return False
        except:
            return False

    @classmethod
    def obter_lista_videos(cls, req: Dict) -> List[str]:
        """Método para obter os vídeos dos canais brasileiros

        Args:
            req (Dict): requisição da api do youtube

        Returns:
            List[str]: Lista de vídeos Brasileiros
        """
        lista_videos = []
        for item in req['items']:
            if cls.verificar_idioma_canal(item['snippet']['channelId']):
                lista_videos.append(item['id']['videoId'])
        return list(set(lista_videos))

    @classmethod
    def obter_lista_comentarios(cls, req: Dict) -> List[str]:
        lista_id_comentarios_encandeados = []
        for comment in req['items']:
            lista_id_comentarios_encandeados.append(comment['id'])
        return lista_id_comentarios_encandeados
