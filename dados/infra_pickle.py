import pickle
from typing import List
import os
from dados.infra_dados import InfraDados


class InfraPicke(InfraDados):

    def __init__(self, diretorio_datalake: str, termo_assunto: str, metrica: str, path_data: str, nome_arquivo: str) -> None:
        super().__init__(diretorio_datalake, termo_assunto, metrica, path_data, nome_arquivo)

    def salvar_dados(self, **kwargs):
        """Método para guardar lista de vídeos str
        """

        if not os.path.exists(self._diretorio_completo):
            os.makedirs(self._diretorio_completo)
            var = kwargs['lista']
        else:
            try:
                lista = self.carregar_dados()
                var = kwargs['lista'] + lista
                var = list(set(var))
            except:
                var = kwargs['lista']

        with open(os.path.join(self._diretorio_completo, self._nome_arquivo), 'wb') as arquivo_pickle:
            if arquivo_pickle is not None:
                pickle.dump(var, arquivo_pickle)

    def carregar_dados(self) -> List[str]:
        """Método para abrir os id únicos, sejá vídeos, comentários e respostas

        Returns:
            List[str]: Lista de strings
        """
        caminho = os.path.join(self._diretorio_completo, self._nome_arquivo)
        if os.path.exists(caminho):
            with open(caminho, 'rb') as arquivo_pickle:
                lista_videos = pickle.load(arquivo_pickle)
            return lista_videos
