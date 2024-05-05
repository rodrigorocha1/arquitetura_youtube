import os
from dados.iinfra_dados import IInfraDados


class InfraDados(IInfraDados):
    def __init__(
        self,
        diretorio_datalake: str,
            termo_assunto: str,
            path_extracao: str,
            metrica: str,
            nome_arquivo: str,

    ) -> None:
        """_summary_

        Args:
            diretorio_datalake (str): _description_
            termo_assunto (str): _description_
            path_extracao (str): _description_
            metrica (str): _description_
            nome_arquivo (str): _description_
        """
        self._diretorio_datalake = diretorio_datalake
        self._termo_assunto = termo_assunto.lower()
        self._path_extracao = path_extracao
        self._metrica = metrica
        self._nome_arquivo = nome_arquivo
        self._CAMINHO_BASE = os.getcwd()
        self._diretorio_completo = os.path.join(
            self._CAMINHO_BASE,
            self._diretorio_datalake,
            self._termo_assunto,
            self._path_extracao,       
            self._metrica,
            self._nome_arquivo
        ) if self._metrica is not None else os.path.join(
            self._CAMINHO_BASE,
            self._diretorio_datalake,
            self._termo_assunto,
            self._path_extracao,

        )
