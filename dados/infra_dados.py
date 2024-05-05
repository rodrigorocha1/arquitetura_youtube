import os
from dados.iinfra_dados import IInfraDados


class InfraDados(IInfraDados):
    def __init__(
        self,
        diretorio_datalake: str,
        termo_assunto: str,
        metrica: str,
        path_data: str,
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
        self._CAMINHO_BASE = os.getcwd()
        self._diretorio_datalake = diretorio_datalake
        self._termo_assunto = termo_assunto.lower()
        self._metrica = metrica
        self._path_data = path_data
        self._nome_arquivo = nome_arquivo
        self._diretorio_completo = os.path.join(
            self._CAMINHO_BASE,
            'data',
            self._diretorio_datalake,
            self._termo_assunto,
            self._metrica,
            self._path_data
        ) if self._metrica is not None else os.path.join(
            self._CAMINHO_BASE,
            'data',
            self._diretorio_datalake,
            self._termo_assunto,
            self._path_data
        )
