import pickle
import os
from datetime import datetime
import pandas as pd
from typing import List, Dict


class Depara:
    def __init__(self, nm_arquivo: str, path_pasta: str) -> None:
        self.__pasta = path_pasta
        self.__CAMINHO_BASE = os.path.join(
            os.getcwd(), "src", "depara", self.__pasta)
        self.__caminho_completo = os.path.join(self.__CAMINHO_BASE, nm_arquivo)

    def abrir_picke(self, param_filtro: str) -> Dict[List, Dict]:
        with open(self.__caminho_completo, 'rb') as arq:
            dic_inputs = pickle.load(arq)
        dic_inputs_arq = dic_inputs[param_filtro]
        return dic_inputs_arq
