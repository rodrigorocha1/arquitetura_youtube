try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from datetime import datetime
from airflow.decorators import task, dag, task_group
import pendulum
from service.youtube_assunto import YoutubeAssunto
from service.youtube_canal import YoutubeCanal
from dados.infra_json import InfraJson
from dados.infra_pickle import InfraPicke

data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
data_hora_busca_path = data_hora_atual.strftime('%Y-%m-%dT%H:%M:%SZ')
data_hora_busca = data_hora_atual.subtract(hours=10)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')
path_data = data_hora_busca_path.replace(
    "-", "_").replace("T", "_").replace("Z", "").replace(":", "_")


@task
def task_obter_assunto(
        assunto: str,
        data_hora_busca: str,
        path_data: str
):
    ya = YoutubeAssunto(
        assunto=assunto,
        data_hora_busca=data_hora_busca
    )
    pgs = ya.executar_paginacao()
    for pg in pgs:
        ij = InfraJson(
            diretorio_datalake='bronze',
            termo_assunto=assunto,
            metrica='requisicao_busca',
            path_data=f'extracao_{path_data}',
            nome_arquivo='req.json'
        )
        lista_canais_videos = [
            (item['snippet']['channelId'], item['id']['videoId'])
            for item in pg['items']
        ]
        lista_canais_videos_brasileiros = []
        for canal_video in lista_canais_videos:
            yc = YoutubeCanal(id_canal=canal_video[0])
            if yc.listar_canais():
                lista_canais_videos_brasileiros.append(canal_video)

        ij.salvar_dados(req=pg)


@task
def task_buscar_dados_canais(assunto: str, path_data: str):
    ifp = InfraPicke(
        diretorio_datalake='bronze',
        termo_assunto=assunto,
        metrica=None,
        path_data=None,
        nome_arquivo='id_canais.pkl'
    )

    lista_canais = ifp.carregar_dados()
    for canal in lista_canais:
        yc = YoutubeCanal(id_canal=canal)
        req = yc.conectar_api()

        ij = InfraJson(
            diretorio_datalake='bronze',
            termo_assunto=assunto,
            metrica='total_inscritos',
            path_data=f'extracao_{path_data}',
            nome_arquivo='req.json'
        )
        ij.salvar_dados(req=req)


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False
)
def dag_youtube():

    lista_assunto = ['linux', 'Cities Skylines 2']

    @task
    def inicio_dag():
        print('Inicio dag')

    @task
    def fim_dag():
        print('fim dag')

    @task_group
    def obter_assunto():

        for assunto in lista_assunto:
            task_obter_assunto.override(
                task_id=f"assunto_{assunto.replace(' ', '_').lower()}"
            )(assunto, data_hora_busca, path_data)

    @task_group
    def obter_dados_canais():
        for assunto in lista_assunto:
            task_buscar_dados_canais.override(
                task_id=f"assunto_{assunto.replace(' ', '_').lower()}"
            )(assunto, path_data)

    inicio_dag() >> obter_assunto() >> obter_dados_canais() >> fim_dag()


dag_youtube()
