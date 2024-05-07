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
from service.youtube_video import YoutubeVideo
from service.youtube_comentario import YoutubeComentario
from service.youtube_resposta_comentarios import YoutubeRespostaComentario
from dados.infra_json import InfraJson
from dados.infra_pickle import InfraPicke

data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
data_hora_busca_path = data_hora_atual.strftime('%Y-%m-%dT%H:%M:%SZ')
data_hora_busca = data_hora_atual.subtract(hours=6)
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

        nome_arquivos = ['id_canais.pkl', 'id_videos.pkl']

        for canal_video_br in lista_canais_videos_brasileiros:
            for chave, nome_arquivo in enumerate(nome_arquivos):
                ifp = InfraPicke(
                    diretorio_datalake='bronze',
                    termo_assunto=assunto,
                    metrica=None,
                    path_data=None,
                    nome_arquivo=nome_arquivo
                )
                ifp.salvar_dados(lista=[canal_video_br[chave]])


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


@task
def task_obter_dados_videos(assunto: str, path_data: str):
    ifp = InfraPicke(
        diretorio_datalake='bronze',
        termo_assunto=assunto,
        metrica=None,
        path_data=None,
        nome_arquivo='id_videos.pkl'
    )
    lista_videos = ifp.carregar_dados()

    for video in lista_videos:
        yv = YoutubeVideo(id_video=video)
        req = yv.conectar_api()
        ij = InfraJson(
            diretorio_datalake='bronze',
            termo_assunto=assunto,
            metrica='estatistica_video',
            path_data=f'extracao_{path_data}',
            nome_arquivo='req.json'
        )
        if yv.verificar_comentarios(req=req):
            ifp = InfraPicke(
                diretorio_datalake='bronze',
                termo_assunto=assunto,
                metrica=None,
                path_data=None,
                nome_arquivo='lista_id_videos_comentarios.pkl'
            )
            ifp.salvar_dados(lista=[video])

        ij.salvar_dados(req)


@task
def task_obter_dados_comentarios(assunto: str, path_data: str):
    ifp = InfraPicke(
        diretorio_datalake='bronze',
        termo_assunto=assunto,
        metrica=None,
        path_data=None,
        nome_arquivo='lista_id_videos_comentarios.pkl'
    )

    lista_videos_comentarios = ifp.carregar_dados()

    if lista_videos_comentarios is not None:

        for video_comentario in lista_videos_comentarios:
            yc = YoutubeComentario(id_video=video_comentario)
            pgs = yc.executar_paginacao()
            for pg in pgs:

                lista_resposta_comentarios = yc.obter_resposta_comentarios(
                    req=pg)
                ij = InfraJson(
                    diretorio_datalake='bronze',
                    termo_assunto=assunto,
                    metrica='comentarios',
                    path_data=f'extracao_{path_data}',
                    nome_arquivo='req.json'
                )
                ij.salvar_dados(req=pg)

                ifp = InfraPicke(
                    diretorio_datalake='bronze',
                    termo_assunto=assunto,
                    metrica=None,
                    path_data=None,
                    nome_arquivo='lista_resposta_comentarios.pkl'
                )
                ifp.salvar_dados(lista=lista_resposta_comentarios)


@task
def task_ober_resposta_comentarios(assunto: str, path_data: str):
    ifp = InfraPicke(
        diretorio_datalake='bronze',
        termo_assunto=assunto,
        metrica=None,
        path_data=None,
        nome_arquivo='lista_resposta_comentarios.pkl'
    )

    lista_videos_resposta_comentarios = ifp.carregar_dados()
    if lista_videos_resposta_comentarios is not None:
        for video_resposta_comentarios in lista_videos_resposta_comentarios:
            if video_resposta_comentarios[1] > 0:
                yrc = YoutubeRespostaComentario(
                    id_resposta_comentario=video_resposta_comentarios[0]
                )
                pgs = yrc.executar_paginacao()
                for pg in pgs:
                    ij = InfraJson(
                        diretorio_datalake='bronze',
                        termo_assunto=assunto,
                        metrica='respota_comentarios',
                        path_data=f'extracao_{path_data}',
                        nome_arquivo='req.json'
                    )
                    ij.salvar_dados(req=pg)


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

    @task
    def sem_dados():
        print('Sem dados')

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

    @task_group
    def obter_dados_videos():
        for assunto in lista_assunto:
            task_obter_dados_videos.override(
                task_id=f"assunto_{assunto.replace(' ', '_').lower()}"
            )(assunto, path_data)

    @task_group
    def obter_dados_comentarios():
        for assunto in lista_assunto:
            task_obter_dados_comentarios.override(
                task_id=f"assunto_{assunto.replace(' ', '_').lower()}"
            )(assunto, path_data)

    @task_group
    def obter_dados_resposta_comentarios():
        for assunto in lista_assunto:
            task_ober_resposta_comentarios.override(
                task_id=f"assunto_{assunto.replace(' ', '_').lower()}"
            )(assunto, path_data)

    inicio_dag() >> obter_assunto() >> obter_dados_canais(
    ) >> obter_dados_videos() >> obter_dados_comentarios() >> obter_dados_resposta_comentarios() >> fim_dag()


dag_youtube()
