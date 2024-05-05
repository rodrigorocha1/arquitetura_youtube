
import pendulum

from service.youtube_canal import YoutubeCanal
from service.youtube_assunto import YoutubeAssunto
from service.youtube_video import YoutubeVideo
from service.youtube_canal import YoutubeCanal
from service.youtube_comentario import YoutubeComentario
from service.youtube_resposta_comentarios import YoutubeRespostaComentario
from dados.infra_json import InfraJson
from dados.infra_pickle import InfraPicke


data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
data_hora_busca_path = data_hora_atual.strftime('%Y-%m-%dT%H:%M:%SZ')
data_hora_busca = data_hora_atual.subtract(hours=10)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')
assunto = 'Cities Skylines 2'
assunto = assunto.replace(' ', '_')


ya = YoutubeAssunto(
    assunto=assunto,
    data_hora_busca=data_hora_busca
)
pgs = ya.executar_paginacao()
path_data = data_hora_busca_path.replace(
    "-", "_").replace("T", "_").replace("Z", "").replace(":", "_")
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


# buscar dados canais
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


# busca_videos
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


# busca comentarios
ifp = InfraPicke(
    diretorio_datalake='bronze',
    termo_assunto=assunto,
    metrica=None,
    path_data=None,
    nome_arquivo='lista_id_videos_comentarios.pkl'
)
lista_videos_comentarios = ifp.carregar_dados()

for video_comentario in lista_videos_comentarios:
    yc = YoutubeComentario(id_video=video_comentario)
    pgs = yc.executar_paginacao()
    for pg in pgs:

        lista_resposta_comentarios = yc.obter_resposta_comentarios(req=pg)
        ij = InfraJson(
            diretorio_datalake='bronze',
            termo_assunto=assunto,
            metrica='comentarios',
            path_data=f'extracao_{path_data}',
            nome_arquivo='req.json'
        )
        ij.salvar_dados(req=req)

        ifp = InfraPicke(
            diretorio_datalake='bronze',
            termo_assunto=assunto,
            metrica=None,
            path_data=None,
            nome_arquivo='lista_resposta_comentarios.pkl'
        )
        ifp.salvar_dados(lista=lista_resposta_comentarios)


# Resposta comentários

ifp = InfraPicke(
    diretorio_datalake='bronze',
    termo_assunto=assunto,
    metrica=None,
    path_data=None,
    nome_arquivo='lista_resposta_comentarios.pkl'
)
lista_videos_resposta_comentarios = ifp.carregar_dados()
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
