
import pendulum

from service.youtube_canal import YoutubeCanal
from service.youtube_assunto import YoutubeAssunto
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
            ifp.salvar_dados(lista=canal_video_br[chave])
