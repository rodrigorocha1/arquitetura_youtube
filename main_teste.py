
import pendulum
from dados.dados_youtube import DadosYoutube
from service.youtube_assunto import YoutubeAssunto
from dados.infra_json import InfraJson
from dados.infra_pickle import InfraPicke


data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
data_hora_busca = data_hora_atual.subtract(hours=10)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')
assunto = 'Cities Skylines 2'


ya = YoutubeAssunto(assunto=assunto.replace(' ', ''), data_hora_busca=data_hora_busca)
pgs = ya.executar_paginacao()
path_data = data_hora_busca.replace("-", "_").replace("T", "_").replace("Z", "").replace(":","_")
for pg in pgs:
    print(path_data)
    ij = InfraJson(
        diretorio_datalake='bronze',
        termo_assunto=assunto.replace(' ', ''),
        metrica='requisicao_busca',
        path_data=f'extracao_{path_data}',
        nome_arquivo='req.json'
    )
    lista_videos = DadosYoutube.obter_lista_videos(req=pg)
    print(lista_videos)
    ij.salvar_dados(req=pg)
    lista_canais = [ item['snippet']['channelId'] for item in pg['items']]
    print(lista_canais)

    ifp = InfraPicke(
        diretorio_datalake='bronze',
        termo_assunto=assunto.replace(' ', ''),
        metrica=None,
        path_data=f'extracao_{path_data}',
        nome_arquivo='videos.pkl'
    )
    ifp.salvar_dados(lista=lista_videos)
    
    # Colocar aqui para verificar o canal e gravar o arquivo pkl se for canal brasileiro

    print()
