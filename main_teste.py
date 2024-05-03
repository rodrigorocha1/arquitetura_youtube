
from service.youtube_assunto import YoutubeAssunto
import pendulum
data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
data_hora_busca = data_hora_atual.subtract(hours=4)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')


ya = YoutubeAssunto(assunto='pop os', data_hora_busca=data_hora_busca)
pgs = ya.executar_paginacao()

for pg in pgs:
    print(pg)
    print()
