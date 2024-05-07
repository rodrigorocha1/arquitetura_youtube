## Introdução
Este projeto tem como a proposta de elaborar uma arquitetura de extração da API do youtube até a elaboração do dashboard.


## Sobre o apache airflow:	
Para a construção das dags, foi utilizado o pacote airflow com o modulo decorators, onde são definidos os decoradores do python  task, dag e task_group.

## Atualizações em relação ao Projeto Anterior.
- Verificação de canais brasileiros
- Verificação de vídeo com comentário
- Verificação de de comentário com respostas

# Endpoints utilizados:

1. Pesquisar assunto do YouTube:
   - **URL:** `/youtube/v3/search`
   - **Descrição:** URL para obter os vídeos por assunto. Salva todo o histórico de busca, independente do idioma do canal.

2. Filtrar canais brasileiros:
   - **URL:** `/youtube/v3/channels`
   - **Descrição:** Após salvar os registros, esta URL é usada para consultar o idioma do canal e gerar uma lista de vídeos dos canais.

3. Obter dados dos vídeos:
   - **URL:** `/youtube/v3/videos`
   - **Descrição:** Após obter a lista de vídeos brasileiros, esta URL é usada para obter os dados completos. Os campos `viewCount`, `likeCount`, `favoriteCount`, `commentCount` serão usados, sempre gravando os dados históricos.

4. Extrair comentários:
   - **URL:** `/youtube/v3/commentThreads`
   - **Descrição:** Depois de extrair a lista de vídeos, esta URL é executada para extrair os comentários, gravando a requisição original em um datalakes.

5. Extrair respostas dos comentários:
   - **URL:** `/youtube/v3/comments`
   - **Descrição:** Depois de extrair os comentários, cada comentário tem um ID que mostra se possui respostas. Esta URL é usada para extrair as respostas dos comentários.


# Estrutura do datalake:
 A figura abaixo mostra a estrutura do datalake que foi construido.
 
![Exemplo de imagem](https://github.com/rodrigorocha1/arquitetura_youtube/blob/main/docs/estrutura_datalake.png)
![Exemplo de imagem](https://github.com/rodrigorocha1/arquitetura_youtube/blob/main/fig/arquitertura_dl.drawio.png)


# Diagrama de Classe:
![Exemplo de imagem](https://github.com/rodrigorocha1/arquitetura_youtube/blob/main/fig/diagrama%20de%20classe.png)


# Diagrama de Atividade:
![Exemplo de imagem](https://github.com/rodrigorocha1/arquitetura_youtube/blob/main/fig/diagrama_atividade.jpg)
