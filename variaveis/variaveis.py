# import os
from airflow.models import Variable


# chave = os.environ['chave_um']
# url = os.environ['url_youtube']


chave = Variable.get('chave_um')
url = Variable.get('url_youtube')
