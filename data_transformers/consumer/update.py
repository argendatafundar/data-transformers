from ghwrappers.api import GitHubAPI, GithubFile
from io import BytesIO, StringIO, BufferedWriter
from fundar.structures import lista
from data_transformers import chain
from fundar import pandas as pd
from fundar import json
from base64 import b64encode
from data_transformers.consumer import (
    parse_source, 
    transformer_from_source, 
)
import warnings
import requests
import os.path
import sys

def df_csv_str(df):
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue()

class LazyAction:
    def __init__(self, f):
        self.f = f

    def run(self, *args, **kwargs):
        return self.f(*args, **kwargs)

def get_transformer_from_source(source_code: str):
    source_code = lista(source_code.split('\n'))
    pipeline_str, f_definitions = parse_source(source_code)

    defined_functions = lista(f_definitions).map(transformer_from_source)

    _globals = {}
    _globals |= dict(defined_functions)
    _globals |= dict(chain=chain)
    pipeline = eval(pipeline_str, _globals)
    return pipeline

def get_data_retry(data, retries=3, on_retry: callable = None):
    for i in range(retries):
        url = data.download_url
        try:
            result = requests.get(url)
            if result.status_code != 200:
                raise Exception(f'Error downloading {data.name} (status code {result.status_code})')
            return result.content
        except Exception as e:
            warnings.warn(f'Error downloading {data.name} (retry {i})')
            if on_retry:
                on_retry()
    raise Exception(f'Error downloading {data.name} (max retries)')


def get_subtopico_info(subtop: str) -> tuple[lista[GithubFile], lista[GithubFile], GithubFile]:
    transformers = GitHubAPI.ls('argendata/transformers', f'{subtop}/')
    transformers = lista[GithubFile](transformers)
    mappings = transformers.find(lambda x: x.name == 'mappings.json')

    data = GitHubAPI.ls('argendata/data', f'{subtop}/')
    data = lista[GithubFile](data)
    return transformers, data, mappings

def create_uploader(gid, data, transformer, path, repo):
    if not data:
        return LazyAction(lambda *args, **kwargs: print(f'{gid} skipped (data not found).'))

    data_content = get_data_retry(data, on_retry=data.update)
    data_content = data_content.decode('utf-8')

    df = pd.read_csv(StringIO(data_content))

    if not transformer:
        result = df
    else:
        transformer_content = get_data_retry(transformer, on_retry=transformer.update)
        try:
            transformer_content = transformer_content.decode('utf-8')
        except UnicodeDecodeError:
            transformer_content = transformer_content.decode('latin-1')

        data_transformer = get_transformer_from_source(transformer_content)

        _, result = data_transformer(df)

    result_str = df_csv_str(result)
    result_str = b64encode(result_str.encode('utf-8')).decode('utf-8')

    lazy_upload = GitHubAPI._upload(
        encoded_resource=result_str, 
        repo=repo, 
        repo_path=os.path.dirname(path)+'/',
        name=os.path.basename(path),
        replace=True)
    
    return lazy_upload

def print_help():
    print('Usage: updater.py [SUBTOP] <run>')
    print("El argumento 'run' sube los archivos a graficos/")
    print("En caso contrario, sólo se aplican los transformers.")
    print("(esto ultimo sirve para ver si hay errores en los transformers)")

def main(subtopico, *args):
    if 'help' in (subtopico, *args):
        print_help()
        return 0

    run = False
    if len(args) > 0:
        run = True if 'run' in args else False

    
    transformers, data, mappings = \
        get_subtopico_info(subtopico)

    mappings: bytes = get_data_retry(mappings)
    mappings: str = mappings.decode('utf-8')
    mappings = json.loads(mappings)

    workspace = {
    p: {'data': data.find(lambda x: x.name == k),
        'transformer': transformers.find(lambda x: p in x.name),
        'target': ('argendata/graficos', f'{subtopico}/{p}/data/{p}.csv')}
    for k,v in mappings.items()
    for x in v
    for p in (x['public'], ) # alias
    }

    for gid, v in workspace.items():
        print(f'Processing {gid}')
        data: GithubFile = v['data']
        transformer: GithubFile = v['transformer']
        repo, path = v['target']

        workspace[gid]['upload_action'] = create_uploader(gid, data, transformer, path, repo)

    if run:
        for gid, v in workspace.items():
            print(f'Actioning {gid}')
            v['upload_action'].run()

    return 0

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Not enough arguments!')
        print_help()
        exit(1)
    
    exit(main(*sys.argv[1:]))