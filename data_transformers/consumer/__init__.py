from fundar.utils import has
from data_transformers import transformer

from pandas import DataFrame

def str_split(sep, **kwargs):
    return lambda x: str.split(x, sep,**kwargs)

def bytes_decode(encoding, **kwargs):
    return lambda x: bytes.decode(x, encoding, **kwargs)

def parse_source(source_code: str):
    definitions_range = source_code.which(has('DEFINITIONS'))
    definitions_range[1] += 1

    pipeline_range = source_code.which(has('PIPELINE'))
    pipeline_range[1] += 1

    definitions = source_code[slice(*definitions_range)]
    pipeline = source_code[slice(*pipeline_range)]

    definitions = definitions[1:-1].map(str.rstrip)

    convert_indices = definitions.which(has('@transformer.convert'))
    return_indices = definitions.which(has('return'))

    definitions_indices = convert_indices.zip(return_indices)

    f_definitions = [None for _ in definitions_indices]

    for k,(i,j) in enumerate(definitions_indices):
        f_definitions[k] = '\n'.join(definitions[i+1:j+1])

    pipeline_str = ''.join(pipeline[1:-1]).split('pipeline =')[1].lstrip()

    return pipeline_str, f_definitions

def transformer_from_source(source: str):
    if not source.startswith('def '):
        raise ValueError('Source string must start with function definition')
    
    left_paren = source.index('(')
    k = len('def ')
    name = source[k:left_paren]
    
    exec(source, globals())
    obj = globals().get(name)
    source = '@transformers.convert\n' + source
    obj = globals()[name] = transformer.convert(obj, name=name, external_sourcelines=source)
    return name, obj