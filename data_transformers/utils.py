import io
from pandas import DataFrame

def get_dataframe_info(df):
    buf = io.StringIO()
    df.info(buf=buf)
    return buf.getvalue()

def dict_to_str(d):
    return ','.join([f'{k}={v}' for k,v in d.items()])

def callstack_to_str(callstack):
    frames = []
    for f, params, presult in callstack:
        frame = []
        params.pop('df', None)
        params_str = dict_to_str(params)
        params_str = f'{f.name}({params_str})'
        frame.append(params_str)
        if isinstance(presult, DataFrame):
            info = '\n'.join(get_dataframe_info(presult).split('\n')[1:-3])
            frame.append(info)
            frame.append('')
            frame.append(presult.head(1).to_markdown())

        frames.append('\n'.join(frame))
        frames.append('')
        frames.append('-'*30)
        frames.append('')

    return '\n'.join(frames)