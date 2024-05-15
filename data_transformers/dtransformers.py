
from functools import reduce as foldl

# ==================================================================================================

from typing import Callable, Any, Tuple, Self
from pandas import DataFrame
from copy import copy

class staticproperty(property):
    def __get__(self, cls, owner):
        return staticmethod(self.fget).__get__(None, owner)()

class transformer_t(Callable[[DataFrame, Any], DataFrame]): ...

class transformer_r(tuple):
    ...

import inspect

class transformer:
    def __init__(self, f: transformer_t, name=None, external_sourcelines: list = None):
        if name:
            self.name = name
        else:
            self.name = '<lambda_t>'

        if not external_sourcelines:        
            self.sourcelines = inspect.getsourcelines(f)
        else:
            self.sourcelines = external_sourcelines

        self.f = f

    def __call__(self, *args, **kwargs) -> Tuple[tuple, DataFrame]:
        return transformer_r(((args, kwargs), self.f(*args, **kwargs)))
    
    @staticmethod
    def convert(f):
        kwargs = {
            'external_sourcelines': inspect.getsourcelines(f),
            'name': f.__name__
        }
        
        def 


class chain:
    def __init__(self, *fs: transformer):
        if not all(isinstance(f, transformer) for f in fs):
            raise TypeError
        
        self.fs = fs
    
    def __call__(self, df) -> Tuple[list, DataFrame]:
        iterator = iter(self.fs)
        thunks = [(transformer(lambda: None, name='start'), (None, None), copy(df))]
        acc = df

        while (f := next(iterator, None)):
            thunk, result = f(acc)
            acc = result
            thunks.append((f, thunk, result))

        return thunks, result
    


drop: transformer
to_long: transformer
renombrar_columnas: transformer
sort_values: transformer
replace_values: transformer
exportar: transformer

