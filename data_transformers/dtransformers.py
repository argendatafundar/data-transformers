from typing import Callable, Any, Tuple
from pandas import DataFrame
from inspect import Parameter
import warnings
from copy import copy

class staticproperty(property):
    def __get__(self, cls, owner):
        return staticmethod(self.fget).__get__(None, owner)()

class transformer_t(Callable[[DataFrame, Any], DataFrame]): ...

class transformer_r(tuple):
    ...

import inspect

class transformer:
    def __init__(self, 
                 f: transformer_t, 
                 name = None, 
                 external_sourcelines: list = None, 
                 partially_applied_args: dict = None):
        
        if name:
            self.name = name
        else:
            self.name = '<lambda_t>'
        
        self.partially_applied_args = partially_applied_args

        if not external_sourcelines:        
            self.sourcelines = inspect.getsourcelines(f)
        else:
            self.sourcelines = external_sourcelines

        self.f = f

    def __call__(self, *args, **kwargs) -> Tuple[dict, DataFrame]:
        if kwargs or len(args) > 1:
            warnings.warn("Warning, transformer being applied with more than one argument.")

        params = inspect.signature(self.f).parameters
        applied_args = {k: v for k, v in zip(params.keys(), args)}
        final_args = {**self.partially_applied_args, **applied_args}

        return transformer_r((final_args, self.f(df=applied_args['df'])))
    
    @staticmethod
    def convert(f):
        kwargs = {
            'external_sourcelines': inspect.getsourcelines(f),
            'name': f.__name__
        }
        
        params = inspect.signature(f).parameters

        if len(params) == 0:
            raise TypeError(f"Function {f.__name__} has to at least take a DataFrame as parameter")
        
        has_df = params.get('df', None)
        
        if has_df is None:
            raise TypeError(f"Function {f.__name__} doesn't have a 'df' parameter")
        
        if has_df.default is not inspect.Parameter.empty:
            raise TypeError(f"Function {f.__name__} cannot have 'df' as a default parameter")
        
        def new_f(*ags, **kw):
            _params = {k: Parameter.empty for k in params.keys() if k != 'df'}
            positional_args = {k: v for k, v in zip(_params.keys(), ags)}
            keyword_args = {k: v for k, v in kw.items()}
            default_args = {k: v.default for k, v in params.items() if v.default is not Parameter.empty}
            intersection = set(positional_args.keys()).intersection(set(keyword_args.keys()))
            
            if 0 != len(intersection):
                raise TypeError(f"Can't merge positional and keyword arguments")

            merge = ( _params
                  | default_args
                  | positional_args
                  | keyword_args)
            
            merge = {k: v for k, v in merge.items() if v is not Parameter.empty}

            def currified_df(df):
                full_args = {**merge, 'df': df}
                return f(**full_args)
            
            return transformer(currified_df, **kwargs, partially_applied_args=merge)
        
        if len(params) > 1:
            return new_f
        else:
            return new_f()
    
    def __str__(self):
        return f"<transformer '{self.name}'>"
    
    def __repr__(self):
        return f"<transformer '{self.name}'>"



class chain:
    def __init__(self, *fs: transformer):
        if not all(isinstance(f, transformer) for f in fs):
            cases = [f for f in fs if not isinstance(f, transformer)]
            types = [type(f).__name__ for f in cases]
            cases_with_types_str = zip(cases, types)
            cases_with_types_str = ', '.join(f"'{x[0]}': '{x[1]}'" for x in cases_with_types_str)
            raise TypeError(f"Expected all arguments to be of type 'transformer', got {cases_with_types_str}")
        
        self.fs = fs

    def transformers_source(self, hide_decorators=False):
        sources = []
        for f in self.fs:
            src, count = f.sourcelines
            if hide_decorators:
                if src[0].startswith('@'):
                    src = src[1:]
            sources.append(''.join(src))
        return sources
    
    def __call__(self, df) -> Tuple[list, DataFrame]:
        if not self.fs:
            return [], df
        iterator = iter(self.fs)
        thunks = [(transformer(lambda: None, name='start'), {}, copy(df))]
        acc = df

        while (f := next(iterator, None)):
            params, result = f(acc)
            acc = result
            thunks.append((f, params, result))

        return thunks, result
