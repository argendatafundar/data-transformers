<div align="center">
<a href="https://fund.ar">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./assets/data_transformers.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/user-attachments/assets/2d2d5751-b9c6-4075-a052-0a34a5047a2d">
    <img src="fund.ar" width="400"></img>
  </picture>
</a>
</div>

_Data Transformers_ es una biblioteca para Python que busca facilitar la escritura, ejecución, reproducibilidad y el versionado del código fuente que se realice para manipular datos estructurados.

Forma parte del conjunto de [herramientas](https://github.com/argendata/backend/tree/main/tools) de ArgenData para el análisis y procesamiento de datos manual-asistido.

# Uso

Supongamos que queremos transformar un `.csv` `CRECIM_g02.csv`[^1], lo cargamos y manipulamos de la siguiente manera:

[^1]: Cada _dataset_ de Argendata tiene un nombre sustantivo y uno simplificado que funciona como _id_, que sigue un formato del tipo `TOPICO_g01.csv`. Para nuestro ejemplo, a `CRECIM_g02.csv` le corresponde[`pib_vs_expectativa.csv`](https://github.com/argendata/data/blob/main/CRECIM/pib_vs_expectativa.csv). El flujo de reproducitibilidad implica en los hechos que se trata de dos _datasets_ diferentes: (i) uno que es el resultado del proceso que generan los investigadores para luego ser reproducidos en el proceso de [ETL](https://github.com/argendata/etl/); y  (ii) otro que es re transformado para darle la forma requerida por el Frontend en el proceso de graficación. Estas equivalencias entre versiones de `.csv` están contenidas en un mapeo dentro de cada subdirectorio de topicos en el repositiorio de [_transformers_](https://github.com/argendata/transformers/blob/main/CRECIM/mappings.json#L11) 


```python
from data_transformers.default_transformers import *
from data_transformers.utils import *
from data_transformers import *

data = pd.read_csv('./CRECIM_g02.csv')
```

|    | iso3   | pais_nombre   | continente_fundar                      |   anio |   expectativa_al_nacer |   pib_pc | nivel_agregacion   |
|---:|:-------|:--------------|:---------------------------------------|-------:|-----------------------:|---------:|:-------------------|
|  0 | ABW    | Aruba         | América del Norte, Central y el Caribe |   1990 |                  73.08 |  30823.5 | pais               |
|  1 | ABW    | Aruba         | América del Norte, Central y el Caribe |   1991 |                  73.1  |  32222.7 | pais               |
|  2 | ABW    | Aruba         | América del Norte, Central y el Caribe |   1992 |                  73.18 |  32986.5 | pais               |
|  3 | ABW    | Aruba         | América del Norte, Central y el Caribe |   1993 |                  73.23 |  34336.6 | pais               |

```python
# Serie de transformaciones que se aplican *en orden* al dataframe
# Están definidas como default_transformers
pipeline = chain(
    drop_col('pais_nombre'),
    drop_col('continente_fundar'),
    drop_col('nivel_agregacion'),
    wide_to_long(primary_keys),
    rename_cols({'iso3': 'geocodigo'})
)

# Se aplican las transformaciones resultando en:
callstack, result = pipeline(df)
result.head()
```

|    | geocodigo   |   anio | indicador            |   valor |
|---:|:------------|-------:|:---------------------|--------:|
|  0 | ABW         |   1990 | expectativa_al_nacer |   73.08 |
|  1 | ABW         |   1991 | expectativa_al_nacer |   73.1  |
|  2 | ABW         |   1992 | expectativa_al_nacer |   73.18 |
|  3 | ABW         |   1993 | expectativa_al_nacer |   73.23 |
|  4 | ABW         |   1994 | expectativa_al_nacer |   73.27 |

Luego, se exportar el `transformador` listo para ser re-aplicado.

```python
exportar_transformador('CRECIM_g02', pipeline, callstack)
```

Ejecutando lo anterior, obtenemos un archivo `CRECIM_g02_transformer.py` (que se muestra a continuación), contiene tres secciones:

- Definiciones (en código fuente) de las transformaciones utilizadas.
- Declaración de la pipeline con los valores pre-interpretados.
- Log de la ejecución original: Contiene la llamada a la función original, con sus parámetros interpretados explícitamente; información general del DataFrame ([`pandas.DataFrame.info`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.info.html)) y una vista del DataFrame como Markdown ([`pandas.DataFrame.to_markdown`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_markdown.html)).

Este archivo implica una auto-replicación casi _standalone_ (salvo los imports) del código fuente original, por lo que en una pipeline de normalización de datos, este script queda listo para reutilizarse.

[(ir al final)](#especificación-de-chain-y-transformer)

```python
from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='pais_nombre', axis=1),
	drop_col(col='continente_fundar', axis=1),
	drop_col(col='nivel_agregacion', axis=1),
	wide_to_long(primary_keys=['iso3', 'anio'], value_name='valor', var_name='indicador'),
	rename_cols(map={'iso3': 'geocodigo'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 5911 entries, 0 to 5910
#  Data columns (total 7 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   iso3                  5911 non-null   object 
#   1   pais_nombre           5911 non-null   object 
#   2   continente_fundar     5911 non-null   object 
#   3   anio                  5911 non-null   int64  
#   4   expectativa_al_nacer  5911 non-null   float64
#   5   pib_pc                5911 non-null   float64
#   6   nivel_agregacion      5911 non-null   object 
#  
#  |    | iso3   | pais_nombre   | continente_fundar                      |   anio |   expectativa_al_nacer |   pib_pc | nivel_agregacion   |
#  |---:|:-------|:--------------|:---------------------------------------|-------:|-----------------------:|---------:|:-------------------|
#  |  0 | ABW    | Aruba         | América del Norte, Central y el Caribe |   1990 |                  73.08 |  30823.5 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  RangeIndex: 5911 entries, 0 to 5910
#  Data columns (total 6 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   iso3                  5911 non-null   object 
#   1   continente_fundar     5911 non-null   object 
#   2   anio                  5911 non-null   int64  
#   3   expectativa_al_nacer  5911 non-null   float64
#   4   pib_pc                5911 non-null   float64
#   5   nivel_agregacion      5911 non-null   object 
#  
#  |    | iso3   | continente_fundar                      |   anio |   expectativa_al_nacer |   pib_pc | nivel_agregacion   |
#  |---:|:-------|:---------------------------------------|-------:|-----------------------:|---------:|:-------------------|
#  |  0 | ABW    | América del Norte, Central y el Caribe |   1990 |                  73.08 |  30823.5 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='continente_fundar', axis=1)
#  RangeIndex: 5911 entries, 0 to 5910
#  Data columns (total 5 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   iso3                  5911 non-null   object 
#   1   anio                  5911 non-null   int64  
#   2   expectativa_al_nacer  5911 non-null   float64
#   3   pib_pc                5911 non-null   float64
#   4   nivel_agregacion      5911 non-null   object 
#  
#  |    | iso3   |   anio |   expectativa_al_nacer |   pib_pc | nivel_agregacion   |
#  |---:|:-------|-------:|-----------------------:|---------:|:-------------------|
#  |  0 | ABW    |   1990 |                  73.08 |  30823.5 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='nivel_agregacion', axis=1)
#  RangeIndex: 5911 entries, 0 to 5910
#  Data columns (total 4 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   iso3                  5911 non-null   object 
#   1   anio                  5911 non-null   int64  
#   2   expectativa_al_nacer  5911 non-null   float64
#   3   pib_pc                5911 non-null   float64
#  
#  |    | iso3   |   anio |   expectativa_al_nacer |   pib_pc |
#  |---:|:-------|-------:|-----------------------:|---------:|
#  |  0 | ABW    |   1990 |                  73.08 |  30823.5 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['iso3', 'anio'], value_name='valor', var_name='indicador')
#  RangeIndex: 11822 entries, 0 to 11821
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3       11822 non-null  object 
#   1   anio       11822 non-null  int64  
#   2   indicador  11822 non-null  object 
#   3   valor      11822 non-null  float64
#  
#  |    | iso3   |   anio | indicador            |   valor |
#  |---:|:-------|-------:|:---------------------|--------:|
#  |  0 | ABW    |   1990 | expectativa_al_nacer |   73.08 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  RangeIndex: 11822 entries, 0 to 11821
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  11822 non-null  object 
#   1   anio       11822 non-null  int64  
#   2   indicador  11822 non-null  object 
#   3   valor      11822 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador            |   valor |
#  |---:|:------------|-------:|:---------------------|--------:|
#  |  0 | ABW         |   1990 | expectativa_al_nacer |   73.08 |
#  
#  ------------------------------
#  
```

# Especificación de `chain` y `transformer`

Las clases principales de la biblioteca son `chain` y `transformer`.

Un `transformer` es un objeto aplicable, similar a una función, que almacena información sobre su código fuente (como `string`) y, de tenerlos, sobre sus parámetros parcialmente aplicados. Como la implementación de este tipo está hecha específicamente para ser usada con `chain`, asume que se lo llamará con un sólo parámetro de tipo `DataFrame`, por lo que los parámetros parcialmente aplicados es todo lo que no sea el `DataFrame` al que se le aplique la función.

Se puede construir un `transformer` para ser usado, por ejemplo, de la siguiente manera:

```python
foo = lambda a: transformer(lambda b: a+b, name='foo')
```

Luego, una `chain` es simplemente una colección de `transformer`s, que los aplica secuencialmente (en orden de declaración) a un `DataFrame`, almacenando información sobre la ejecución en un _"call stack"_. 

Podríamos escribir, por ejemplo:

```python
pipeline = chain(foo(1))
```

Nótese que no aplicar el parámetro de `foo` fallaría, pues `foo` es una _lambda_ y no un `transformer`. `chain` type-chequea los elementos de su colección, sólo acepta objetos del tipo `transformer`. Por eso la escritura de `foo` es una _lambda_ que devuelve un `transformer` al que **le decimos** que tiene de nombre _"foo"_.

Quizá, entonces, la mejor manera de crear un `transformer` para que esté conforme a las necesidades de `chain` es a través del decorador de utilidad `transformer.convert(f)`.

`transformer.convert` es un decorador de funciones que envuelve una función y la convierte en una lambda válida para una `chain`, devolviendo el tipo correcto. Cuando se ejecuta una `chain` aplica un sólo parámetro (el `DataFrame` _target_) a la primer función de la cadena, que luego se va encadenando con el resto. (Similar a la composición de funciones, los tipos de entrada/salida de las funciones de la cadena tienen que coincidir.)

### Conversión de funciones de un parámetro

Nótese que no podemos tener funciones que no toman ningún parámetro, pues tienen que poder ser _aplicadas_ a un `DataFrame`, por ende tener un sólo parámetro es el caso más chico.

```python
@transformer.convert
def foo(df: pd.DataFrame):
    print(df)
```

Esta conversión es equivalente a la siguiente expresión:

```python
foo = transformer(lambda df: print(df), name='foo')
```

### Conversión de funciones de más parámetros

```python
@transformer.convert
def bar(y: str, df, x: int, z=[1,2,3]):
    print(x,y,z)
    return df
```

En éste caso se puede apreciar mejor la conversión. La función definida es equivalente a la siguiente expresión:

```python

def _inner_bar(df):
    print(df)
    return df

bar = lambda y, x, z=[1,2,3]: transformer(_inner_bar, name='bar')
```

Es decir, todos los parámetros que no sean **literalmente** `df` se esperan _"afuera"_ para ser pre-inicializados. Nótese que si no se inicializan los parámetros, entonces `bar` es de tipo _función_ y no tipará con la `chain`.

### Construcción de `chain`s

Veamos algunos ejemplos usando las funciones previamente definidas:

```python
pipeline = chain(
    foo,
    bar(x=3, y='a')
)

callstack, result = pipeline(1)

# >> 1
# >> 3
```

Esta es una aplicación correcta que, de hecho, es equivalente a:

```python
pipeline = chain(
    foo,
    bar('a', 3)
)

callstack, result = pipeline(1)

# >> 1
# >> 3
```

Sin embargo, aplicar los parámetros de esta manera:

```python
pipeline = chain(
    foo,
    bar(3, y='a')
)

callstack, result = pipeline(1)
```

Genera un error, pues `bar(3, y='a')` es posicionalmente equivalente a `bar(y=3, y='a')`, no sólo `x` no queda definido, sino que hay dos valores posibles para `y`, por lo que este programa falla.

### Especificación del _call-stack_

El _call stack_ resultante de la ejecución de una `chain` sobre un `DataFrame` es, esencialmente una lista de tuplas, cada tupla $t$ contiene (en orden):

- ($t_0$) - El puntero a la función original.
- ($t_1$) - Un diccionario con los parámetros aplicados.
- ($t_2$) - El resultado de haber aplicado el resultado anterior a la función ($t_0$) con los parámetros ($t_1$).

El primer elemento del _call stack_ siempre es una ejecución especial, contiene la una función _dummy_ `start`, que no hace nada, y contiene el `DataFrame` como se lo pasó originalmente.
