import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

def _fetch_movie_info(query: str, year: int, movie_or_show: str, api_key: str):
    """
    Realiza uma chamada à API do The Movie Database (TMDb) para obter informações sobre um filme ou programa de TV.

    Parâmetros:
        query (str): O título do filme ou programa de TV a ser pesquisado.
        year (int): O ano de lançamento do filme (apenas para filmes).
        movie_or_show (str): Indica se a pesquisa é para um filme ('MOVIE') ou um programa de TV ('SHOW').
        api_key (str): A chave de API necessária para acessar a API do TMDb.

    Retorna:
        dict: Um dicionário contendo as informações do filme ou programa de TV. As chaves do dicionário são:
              'title' (str), 'year' (str), 'tm_id' (int), 'popularity' (float),
              'vote_average' (float) e 'vote_count' (int). Caso a pesquisa não retorne
              resultados, os valores para 'year', 'tm_id', 'popularity', 'vote_average' e 'vote_count'
              serão None.

    """
    url = f"https://api.themoviedb.org/3/search/{'movie' if movie_or_show == 'MOVIE' else 'tv'}" 
    headers = {
        "accept": "application/json",
        "Authorization": f"{api_key}"
    }

    params = {
        "query": query,
        "include_adult": False,
        "page": 1
    }

    if movie_or_show == "MOVIE":
        params["year"] = year

    response = requests.get(url, headers=headers, params=params)
    response_data = response.json().get('results')

    if response_data:
        result = response_data[0]
        release_date = result.get('release_date') if movie_or_show == "MOVIE" else result.get('first_air_date')
        return {
            'title': query,
            'year': release_date,
            'tm_id': result.get('id'),
            'overview': result.get('overview'),
            'popularity': result.get('popularity'),
            'vote_average': result.get('vote_average'),
            'vote_count': result.get('vote_count')
        }
    else:
        return {
            'title': query,
            'year': None,
            'tm_id': None,
            'overview': None, 
            'popularity': None,
            'vote_average': None,
            'vote_count': None
        }

def search_info(queries: list, years: list, movie_or_show: list, api_key: str = None, return_all = False) -> pd.DataFrame:
    """
    Realiza a busca de informações de filmes ou programas de TV utilizando chamadas paralelas à API do The Movie Database (TMDb).

    Parâmetros:
        queries (list): Uma lista contendo os títulos dos filmes ou programas de TV a serem pesquisados.
        years (list): Uma lista contendo os anos de lançamento correspondentes aos filmes (apenas para filmes).
        movie_or_show (list): Uma lista contendo os tipos de pesquisa, indicando se cada item é para um filme ('MOVIE')
                              ou um programa de TV ('SHOW').
        api_key (str, opcional): A chave de API necessária para acessar a API do TMDb.
        return_all (bool, opcional): Define se todas as informações retornadas pela API serão incluídas no DataFrame
                                     resultante. Por padrão, somente as informações básicas dos filmes ou programas
                                     de TV são retornadas.

    Retorna:
        pandas.DataFrame: Um DataFrame contendo os resultados das chamadas à API. Cada linha do DataFrame representa
                          um filme ou programa de TV e as colunas incluem: 'title' (título), 'year' (ano de lançamento
                          ou estreia), 'tm_id' (ID no TMDb), 'popularity' (popularidade), 'vote_average' (média de votos)
                          e 'vote_count' (total de votos).
    """

    with ThreadPoolExecutor() as executor:
        futures = []
        for query, year, movie_or_show in zip(queries, years, movie_or_show):
            future = executor.submit(_fetch_movie_info, query, year, movie_or_show, api_key)
            futures.append(future)

        results = []
        for future in futures:
            result = future.result()
            results.append(result)

    return pd.DataFrame(results)
