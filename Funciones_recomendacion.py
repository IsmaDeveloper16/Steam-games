""" 
    Aqui estare pegando las funciones que hice en el ipynb, el objetivo de esto es eficientizar el uso de las funciones
    y sistemas de recomendacion 
"""
import pandas as pd
import pyarrow.parquet as pq

def developer(dev,games,items):
    #Eliminamos las filas vacias en price y id
    games.dropna(subset=['price','id'],inplace=True)
    
    #Cambiamos los tipos de datos de games['id'] y items['id']
    games['id'] = games['id'].astype(int)
    items['id'] = items['id'].astype(int)

    #Filtramos games por el desarrolador seleccionado y reseteamos el indice
    games_developer_select = games[games['developer'] == str(dev).capitalize()].reset_index()

    #seleccionamos las columnas con las que trabajaremos
    games_dev_all = games_developer_select[['release_date','id']]

    #hacemos un primer merge para unir los juegos con los items
    merge = pd.merge(items[['id','user_id']],games_dev_all,on='id')

    #hacemos un conteo de items por año, reseteamos el indice y renombramos la columna
    recuento = merge.groupby('release_date').agg({'user_id':'count'}).reset_index()
    recuento.rename({'user_id':'cantidad_items'},axis=1,inplace=True)

    #Ahora filtramos los juegos gratis
    games_dev_free = games_developer_select[games_developer_select['price'] == '0']

    #ahora hacemos un merge con los juegos gratis y items, tambien hacemos un groupby con un recuento de items gratis y renombramos una columna
    merge_2 = pd.merge(games_dev_free[['release_date','id']],items[['id','user_id']],on='id')
    recuento_2 = merge_2.groupby('release_date').agg({'user_id':'count'}).reset_index()
    recuento_2.rename({'user_id':'cantidad_items_free'},axis=1,inplace=True)

    
    #creamos un tercer merge con los 2 recuentos
    merge_3 = recuento.merge(recuento_2,on='release_date',how='left').fillna(0)

    #calculamos y pasamos a porcentaje la columna calculada
    merge_3['contenido_Free'] = (recuento_2['cantidad_items_free'] / recuento['cantidad_items'] * 100).fillna(0)
    merge_3['contenido_Free'] = merge_3['contenido_Free'].apply(lambda x: f'{x}%')

    #por ultimo eliminamos la columna cantidad_items_free y renombramos release_date
    merge_3.drop('cantidad_items_free',axis=1,inplace=True)
    merge_3.rename({'release_date':'año'},axis=1,inplace=True)

    return merge_3.to_dict(orient='records')



def userdata(user,games,items,reviews):

    #realizamos un filtro con el usuario ingresado
    user = items[items['user_id'] == str(user)].reset_index()

    #hacemos un merge para obtener datos de games y cambiamos el tipo de dato de la columna price
    merge = user.merge(games[['id','price']])
    merge['price'] = merge['price'].astype(float)

    #realizamos un groupby de el usuario, un recuento con el id, una suma de price y un reseteo del indice
    merge_1 = merge.groupby('user_id').agg({'id':'count','price':'sum'}).reset_index()

    #realizamos un merge de los datos del usuario filtrado con reviews y eliminamos duplicados
    user_reviews = user.merge(reviews[['user_id','recommend']])
    user_reviews.drop_duplicates(subset='id',inplace=True)

    #realizamos un groupby para realizar un recuento de reviews y reseteamos el indice
    Cantidad_reviews = user_reviews.groupby('user_id').agg({'id':'count'}).reset_index()

    #en 2 variables guardamos la cantidad de reviews que hizo el usuario y la cantidad de reviews recomendadas
    cantidad = Cantidad_reviews['id'][0]
    recomendados = user_reviews['recommend'].sum()

    #retornamos
    return (f"Usuario: {merge_1['user_id'][0]}, dinero gastado: {merge_1['price'][0]}$, porcentaje de recomendacion: {(recomendados//cantidad)*100}%, cantidad de items: {merge_1['id'][0]}")



def UserForGenre(genre,games,items):
    #eliminamos los datos nulos en la columna genres
    games.dropna(subset='genres',inplace=True)

    #aplicamos un filtro con el genero ingresado
    juegos_genero_steam = games[games['genres'].str.lower().str.contains(genre)]
    juegos_genero_items = items[items['game'].isin(juegos_genero_steam['game'])]

    #hacemos un merge
    df_combinado = pd.merge(juegos_genero_items, juegos_genero_steam[['game', 'release_date']], on='game')

    #hacemos un groupby de los diferentes usuarios, hacemos una suma de sus horas jugadas y reseteamos el indice
    playtime_user = df_combinado.groupby('user_id').agg({'playtime_forever':'sum'}).reset_index()

    #obtenemos el usuario con mas horas jugadas de ese genero
    user = playtime_user[playtime_user['playtime_forever'] == (playtime_user['playtime_forever'].max())]
    user = user['user_id'].values[0]

    #filtramos ese usuario, hacemos un groupby con cada año y sus horas jugadas
    data_user = df_combinado[df_combinado['user_id'] == user]
    playtime_year = data_user.groupby('release_date').agg({'playtime_forever':'sum'}).reset_index()

    #hacemos una lista y un for para ir cargando los diccionarios a nuestra lista
    lista = []
    for i in range(len(playtime_year['release_date'].values)):
        dic = {'año':playtime_year['release_date'].values[i], 'horas':playtime_year['playtime_forever'].values[i]}
        lista.append(dic)

    #retornamos los resultados
    return (f"El usuario con mas horas jugadas para el genero:{genre} es:{user}, horas jugadas: {lista}")



def best_developer_year(year,games,reviews):
    #filtramos los datos de games por año
    games_year = games[games['release_date'] == int(year)]

    #hacemos un condicional para que si no hay datos vacios le devuelva un mensaje al usuario
    # y si hay datos guarde el merge en una variable
    if games_year.merge(reviews,on='id').empty:
        return ("No hay registros de este año, ingrese otro año")
    games_year_reviews = games_year.merge(reviews,on='id')

    #hacemo un filtro para obtener los recomendados y los de reviews positivos
    games_year_reviews[(games_year_reviews['recommend'] == 1) & (games_year_reviews['sentiment analysis'] == 2)]

    #hacemos una agrupacion de los desarrolladores y realizamos 
    dev_recommend = games_year_reviews.groupby('developer').agg({'recommend':'count','sentiment analysis':'count'}).reset_index()

    #realizamos una suma de los conteos y guardamos en una nueva columna
    dev_recommend['suma_recommend'] = dev_recommend['recommend'] + dev_recommend['sentiment analysis']

    #obtenemos los indices
    indices = dev_recommend['suma_recommend'].nlargest(3).index

    return(f"Puesto 1: {dev_recommend['developer'][indices[0]]}, Puesto 2: {dev_recommend['developer'][indices[1]]}, Puesto 3: {dev_recommend['developer'][indices[2]]}")


def developer_2(dev,games,reviews):
    #aplicamos un filtro para tener solo el desarollador ingresado
    games_dev = games[games['developer'] == str(dev).capitalize()]

    #hacemos una condicional para indicar si el desarrollador ingresado es valido o si hay datos
    if games_dev.empty:
        return("no se encuentran datos de el desarrolador ingresado, intente otro")

    #realizamos un merge, hacemos un groupby con el recuento de los analisys y reseteamos el indice
    merge = games_dev.merge(reviews,on='id')
    merge = merge.groupby('sentiment analysis').agg({'developer':'count'}).reset_index()
    #nos quedamos con las filas que no sean neutras
    merge = merge[merge['sentiment analysis'] != 1]
    #retornamos el diccionario
    return {dev:list(merge['developer'].values)}



""" 
    SISTEMAS DE RECOMENDACION:
"""


def item_item(id,games_model):
    #hacemos un copia de nuestro dataframe games_model y elimninamos las columnas id y game
    games_copy = games_model.copy()
    games_copy.drop(['game','id'],axis=1,inplace=True)

    #importamos el cosine_similarity 
    from sklearn.metrics.pairwise import cosine_similarity

    #reemplazamos los datos de games_copy con los resultados de la funcion cosine_similarity y convertimos en un dataframe
    games_copy = cosine_similarity(games_copy)
    games_copy = pd.DataFrame(games_copy)

    #obtenemos el indice del id del juego ingresado
    index = games_model[games_model['id'] == id].index[0]

    #obtenemos los indices de las mejores recomendaciones
    recommend = games_copy[index].sort_values(ascending=False).iloc[1:6].index

    #localizamos las recomendaciones y obtenemos el id y el nombre
    recommend = games_model.loc[recommend, ['id','game']].values

    return(f'estos son los juegos recomendados con sus ids y nombres: 1 id:{int(recommend[0][0])}  name: {recommend[0][1]}, 2 id:{int(recommend[1][0])} name: {recommend[1][1]}, 3 id:{int(recommend[2][0])} name: {recommend[2][1]}, 4 id:{int(recommend[3][0])} name: {recommend[3][1]}, 5 id:{int(recommend[4][0])} name: {recommend[4][1]}.')



def user_item(user,items,games_model):
    # filtramos los usuarios para traer todos los juegos jugados por el.
    items = items[items['user_id'] == str(user)]
    #traeré el id del juego mas jugado por ese usuario.
    max = items['playtime_forever'].idxmax()
    id_game = items['id'].iloc[max]
    #Usaré el id del juego y lo metemos a la función que habia creado antes para encontrar los 5 juegos recomendados
    return item_item(int(id_game),games_model)

