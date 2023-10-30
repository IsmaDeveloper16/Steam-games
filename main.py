""" Importamos las librerias que estaremos usando para la creacion de la api """
from fastapi import FastAPI
import pandas as pd
from pydantic import BaseModel
import pyarrow.parquet as pq

#usamos la clase Model para instanciar el tipo de datos de las variables
class Model(BaseModel):
    user:str
    genre:str
    year:int
    item:int
    dev:str
    dev_2:str

#   Cargamos los datos para las funciones a las variables
games = pd.read_csv('nueva data/games.csv')
items = pq.read_table('nueva data/items_parquet.parquet').to_pandas()
reviews = pd.read_csv('nueva data/reviews.csv')
games_model = pd.read_csv('nueva data/games_model.csv')

#   Tambien importamos las funciones desde nuestro archivo .py
from Funciones_recomendacion import developer,userdata,UserForGenre,best_developer_year,developer_2,item_item,user_item

#   Creamos el objeto "app"
app = FastAPI()

#Creamos la ruta principal con un diccionario de direcciones para manejarse entre las rutas que haré:
@app.get("/")
def index():
    return None

#Aquí comence las rutas de las Apis:

@app.get("/developer/{dev}")
def dev(dev):
    return developer(dev,games,items)

@app.get("/userdata/{user}")
def user(user):
    return userdata(user,games,items,reviews)

@app.get("/UserForGenre/{genre}")
def genre(genre):
    return UserForGenre(genre,games,items)

@app.get("/Best_developer_year/{year}")
def best_developer(year):
    return best_developer_year(year,games,reviews)

@app.get("/developer_2/{dev_2}")
def dev_2(dev_2):
    return developer_2(dev_2,games,reviews)

@app.get("/item_recomend/{item}")
def item(item:int):
    return item_item(item,games_model)

@app.get("/user_recomend/{user}")
def user(user):
    return user_item(user,items,games_model)