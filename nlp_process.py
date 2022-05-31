import psycopg2
import io
import json
import pandas as pd
from psycopg2 import OperationalError, errorcodes, errors
import psycopg2.extras as extras
import sys
import re
import os
import emoji
import nltk
from nltk.util import ngrams
import collections
from nltk.corpus import stopwords
from nltk import tokenize
import googletrans
from googletrans import Translator
import pysentimiento
from pysentimiento import create_analyzer
import logging
import sklearn
from sklearn.feature_extraction.text import CountVectorizer
logging.disable(logging.ERROR)

# loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
# print(loggers)


# Definición detalles de conexión
con_det = {"host" : "localhost",
           "port":2222,
            "database" : "Twitter",
            "user" : "XXXXXXXX",
            "password" : "XXXXXXXX"}

# Definición conexión y error handling
def connect(con_par):
    conn = None
    #Try except para conectar a Postgres
    try:
        print('Conectando...')
        conn = psycopg2.connect(**con_par)
        print('Conectado a Postgres')

    except OperationalError as err:
        # Enseñar error
        show_psycopg2_exception(err)
        # Abortar la conexión definiendo la conexión como nada
        conn = None
    return conn

conn = connect(con_det)

#Inicio cursor
cur = conn.cursor()

#Ejecución select
cur.execute('''select ids, tweets from "Twitter"."TW_bruto"''')

#Selección de todos los registros
reg = cur.fetchall()

#Creación dataframe vacío para insertar valores posteriormente
df = []


def give_emoji_free_text(text):
    return emoji.get_emoji_regexp().sub(r'', text.decode('utf8'))

for r in reg:

    jsonobj = json.loads(r[1])
    #jsonpd = pd.read_json(r[1])
    jsonpd = pd.json_normalize(jsonobj,max_level=1)
    df_pd = pd.DataFrame(jsonpd)
    df.append(df_pd)

df = pd.concat(df)

#Se seleccionan las columnas que interesan
dff = df[["id","created_at","text","quote_count","reply_count","retweet_count","lang","user.id","user.name","user.screen_name"
           ,"user.location","user.followers_count","user.created_at","quoted_status.created_at","quoted_status.id","geo","coordinates","place"]] #,"place.name","place.country"]]


#Se reemplazan los . por _ para poder leer mejor y se convierte el campo "text" a string. De no ser así da problemas.
dff.columns = dff.columns.str.replace('.','_')
dff["text"] = dff["text"].astype(str)

#Eliminar retweets y filtrar por los tweets en catalán y castellano.
#Así se reduce el número de registros antes de aplicar otras funciones.

idiomas = ['ca','es']
dff = dff.loc[(~dff["text"].str.contains('RT')) & (dff["lang"].isin(idiomas))]

#Función para limpiar tweets. Se modifican caracteres y/o palabras pero no se eliminan registros.
def cleantext(tweets):
    tweets = re.sub(r'@[A-Za-z0-9]+', '', tweets) #Se eliminan menciones @
    tweets = re.sub(r'#', '', tweets) #Se elimina el símbolo hashtag #
    tweets = re.sub(r'https?:\/\/\S+', '', tweets) #Se eliminan los links http o https
    tweets = tweets.lower() #Se transforma a lowercase
    tweets = emoji.replace_emoji(tweets, replace='') #Se excluyen emojis

    return tweets

dff["text"] = dff["text"].apply(cleantext)


# path = r"PathPC/csvs/output_n2.csv"
# dff.to_csv(path, index = False, header = True)

# Subset catalán
dff_cat = dff[(dff.lang == "ca")]

# Lista stopwords en catalán (fuente: Kaggle).
sw_cat = open("/PathPC/csvs/stopwords_cat.txt").read().splitlines()

# Aplicación stopwords en catalán
dff_cat["text"] = dff_cat["text"].apply(lambda x: ' '.join([word for word in x.split() if word not in (sw_cat)]))
# path = r"PathPC/csvs/dff_cat.csv"
# dff_cat.to_csv(path, index = False, header = True)

# Traducción tweets cat -- es
translator = Translator()
dff_cat["text"] = dff_cat["text"].apply(translator.translate, src='ca',dest='es').apply(getattr, args=('text',))
#dff_cat['text'] = dff_cat['text'].apply(lambda x: translator.translate(x, src='ca',dest='es'))

# Mismo proceso stopwords para los tweets en castellano.
dff_es = dff[(dff.lang == "es")]

dff_conc = pd.concat([dff_cat,dff_es], axis=0)
sw_conc = open("/PathPC/csvs/stopwords_es.txt").read().splitlines()
dff_conc["text"] = dff_conc["text"].apply(lambda x: ' '.join([word for word in x.split() if word not in (sw_conc)]))


# Se "tokenizan" los textos.
dff_conc["text_tokens"] = dff_conc.apply(lambda row: nltk.word_tokenize(row["text"]), axis=1)


#N-GRAMAS (deshabilitar si se ejecuta polaridad)
def get_ngrams(text, ngram_from=1, ngram_to=3, n=None, max_features=20000):
    vec = CountVectorizer(ngram_range=(ngram_from, ngram_to),
                          max_features=max_features).fit(text)
    bag_of_words = vec.transform(text)
    sum_words = bag_of_words.sum(axis=0)
    words_freq = [(word, sum_words[0, i]) for word, i in vec.vocabulary_.items()]
    words_freq = sorted(words_freq, key=lambda x: x[1], reverse=True)

    return words_freq[:n]

unigrams = get_ngrams(dff_conc["text"], ngram_from=1, ngram_to=3, n=10000)
unigrams_df = pd.DataFrame(unigrams)
unigrams_df.columns=["texto", "frecuencia"]
print(unigrams_df.head())


#path = r"/pathPC/csvs/top_ngrams.csv"
#unigrams_df.to_csv(path, index = False, header = True)


#POLARIDAD
def polaridad(textos):
    analyzer = create_analyzer(task="sentiment", lang="es")
    texts = analyzer.predict(textos)
    salida = texts.output

    return salida

dff_conc["polaridad"] = dff_conc["text"].apply(polaridad)


def emocion(textos):
    em_analyzer = create_analyzer(task="emotion", lang="es")
    ems = em_analyzer.predict(textos)
    salida = ems.output

    return salida

dff_conc["emocion"] = dff_conc["text"].apply(emocion)

print(dff_conc["emocion"])


#Commit
conn.commit()

#Cerrar cursor
cur.close()


# Error handling de psycopg2 (built-in)
def show_psycopg2_exception(err):
    #Detalles de la excepción
    err_type, err_obj, traceback = sys.exc_info()
    #Trazabilidad a nivel de línea del error
    line_n = traceback.tb_lineno
    # print the connect() error
    print("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print("psycopg2 traceback:", traceback, "-- type:", err_type)
    # psycopg2 extensions.Diagnostics object attribute
    print("\nextensions.Diagnostics:", err.diag)
    # print the pgcode and pgerror exceptions
    print("pgerror:", err.pgerror)
    print("pgcode:", err.pgcode, "\n")  # Define a connect function for PostgreSQL database server


#Definición del data dump de DF a Postgres
def execute_values(conn, dtfr, table):
    # Crear una lista de tuplas a partir del dataframe definido anteriormente
    tpls = [tuple(x) for x in dtfr.to_numpy()]

    # Definición de las columnas que definirán la línea anterior
    cols = ','.join(list(dtfr.columns))

    # Insert SQL
    sql = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, sql, tpls)
        conn.commit()
        print('Datos insertados correctamente')
    except (Exception, psycopg2.DatabaseError) as err:
        show_psycopg2_exception(err)
        cursor.close()


conn.autocommit = True

#Ejecución del insert
execute_values(conn,dff,'"Twitter"."tw_procesados"')

#Cerrar conexión
conn.close()