import json
import tweepy
import pika
import sys
import os

# Credenciales
api_key = "XXXXXXXX"
api_secret_key = "XXXXXXXX"
access_token = "XXXXXXXX"
access_token_secret = "XXXXXXXX"

# Entrada de las credenciales y conexión a la API
auth = tweepy.OAuthHandler(api_key, api_secret_key)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# Keywords
keywords = ["#zbe","zbe barcelona","#zbebarcelona","zona baixes emissions","zona bajas emisiones","#zonabajasemisiones"]

try:
    # Conexión servidor RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',heartbeat=2000,blocked_connection_timeout=2500))
    channel = connection.channel()

    # Creación cola
    channel.queue_declare(queue='XXXXXXXX')

except Exception as err:
    print(err)


# Tweepy listener para procesar tweets en tiempo real
class tweets_st(tweepy.Stream):
    # def __init__(self, api):
    #
    #     # initialise the Twitter stream
    #     self.api = api
    #     super(tweepy.Stream, self).__init__()

#Definición de on_data, error handling en caso de errores, timeout, desconexión, etc.
    def on_data(self, tweet):
        try:

            #Enviar tweets a la cola de mensajes de RabbitMQ
            channel.basic_publish(exchange='',
                                  routing_key='XXXXXXXX',
                                  body=tweet
                                  )
            print(tweet)

        except Exception as err:
            print(err)

    def on_error(self, status_code):
        print('Hay un error. Status = %s' % status_code)
        return True

    def on_timeout(self):
        print('Standby...')
        return True

    def on_disconnect(self, notice):
        print('Desconectado: %s' % notice)
        return False

    def on_connection_error(self):
        self.disconnect()

#Stream
#tw_streamer = tweepy.Stream(auth, tweets_st(api))

tw_streamer = tweets_st(
    api_key,api_secret_key,
    access_token, access_token_secret
)

#Filtros aplicados
tw_streamer.filter(track=keywords, stall_warnings=True)
