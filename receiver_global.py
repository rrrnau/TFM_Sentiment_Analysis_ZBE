import pika
import psycopg2
import json

#Conexión RabbitMQ
try:

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # Create a queue called <your_queue_name>
    channel.queue_declare(queue='XXXXXXXX')

except Exception as err:
    print(err)

#Conexión Postgresql
try:
#Conexión
    con = psycopg2.connect(
        host="localhost",
        database="Twitter",
        user="XXXXXXXX",
        password="XXXXXXXX")
except Exception as err:
    print(err)


# Función para insertar datos en la bbdd PostgreSQL
def callback(ch, method, properties, body):

    try:

# Inicio cursor
        cur = con.cursor()

#Manipulación tweet
        tweet = (body.decode(),)

# Ejecución insert a la tabla de destino
        cur.execute('insert into "Twitter"."TW_brutos" (tweets) values (%s)', tweet) 
        con.commit()
        cur.close()
        print('Success')

    except Exception as err:
        print(err)


channel.basic_consume(queue='tw_test', on_message_callback=callback, auto_ack=True)

# Indicador OK
print(' [*] Esperando mensajes...')

# Consumo mensajes de RabbitMQ
channel.start_consuming()

#Cerrar conexión
con.close()