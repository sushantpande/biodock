import os
import pika
import redis
from utility import get_env_param 


'''Get RabbitMQ conn'''
def get_amqp_conn():
    host = get_env_param('AMQP_SERVER')
    port = get_env_param('AMQP_PORT')
    user = get_env_param('AMQP_USER')
    pwd = get_env_param('AMQP_PWD')
    credentials = pika.PlainCredentials(user, pwd)
    parameters = pika.ConnectionParameters(host, port, '/', credentials, heartbeat=0)
    conn = pika.BlockingConnection(parameters)
    return conn
    

'''Publish to RabbitMQ'''
def publish_to_amqp(channel, routing_key='', body=''):
    try:
        channel.queue_declare(queue=routing_key)
        channel.basic_publish(exchange='', routing_key=routing_key, body=body)
        return True
    except:
        return False


'''Read from RabbitMQ'''
def consume_from_amqp(channel, queue):
    method_frame, header_frame, body = channel.basic_get(queue=queue, auto_ack=False)
    print (method_frame)
    if method_frame == None or method_frame.NAME == 'Basic.GetEmpty':
        print("Channel Empty.")
        return None
        # We are done, lets break the loop and stop the application.
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    return body.decode("utf=8")


'''Close RabbitMQ channel and connection'''
def close_amqp_channel(channel):
    channel.close()


def close_amqp_conn(conn):
    if not conn.is_closed or not conn.is_closing:
        amqp_conn.close()


'''Get REDIS conn'''
def get_redis_conn():
    conn = None
    host = get_env_param('REDIS_HOST') 
    try:
        conn = redis.Redis(host=host)
    except:
        print ("Failed to open connection to redis server (%s)" %(host))
    return conn


'''Update key on Redis server'''
def update_to_redis(redis_conn, key, val):
    try:
        redis_conn.set(key, val)
        return True
    except:
        return False


'''Get key from Redis server'''
def get_from_redis(redis_conn, key):
    return redis_conn.get(key)
