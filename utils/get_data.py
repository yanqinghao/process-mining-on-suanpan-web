# coding=utf-8

import time


def timestamp():
    return int(time.time() * 1000)


def collector(redis_client, time_interval, nodes):
    if time_interval:
        time_now = timestamp()
        time_left = time_now - time_interval * 1000
    else:
        time_now = "+"
        time_left = "-"
    master_messages = redis_client.xrange("mq-master", str(time_left), str(time_now))
    node_messages = {}
    for nodeid in nodes:
        node_messages[nodeid] = redis_client.xrange(f"mq-{nodeid}", str(time_left), str(time_now))
    return master_messages, node_messages
