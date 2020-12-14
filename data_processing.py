# coding=utf-8

import redis
import time
import urllib
import gevent
import socketio
import geventwebsocket.handler
from suanpan import g
from suanpan.api.app import getAppGraph
from suanpan.utils import json

REDIS_HOST = f"app-{g.appId}-redis"


class WebSocketHandler(geventwebsocket.handler.WebSocketHandler):
    def get_environ(self):
        env = super(WebSocketHandler, self).get_environ()
        urlpath = self.path.split("?", 1)[0] if "?" in self.path else self.path
        env["PATH_INFO"] = urllib.parse.unquote(urlpath)
        return env


class DataProcessing(object):
    STARTED = False

    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=6379,
            decode_responses=True,
            socket_keepalive=True,
            socket_connect_timeout=1,
        )
        self.graph = getAppGraph(g.appId)["connections"]
        self.nodes = list(getAppGraph(54032)["processes"].keys())
        self.sio = socketio.Server(async_mode="gevent", cors_allowed_origins="*", json=json)
        self.sio.on("start.replay", handler=self.start)
        self.sio.on("stop.replay", handler=self.stop)
        self.app = socketio.WSGIApp(self.sio)
        gevent.pywsgi.WSGIServer(("", 8888), self.app,
                                 handler_class=WebSocketHandler).serve_forever()

    def start(self):
        self.STARTED = True

    def stop(self):
        self.STARTED = False

    def timestamp(self):
        return int(time.time() * 1000)

    def data_collector(self):
        time_now = self.timestamp()
        time_left = time_now - 1000
        master_messages = self.redis_client.xrange("mq-master", str(time_left), str(time_now))
        node_messages = {}
        for nodeid in self.nodes:
            node_messages[nodeid] = self.redis_client.xrange(f"mq-{nodeid}", str(time_left),
                                                             str(time_now))

    def replay_process(self):
        pass

    def loop(self):
        while True:
            if self.STARTED:
                self.data_collector()
                self.sio.emit("data.replay", {})
                time.sleep(1)
