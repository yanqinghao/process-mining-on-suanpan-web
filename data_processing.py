import redis
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

    def data_collector(self):
        pass

    def loop(self):
        while self.STARTED:
            self.data_collector()
            self.sio.emit("data.replay", {})
