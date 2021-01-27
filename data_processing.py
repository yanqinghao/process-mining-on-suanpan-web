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
from suanpan import asyncio
from suanpan.log import logger
from utils import replay, get_data, statistics

REDIS_HOST = f"app-{g.appId}-redis"


class WebSocketHandler(geventwebsocket.handler.WebSocketHandler):

    def get_environ(self):
        env = super(WebSocketHandler, self).get_environ()
        urlpath = self.path.split("?", 1)[0] if "?" in self.path else self.path
        env["PATH_INFO"] = urllib.parse.unquote(urlpath)
        return env


class DataProcessing(object):
    REPLAY_STARTED = False
    TIME_INTERVAL = 1
    TIMEOUT = 1

    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=6379,
            decode_responses=True,
            socket_keepalive=True,
            socket_connect_timeout=1,
        )
        connections = getAppGraph(g.appId)["connections"]
        self.graph = {}
        for connection in connections:
            src = connection["src"]["process"] + "_" + connection["src"]["port"]
            tgt = connection["tgt"]["process"] + "_" + connection["tgt"]["port"]
            if src not in self.graph:
                self.graph.update({src: [tgt]})
            else:
                self.graph[src].append(tgt)
        self.nodes = list(getAppGraph(g.appId)["processes"].keys())
        self.remains = {}
        asyncio.wait(asyncio.run([self.init_sio_server, self.replay_loop]))

    def init_sio_server(self):
        logger.info("Start to initial socket io server...")
        self.sio = socketio.Server(async_mode="gevent", cors_allowed_origins="*", json=json)
        self.sio.on("connect", handler=self.connect)
        self.sio.on("control.replay", handler=self.set_replay_started)
        self.sio.on("query.replay.status", handler=self.get_replay_started)
        self.sio.on("count.errors", handler=self.node_error)
        self.sio.on("count.nodes", handler=self.node_count)
        self.sio.on("count.edges", handler=self.edge_count)
        self.sio.on("cost.nodes", handler=self.node_cost)
        self.sio.on("cost.edges", handler=self.edge_cost)
        self.app = socketio.WSGIApp(self.sio, socketio_path=f"socket/{g.appId}/pstream")
        gevent.pywsgi.WSGIServer(("", 8888), self.app,
                                 handler_class=WebSocketHandler).serve_forever()

    def connect(self, sid, data):
        logger.info(f"Connect with {sid}, get data: {data}")

    def node_error(self, sid, data):
        logger.info("Collect data and get errors on each node...")
        time_interval = data.get("time_interval")
        master_messages, node_messages = get_data.collector(self.redis_client, time_interval,
                                                            self.nodes)
        return statistics.count_errors(master_messages)

    def node_count(self, sid, data):
        logger.info("Collect data and count messages on each node...")
        time_interval = data.get("time_interval")
        master_messages, node_messages = get_data.collector(self.redis_client, time_interval,
                                                            self.nodes)
        return statistics.count_nodes(node_messages)

    def node_cost(self, sid, data):
        logger.info("Collect data and calculate messages cost on each node...")
        time_interval = data.get("time_interval")
        master_messages, node_messages = get_data.collector(self.redis_client, time_interval,
                                                            self.nodes)
        return statistics.time_cost_nodes(self.nodes, master_messages, node_messages)

    def edge_count(self, sid, data):
        logger.info("Collect data and count messages on each edge...")
        time_interval = data.get("time_interval")
        master_messages, node_messages = get_data.collector(self.redis_client, time_interval,
                                                            self.nodes)
        return statistics.count_edges(self.graph, node_messages)

    def edge_cost(self, sid, data):
        logger.info("Collect data and calculate messages cost on each edge...")
        time_interval = data.get("time_interval")
        master_messages, node_messages = get_data.collector(self.redis_client, time_interval,
                                                            self.nodes)
        return statistics.time_cost_edges(self.graph, master_messages, node_messages)

    def set_time_interval(self, sid, data):
        interval = data["interval"]
        logger.info(f"Set time interval to {interval}...")
        self.TIME_INTERVAL = interval

    def set_replay_started(self, sid, data):
        log = "Start to send replay data..." if data["start"] else "Stop to send replay data..."
        logger.info(log)
        self.REPLAY_STARTED = data["start"]
        self.sio.emit("listen.replay.status", {"success": True, "data": self.REPLAY_STARTED})
        return {"success": True, "data": data}

    def get_replay_started(self, sid):
        logger.info("Query replay status...")
        return {"success": True, "data": self.REPLAY_STARTED}

    def replay_loop(self):
        while True:
            if self.REPLAY_STARTED:
                logger.info("Collect data and send to clients...")
                master_messages, node_messages = get_data.collector(self.redis_client,
                                                                    self.TIME_INTERVAL, self.nodes)
                processed_data = replay.data_processor(master_messages, node_messages,
                                                       self.remains, self.graph, self.TIMEOUT)
                if processed_data:
                    self.sio.emit("data.replay", processed_data)
                time.sleep(self.TIME_INTERVAL)
            asyncio.sleep(0.01)
