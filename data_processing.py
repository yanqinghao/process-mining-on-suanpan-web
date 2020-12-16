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
                self.graph[src] = self.graph[src].append(tgt)
        self.nodes = list(getAppGraph(g.appId)["processes"].keys())
        self.remains = {}
        self.sio = socketio.Server(async_mode="gevent", cors_allowed_origins="*", json=json)
        self.sio.on("control.replay", handler=self.set_started)
        self.app = socketio.WSGIApp(self.sio)
        gevent.pywsgi.WSGIServer(("", 8888), self.app,
                                 handler_class=WebSocketHandler).serve_forever()

    def set_time_interval(self, sid, data):
        self.TIME_INTERVAL = data["interval"]

    def set_started(self, sid, data):
        self.STARTED = data["start"]

    def timestamp(self):
        return int(time.time() * 1000)

    def data_collector(self):
        time_now = self.timestamp()
        time_left = time_now - self.TIME_INTERVAL * 1000
        master_messages = self.redis_client.xrange("mq-master", str(time_left), str(time_now))
        node_messages = {}
        for nodeid in self.nodes:
            node_messages[nodeid] = self.redis_client.xrange(
                f"mq-{nodeid}", str(time_left), str(time_now)
            )
        return master_messages, node_messages

    def data_preprocessor(self, master_messages, node_messages):
        request_ids = set([i[1]["request_id"] for i in master_messages])
        processed_data = {}
        for request_id, sources in self.remains.items():
            tmp_data = {"messages": []}
            for source in sources:
                if int(time.time() * 1000) - source["node_id"] < self.TIMEOUT * 1000:
                    tgts = self.graph[source["node_id"] + "_" + source["port"]]
                    ins = []
                    for tgt in tgts:
                        node_message = node_messages[tgt.split("_")[0]]
                        ins.append(
                            {
                                "tgt":
                                    tgt,
                                "ins":
                                    [
                                        i for i in node_message if i[1]["id"] == request_id and
                                        tgt.split("_")[1] in i[1].keys()
                                    ]
                            }
                        )
                    if len(ins) > 0:
                        for tgt_info in ins:
                            target = {
                                "node_id": tgt_info["tgt"].split("_")[0],
                                "port": tgt_info["tgt"].split("_")[1],
                                "timestamp": int(tgt_info["ins"][0][0].split("-")[0]),
                                "data": tgt_info["ins"][0][1][tgt.split("_")[1]]
                            }
                            tmp_data["messages"].append({"source": source, "target": target})
                        self.remains[request_id].remove(source)
                else:
                    self.remains[request_id].remove(source)
                processed_data[request_id] = tmp_data

        for request_id in request_ids:
            tmp_data = {"messages": []}
            outs = [i for i in master_messages if i[1]["request_id"] == request_id]
            for out in outs:
                if out[1]["success"] == "true":
                    source = {
                        "node_id": out[1]["node_id"],
                        "port": [key for key in out[1].keys() if "out" in key][0],
                        "timestamp": int(out[0].split("-")[0]),
                        "data": [out[1][key] for key in out[1].keys() if "out" in key][0]
                    }
                    tgts = self.graph[source["node_id"] + "_" + source["port"]]
                    ins = []
                    for tgt in tgts:
                        node_message = node_messages[tgt.split("_")[0]]
                        ins.append(
                            {
                                "tgt":
                                    tgt,
                                "ins":
                                    [
                                        i for i in node_message if i[1]["id"] == request_id and
                                        tgt.split("_")[1] in i[1].keys()
                                    ]
                            }
                        )
                    if len(ins) > 0:
                        for tgt_info in ins:
                            target = {
                                "node_id": tgt_info["tgt"].split("_")[0],
                                "port": tgt_info["tgt"].split("_")[1],
                                "timestamp": int(tgt_info["ins"][0][0].split("-")[0]),
                                "data": tgt_info["ins"][0][1][tgt.split("_")[1]]
                            }
                            tmp_data["messages"].append({"source": source, "target": target})
                    else:
                        if request_id in self.remains:
                            self.remains[request_id].append(source)
                        else:
                            self.remains[request_id] = [source]
            if request_id not in processed_data:
                processed_data[request_id] = tmp_data
            else:
                processed_data[request_id]["messages"].extend(tmp_data["messages"])
        return processed_data

    def replay_process(self):
        pass

    def loop(self):
        while True:
            if self.STARTED:
                master_messages, node_messages = self.data_collector()
                processed_data = self.data_preprocessor(master_messages, node_messages)
                self.sio.emit("data.replay", processed_data)
                time.sleep(self.TIME_INTERVAL)
