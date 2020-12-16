# coding=utf-8

import time


def data_processor(master_messages, node_messages, remains, graph, timeout):
    request_ids = set([i[1]["request_id"] for i in master_messages])
    processed_data = {}
    for request_id, sources in remains.items():
        tmp_data = {"messages": []}
        for source in sources:
            if int(time.time() * 1000) - source["timestamp"] < timeout * 1000:
                tgts = graph[source["node_id"] + "_" + source["port"]]
                ins = []
                for tgt in tgts:
                    node_message = node_messages[tgt.split("_")[0]]
                    ins.append({
                        "tgt":
                            tgt,
                        "ins": [
                            i for i in node_message
                            if i[1]["id"] == request_id and tgt.split("_")[1] in i[1].keys()
                        ]
                    })
                if len([j for i in ins for j in i["ins"]]) > 0:
                    for tgt_info in ins:
                        target = {
                            "node_id": tgt_info["tgt"].split("_")[0],
                            "port": tgt_info["tgt"].split("_")[1],
                            "timestamp": int(tgt_info["ins"][0][0].split("-")[0]),
                            "data": tgt_info["ins"][0][1][tgt.split("_")[1]]
                        }
                        tmp_data["messages"].append({"source": source, "target": target})
                    remains[request_id].remove(source)
            else:
                remains[request_id].remove(source)
            if tmp_data["messages"]:
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
                tgts = graph[source["node_id"] + "_" + source["port"]]
                ins = []
                for tgt in tgts:
                    node_message = node_messages[tgt.split("_")[0]]
                    ins.append({
                        "tgt":
                            tgt,
                        "ins": [
                            i for i in node_message
                            if i[1]["id"] == request_id and tgt.split("_")[1] in i[1].keys()
                        ]
                    })
                if len([j for i in ins for j in i["ins"]]) > 0:
                    for tgt_info in ins:
                        target = {
                            "node_id": tgt_info["tgt"].split("_")[0],
                            "port": tgt_info["tgt"].split("_")[1],
                            "timestamp": int(tgt_info["ins"][0][0].split("-")[0]),
                            "data": tgt_info["ins"][0][1][tgt.split("_")[1]]
                        }
                        tmp_data["messages"].append({"source": source, "target": target})
                else:
                    if request_id in remains:
                        remains[request_id].append(source)
                    else:
                        remains[request_id] = [source]
        if request_id not in processed_data:
            processed_data[request_id] = tmp_data
        else:
            processed_data[request_id]["messages"].extend(tmp_data["messages"])
    return processed_data
