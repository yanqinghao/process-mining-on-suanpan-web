# coding=utf-8


def count_nodes(node_messages):
    return [{
        "node_id": node_id,
        "count": len(set([m[1]["id"] for m in messages]))
    } for node_id, messages in node_messages.items()]


def count_edges(graph, node_messages):
    edges = []
    for src, tgts in graph.items():
        source = {"node_id": src.split("_")[0], "port": src.split("_")[1]}
        for tgt in tgts:
            target = {"node_id": tgt.split("_")[0], "port": tgt.split("_")[1]}
            node_message = node_messages[tgt.split("_")[0]]
            edges.append({
                "src":
                    source,
                "tgt":
                    target,
                "count":
                    len(set([i[1]["id"] for i in node_message if tgt.split("_")[1] in i[1].keys()])
                       )
            })
    return edges


def time_cost_nodes(nodes, master_messages, node_messages):
    nodes_cost = []
    for node in nodes:
        node_out = [i for i in master_messages if i[1]["node_id"] == node]
        times = []
        for request_id in set([i[1]["request_id"] for i in node_out]):
            node_in = node_messages.get(node)
            if node_in:
                in_times = [int(i[0].split("-")[0]) for i in node_in if i[1]["id"] == request_id]
                if in_times:
                    in_time = min(in_times)
                    out_time = max([
                        int(i[0].split("-")[0])
                        for i in node_out
                        if i[1]["request_id"] == request_id
                    ])
                    times.append(out_time - in_time)
        nodes_cost.append({"node_id": node, "cost": sum(times) / len(times) if times else None})
    return nodes_cost


def time_cost_edges(graph, master_messages, node_messages):
    edges_cost = []
    for src, tgts in graph.items():
        source = {"node_id": src.split("_")[0], "port": src.split("_")[1]}
        for tgt in tgts:
            target = {"node_id": tgt.split("_")[0], "port": tgt.split("_")[1]}
            node_message = node_messages[tgt.split("_")[0]]
            out_messages = [
                i for i in master_messages
                if i[1]["node_id"] == src.split("_")[0] and src.split("_")[1] in i[1].keys()
            ]
            times = []
            for out_message in out_messages:
                out_time = int(out_message[0].split("-")[0])
                in_times = [i for i in node_message if i[1]["id"] == out_message[1]["request_id"]]
                if in_times:
                    in_time = int(in_times[0][0].split("-")[0])
                    times.append(in_time - out_time)
            edges_cost.append({
                "src": source,
                "tgt": target,
                "time": sum(times) / len(times) if times else None
            })
    return edges_cost


def count_errors(master_messages):
    return [{
        "node_id": message["node_id"],
        "request_id": message["request_id"],
        "msg": message["msg"]
    } for message in master_messages if message[1]["success"] == "false"]


def count_node_in_out(node_messages):
    return [{
        "node_id": node_id,
        "count": len(set([m[1]["id"] for m in messages]))
    } for node_id, messages in node_messages.items()]


def count_node_data_size(node_messages):
    return [{
        "node_id": node_id,
        "count": len(set([m[1]["id"] for m in messages]))
    } for node_id, messages in node_messages.items()]
