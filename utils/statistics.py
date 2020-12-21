# coding=utf-8


def count_nodes(node_messages):
    return {
        node_id: len(set([m[1]["id"] for m in messages])) for node_id, messages in node_messages
    }


# {src:{node_id:,port:} tgt:{node_id:,port:} count:}
def count_edges():
    pass


# node_id: time
def time_cost_nodes():
    pass


# {src:{node_id:,port:} tgt:{node_id:,port:} time:}
def time_cost_edges():
    pass


def count_errors(master_messages):
    return [{
        "node_id": message["node_id"],
        "request_id": message["request_id"],
        "msg": message["msg"]
    } for message in master_messages if message["success"] == "false"]
