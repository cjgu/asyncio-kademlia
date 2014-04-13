"""DHT Node

Usage:
  node.py <listen-port> [--remote=<remote-node-ip>] [--remote-port=<remote-port>] [--node-id=<node-id>]
  node.py (-h | --help)
  node.py --version

Options:
  --node-id=<node-id>
  --remote=<remote-node-ip>    The remote IP of a node in system
  --remote-port=<remote port>  The port of the remote node
"""

import asyncio
from docopt import docopt
import hashlib
import binascii
import os
import json
from datetime import datetime

networkParallelism = 3
maxContactsPerBucket = 20
tExpire = 86410  # K/V TTL
tRefresh = 3600  # Bucket refresh time
tReplicate = 3600
tRepublish = 86400


class Contact(object):

    def __init__(self, node_id, address):
        self.node_id = node_id
        self.address = address
        self.last_seen = datetime.utcnow()

    def __repr__(self):
        return "<Contact {},{},{}>".format(
            self.node_id, self.address, self.last_seen)


class Value(object):

    def __init__(self, key, value, create_time):
        self.key = key
        self.value = value
        self.create_time = create_time


class RoutingTable(object):

    def __init__(self, node_id):
        self.node_id = node_id
        self.table = []
        for i in range(160):
            self.table.append([])

    def add_seen_contact(self, contact):
        dist = self.distance(contact)
        print("Distance to seen node: {}".format(dist))
        print("Bucket for node: {}".format(self.bucket_number(dist)))

        bucket = self.get_bucket(self.bucket_number(dist))

        existing = None
        for n in bucket:
            if n.node_id == contact.node_id:
                existing = n
                break

        if existing:
            existing.last_seen = contact.last_seen
        else:
            bucket.append(contact)

    def distance(self, contact):
        return contact.node_id ^ self.node_id

    def bucket_number(self, distance):
        return distance.bit_length() - 1

    def get_bucket(self, bucket_number):
        return self.table[bucket_number]


class MemoryRepository(object):
    def __init__(self):
        self.data = {}

    def store(self, value):
        self.data[value.key] = value

    def get(self, key):
        return self.data.get(key)

    def has_key(self, key):
        return key in self.data


class KademliaClient(asyncio.DatagramProtocol):

    def __init__(self):
        self.transport = None

    def connection_made(self, transport):
        print("Connection made")
        self.transport = transport
        print("Sending message: {}".format(self.message))
        self.transport.sendto(self.message)

    def send(self, message, callback):
        self.message = message
        self.callback = callback

    def datagram_received(self, data, addr):
        print('Data received: {}'.format(data))
        loop = asyncio.get_event_loop()
        loop.call_soon(self.callback, data)


class RpcRequest(object):

    def __init__(self, method, params, source_node_id, request_id=None):
        self.method = method
        self.params = params
        self.request_id = generate_request_id() if request_id is None else request_id
        self.source_node_id = source_node_id

    def serialize(self):
        return json.dumps({
            "method": self.method,
            "request_id": self.request_id,
            "params": self.params,
            "node_id": int_to_hex(self.source_node_id)
        }).encode()

    @classmethod
    def deserialize(cls, data):
        data = json.loads(data.decode())

        return RpcRequest(
            data.get("method"),
            data.get("params"),
            hex_to_int(data.get("node_id")),
            data.get("request_id"))

    def __repr__(self):
        return "RpcRequest <{},{},{},{}>".format(
            self.method,
            self.params,
            self.request_id,
            self.source_node_id)


class RpcResponse(object):

    def __init__(self, result, error, request_id, node_id):
        self.result = result
        self.error = error
        self.request_id = request_id
        self.node_id = node_id

    def serialize(self):
        return json.dumps({
            "request_id": self.request_id,
            "result": self.result,
            "node_id": int_to_hex(self.node_id),
            "error": self.error
        }).encode()

    @classmethod
    def deserialize(cls, data):
        data = json.loads(data.decode())

        return RpcResponse(
            data.get("result"),
            data.get("error"),
            data.get("request_id"),
            hex_to_int(data.get('node_id')))

    def __repr__(self):
        return "RpcResponse <{},{},{},{}>".format(
            self.result, self.error, self.request_id, self.node_id)


class RpcClient(object):

    def __init__(self, source_node_id, addr, port):
        self.client = KademliaClient()
        self.source_node_id = source_node_id
        self.addr = addr
        self.port = port

    @asyncio.coroutine
    def _connect(self):
        loop = asyncio.get_event_loop()
        yield from loop.create_datagram_endpoint(
            lambda: self.client, remote_addr=(self.addr, self.port))

    @asyncio.coroutine
    def ping(self, params, callback):
        self.callback = callback

        self.request = RpcRequest('ping', params, self.source_node_id)

        self.client.send(self.request.serialize(), self.check_response)

        yield from self._connect()

    def check_response(self, response):
        response = RpcResponse.deserialize(response)
        self.callback(response)


class RpcClientFactory(object):
    @classmethod
    def client(self, source_node_id, address):
        addr, port = address
        client = RpcClient(source_node_id, addr, port)
        return client


class KademliaNode(asyncio.DatagramProtocol):

    def __init__(self, node_id):
        self.transport = None
        self.node_id = node_id
        self.routes = RoutingTable(self.node_id)
        self.repo = MemoryRepository()
        self.client_factory = RpcClientFactory()

    def connection_made(self, transport):
        self.transport = transport

    def _is_valid_request(self, request):
        if request is None or \
           request.request_id is None or\
           request.method is None or\
           request.params is None:
            return False
        return True

    def datagram_received(self, data, addr):
        print("Received from {}:{}".format(addr[0], addr[1]))
        request = RpcRequest.deserialize(data)

        if not self._is_valid_request(request):
            # TODO extract function
            print("Invalid message")
            response = RpcResponse(None, "Invalid request", request.request_id, self.node_id)
            self.transport.sendto(response.serialize(), addr=addr)
            return

        dispatch = {
            'ping': self.ping,
            'store': self.store,
            'find_node': self.find_node,
            'find_value': self.find_value,
        }
        func = dispatch.get(request.method)

        if func is not None:
            result = error = None
            try:
                result = func(request.params, request.source_node_id, addr)
            except Exception as e:
                print(e)
                error = "Internal server error"
            response = RpcResponse(result, error, request.request_id, self.node_id)
            self.transport.sendto(response.serialize(), addr=addr)
        else:
            response = RpcResponse(None, "Invalid method", request.request_id, self.node_id)
            self.transport.sendto(response.serialize(), addr=addr)

    def ping(self, message, node_id, addr):
        """ Ping command """
        print("Received PING from node {}".format(node_id))

        contact = Contact(node_id, addr)
        self.routes.add_seen_contact(contact)

        # TODO: Return known hosts

        print(self.routes.table)
        return {}

    def store(self, message, node_id, addr):
        """ Store command """
        return {}

    def find_node(self, message, node_id, addr):
        """ Find node command"""
        return {}

    def find_value(self, message, node_id, addr):
        """ Find value command"""
        return {}

    def error_received(self, exc):
        print(exc)

    @asyncio.coroutine
    def connect_to_node(self, addr):
        remote_addr, remote_port = addr
        print("Attempt to connect to {}:{}".format(remote_addr, remote_port))
        client = self.client_factory.client(self.node_id, addr)

        print("Pinging node")
        yield from client.ping([], self.ping_response)

    def ping_response(self, response):
        # TODO: Update routing table
        print("PING RESPONSE", response)


def generate_node_id():
    """Generates a random 160 bit integer"""
    id = generate_byte_string(160)
    return int.from_bytes(id, byteorder='big')


def generate_request_id():
    return bytes_to_hex(generate_byte_string(160))


def int_to_hex(n):
    bytes = (n).to_bytes(160//8, byteorder='big')
    return bytes_to_hex(bytes)


def bytes_to_hex(bytes):
    return binascii.hexlify(bytes).decode('utf-8')


def hex_to_bytes(hex_str):
    return binascii.unhexlify(hex_str)


def hex_to_int(hex_str):
    if not hex_str:
        return None
    bytes = hex_to_bytes(hex_str)
    return bytes_to_int(bytes)


def bytes_to_int(bytes):
    return int.from_bytes(bytes, byteorder='big')


def generate_byte_string(bit_size):
    return os.urandom(bit_size//8)


def main():
    arguments = docopt(__doc__, version='DHT Node 0.1')

    port = arguments['<listen-port>']
    remote_addr = arguments['--remote']
    remote_port = arguments['--remote-port']
    node_id = arguments['--node-id']

    if not node_id:
        node_id = generate_node_id()

    print("Port: {}".format(port))
    print("Remote address {}:{}".format(remote_addr, remote_port))
    print("Node id {0}".format(node_id))

    node = KademliaNode(node_id)

    loop = asyncio.get_event_loop()
    coro = loop.create_datagram_endpoint(lambda: node, local_addr=('127.0.0.1', port))
    server, _ = loop.run_until_complete(coro)

    if remote_addr and remote_port:
        loop.run_until_complete(node.connect_to_node((remote_addr, remote_port)))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print("Shutting down")
    finally:
        server.close()
        loop.close()


if __name__ == '__main__':
    main()
