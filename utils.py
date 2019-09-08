import os
import binascii


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
