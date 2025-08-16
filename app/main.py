import socket

# In-memory key-value store
store = {}

def encode_simple_string(s: str) -> bytes:
    return f"+{s}\r\n".encode()

def encode_bulk_string(s: str) -> bytes:
    if s is None:
        return b"$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n".encode()

def encode_integer(i: int) -> bytes:
    return f":{i}\r\n".encode()

def decode_request(data: bytes):
    """
    Decodes RESP arrays like:
    *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    """
    parts = data.split(b"\r\n")
    arr_len = int(parts[0][1:])  # *N
    items = []
    idx = 1
    for _ in range(arr_len):
        length = int(parts[idx][1:])  # $len
        idx += 1
        items.append(parts[idx].decode())  # actual string
        idx += 1
    return items

def handle_command(command: list[str]) -> bytes:
    cmd = command[0].upper()

    if cmd == "PING":
        return encode_simple_string("PONG")

    elif cmd == "ECHO":
        return encode_bulk_string(command[1])

    elif cmd == "SET":
        key, value = command[1], command[2]
        store[key] = value
        return encode_simple_string("OK")

    elif cmd == "GET":
        key = command[1]
        return encode_bulk_string(store.get(key))

    elif cmd == "RPUSH":
        key = command[1]
        values = command[2:]  # could be multiple
        if key not in store:
            store[key] = []
        if not isinstance(store[key], list):
            return encode_simple_string("ERR Wrong type")  # Redis-like error
        store[key].extend(values)
        return encode_integer(len(store[key]))

    else:
        return encode_simple_string("ERR unknown command")

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("localhost", 6379))
    server_socket.listen(1)

    print("Server running on port 6379")

    while True:
        client_socket, _ = server_socket.accept()
        data = client_socket.recv(1024)

        if not data:
            client_socket.close()
            continue

        try:
            command = decode_request(data)
            response = handle_command(command)
        except Exception as e:
            response = encode_simple_string(f"ERR {str(e)}")

        client_socket.sendall(response)
        client_socket.close()

if __name__ == "__main__":
    main()
