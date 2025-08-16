import socket

store = {}  # key-value store, can hold strings or lists

def encode_simple_string(s: str) -> bytes:
    return f"+{s}\r\n".encode()

def encode_integer(i: int) -> bytes:
    return f":{i}\r\n".encode()

def handle_command(parts):
    command = parts[0].upper()

    if command == "PING":
        return encode_simple_string("PONG")

    elif command == "ECHO":
        return encode_simple_string(parts[1])

    elif command == "SET":
        store[parts[1]] = parts[2]
        return encode_simple_string("OK")

    elif command == "GET":
        value = store.get(parts[1])
        if value is None:
            return b"$-1\r\n"   # RESP nil
        return f"${len(value)}\r\n{value}\r\n".encode()

    elif command == "RPUSH":
        key, value = parts[1], parts[2]

        if key not in store:
            store[key] = []  # create a new list

        store[key].append(value)  # append element
        return encode_integer(len(store[key]))  # length of list

    return encode_simple_string("ERR unknown command")

def parse_resp(data: bytes):
    # Very naive RESP array parser: only handles arrays of bulk strings
    if not data.startswith(b"*"):
        return []

    lines = data.split(b"\r\n")
    parts = []
    for i in range(2, len(lines), 2):  # skip "*<n>" and "$<len>"
        if lines[i] == b"":
            continue
        parts.append(lines[i].decode())
    return parts

def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", 6379))
    server.listen(1)

    while True:
        client, _ = server.accept()
        data = client.recv(1024)
        if not data:
            client.close()
            continue

        parts = parse_resp(data)
        if not parts:
            client.close()
            continue

        response = handle_command(parts)
        client.sendall(response)
        client.close()

if __name__ == "__main__":
    main()
