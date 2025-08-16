import socket

# In-memory key-value store
store = {}

def encode_simple_string(s: str) -> bytes:
    return f"+{s}\r\n".encode()

def encode_bulk_string(s: str) -> bytes:
    return f"${len(s)}\r\n{s}\r\n".encode()

def encode_integer(i: int) -> bytes:
    return f":{i}\r\n".encode()

def handle_command(parts):
    command = parts[0].upper()

    if command == "PING":
        return encode_simple_string("PONG")

    elif command == "ECHO" and len(parts) > 1:
        return encode_bulk_string(parts[1])

    elif command == "SET" and len(parts) > 2:
        key, value = parts[1], parts[2]
        store[key] = value
        return encode_simple_string("OK")

    elif command == "GET" and len(parts) > 1:
        key = parts[1]
        if key in store:
            value = store[key]
            # If it's a list, Redis GET returns an error, but for now keep it simple
            if isinstance(value, list):
                return encode_bulk_string("")  
            return encode_bulk_string(value)
        else:
            return b"$-1\r\n"

    elif command == "RPUSH" and len(parts) > 2:
        key, value = parts[1], parts[2]
        if key not in store:
            store[key] = []   # create new list
        if not isinstance(store[key], list):
            # Redis would return an error if key is not a list
            return encode_simple_string("ERR: wrong type")
        store[key].append(value)
        return encode_integer(len(store[key]))

    else:
        return encode_simple_string("ERR: unknown command")

def parse_message(message: str):
    lines = message.split("\r\n")
    parts = [line for line in lines if line and not line.startswith("*") and not line.startswith("$")]
    return parts

def start_server():
    host = "localhost"
    port = 6379

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((host, port))
        server_socket.listen(1)
        print(f"Redis clone running on {host}:{port}")

        while True:
            client_socket, _ = server_socket.accept()
            with client_socket:
                data = client_socket.recv(1024).decode()
                if not data:
                    continue
                parts = parse_message(data)
                response = handle_command(parts)
                client_socket.sendall(response)

if __name__ == "__main__":
    start_server()
