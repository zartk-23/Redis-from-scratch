import socket

# In-memory key-value store
store = {}

def encode_simple_string(s):
    return f"+{s}\r\n"

def encode_bulk_string(s):
    if s is None:
        return "$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n"

def encode_integer(i):
    return f":{i}\r\n"

def encode_array(arr):
    resp = f"*{len(arr)}\r\n"
    for el in arr:
        resp += encode_bulk_string(el)
    return resp

def handle_command(command_parts):
    if not command_parts:
        return encode_simple_string("ERR empty command")

    cmd = command_parts[0].upper()

    # PING command
    if cmd == "PING":
        if len(command_parts) == 1:
            return encode_simple_string("PONG")
        elif len(command_parts) == 2:
            return encode_bulk_string(command_parts[1])
        else:
            return encode_simple_string("ERR wrong number of arguments for 'PING' command")

    # ECHO command
    elif cmd == "ECHO":
        if len(command_parts) != 2:
            return encode_simple_string("ERR wrong number of arguments for 'ECHO' command")
        return encode_bulk_string(command_parts[1])

    # SET command
    elif cmd == "SET":
        if len(command_parts) != 3:
            return encode_simple_string("ERR wrong number of arguments for 'SET' command")
        store[command_parts[1]] = command_parts[2]
        return encode_simple_string("OK")

    # GET command
    elif cmd == "GET":
        if len(command_parts) != 2:
            return encode_simple_string("ERR wrong number of arguments for 'GET' command")
        value = store.get(command_parts[1], None)
        return encode_bulk_string(value)

    # RPUSH command
    elif cmd == "RPUSH":
        if len(command_parts) < 3:
            return encode_simple_string("ERR wrong number of arguments for 'RPUSH' command")
        key = command_parts[1]
        values = command_parts[2:]
        if key not in store:
            store[key] = []
        store[key].extend(values)
        return encode_integer(len(store[key]))

    # LPUSH command
    elif cmd == "LPUSH":
        if len(command_parts) < 3:
            return encode_simple_string("ERR wrong number of arguments for 'LPUSH' command")
        key = command_parts[1]
        values = command_parts[2:]
        if key not in store:
            store[key] = []
        # insert elements from the left
        for v in values:
            store[key].insert(0, v)
        return encode_integer(len(store[key]))

    # LRANGE command
    elif cmd == "LRANGE":
        if len(command_parts) != 4:
            return encode_simple_string("ERR wrong number of arguments for 'LRANGE' command")
        key = command_parts[1]
        start = int(command_parts[2])
        end = int(command_parts[3])

        if key not in store:
            return "*0\r\n"

        lst = store[key]
        if end == -1:  # support -1 meaning "end of list"
            end = len(lst) - 1

        # slice includes end index
        result = lst[start:end+1]
        return encode_array(result)

    # LLEN command
    elif cmd == "LLEN":
        if len(command_parts) != 2:
            return encode_simple_string("ERR wrong number of arguments for 'LLEN' command")
        key = command_parts[1]
        if key not in store:
            return encode_integer(0)
        return encode_integer(len(store[key]))

    else:
        return encode_simple_string(f"ERR unknown command '{cmd}'")

def parse_resp_message(data):
    parts = []
    i = 0
    while i < len(data):
        if data[i] == "*":  # array
            num_elements = int(data[i+1:data.find("\r\n", i)])
            i = data.find("\r\n", i) + 2
            for _ in range(num_elements):
                if data[i] == "$":  # bulk string
                    length = int(data[i+1:data.find("\r\n", i)])
                    i = data.find("\r\n", i) + 2
                    parts.append(data[i:i+length])
                    i += length + 2
        else:
            break
    return parts

def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("localhost", 6379))
    server_socket.listen(1)
    print("Server running on port 6379...")

    while True:
        client_socket, addr = server_socket.accept()
        data = client_socket.recv(1024).decode()
        if not data:
            client_socket.close()
            continue

        command_parts = parse_resp_message(data)
        response = handle_command(command_parts)
        client_socket.sendall(response.encode())
        client_socket.close()

if __name__ == "__main__":
    start_server()
