import socket
import threading
import time

store = {}  # key -> (value, expiry_timestamp or list)

# RESP encoding helpers
def encode_simple_string(s):
    return f"+{s}\r\n".encode()

def encode_error(msg):
    return f"-{msg}\r\n".encode()

def encode_integer(i):
    return f":{i}\r\n".encode()

def encode_bulk_string(s):
    if s is None:
        return b"$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n".encode()

def encode_array(arr):
    if arr is None:
        return b"*-1\r\n"
    out = f"*{len(arr)}\r\n".encode()
    for a in arr:
        if isinstance(a, bytes):
            out += encode_bulk_string(a.decode())
        else:
            out += encode_bulk_string(str(a))
    return out

# RESP parser
def parse_resp(buffer):
    if not buffer:
        return None, buffer
    
    prefix = buffer[:1]
    if prefix == b'*':  # Array
        try:
            end = buffer.index(b'\r\n')
        except ValueError:
            return None, buffer
        length = int(buffer[1:end])
        items = []
        rest = buffer[end+2:]
        for _ in range(length):
            if rest[:1] != b'$':
                return None, buffer
            lend = rest.index(b'\r\n')
            strlen = int(rest[1:lend])
            start = lend+2
            bulk = rest[start:start+strlen]
            items.append(bulk.decode())
            rest = rest[start+strlen+2:]
        return items, rest
    return None, buffer

waiting_clients = {}  # key -> list of (conn, timeout_timestamp)
lock = threading.Lock()

def handle_command(conn, parts):
    cmd = parts[0].upper()

    # PING
    if cmd == 'PING':
        if len(parts) == 2:
            return encode_bulk_string(parts[1])
        return encode_simple_string("PONG")

    # ECHO
    elif cmd == 'ECHO':
        if len(parts) != 2:
            return encode_error("ERR wrong number of arguments for 'echo' command")
        return encode_bulk_string(parts[1])

    # SET
    elif cmd == 'SET':
        if len(parts) < 3:
            return encode_error("ERR wrong number of arguments for 'set' command")
        key, value = parts[1], parts[2]
        expiry = None
        if len(parts) > 3 and parts[3].upper() == 'PX':
            expiry = time.time() + int(parts[4]) / 1000
        store[key] = (value, expiry)
        return encode_simple_string("OK")

    # GET
    elif cmd == 'GET':
        if len(parts) != 2:
            return encode_error("ERR wrong number of arguments for 'get' command")
        key = parts[1]
        if key not in store:
            return encode_bulk_string(None)
        value, expiry = store[key]
        if expiry and expiry < time.time():
            del store[key]
            return encode_bulk_string(None)
        if isinstance(value, list):
            return encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
        return encode_bulk_string(value)

    # RPUSH
    elif cmd == 'RPUSH':
        if len(parts) < 3:
            return encode_error("ERR wrong number of arguments for 'rpush' command")
        key = parts[1]
        values = parts[2:]
        if key not in store:
            store[key] = ([], None)
        val, expiry = store[key]
        if not isinstance(val, list):
            return encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
        val.extend(values)
        store[key] = (val, expiry)
        return encode_integer(len(val))

    # LPOP
    elif cmd == 'LPOP':
        if len(parts) < 2:
            return encode_error("ERR wrong number of arguments for 'lpop' command")
        key = parts[1]
        count = 1
        if len(parts) == 3:
            count = int(parts[2])
        if key not in store:
            return encode_bulk_string(None) if count == 1 else encode_array([])
        val, expiry = store[key]
        if not isinstance(val, list):
            return encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
        popped = []
        for _ in range(min(count, len(val))):
            popped.append(val.pop(0))
        if not val:
            del store[key]
        else:
            store[key] = (val, expiry)
        if count == 1:
            return encode_bulk_string(popped[0]) if popped else encode_bulk_string(None)
        return encode_array(popped)

    # BLPOP
    elif cmd == 'BLPOP':
        if len(parts) < 3:
            return encode_error("ERR wrong number of arguments for 'blpop' command")
        keys = parts[1:-1]
        timeout = float(parts[-1])

        for key in keys:
            if key in store:
                val, expiry = store[key]
                if isinstance(val, list) and val:
                    item = val.pop(0)
                    if not val:
                        del store[key]
                    else:
                        store[key] = (val, expiry)
                    return encode_array([key, item])

        if timeout == 0:
            return encode_bulk_string(None)

        with lock:
            expire_at = time.time() + timeout
            for key in keys:
                waiting_clients.setdefault(key, []).append((conn, expire_at))
        return None

    return encode_error("ERR unknown command")

def client_thread(conn, addr):
    buffer = b""
    while True:
        try:
            data = conn.recv(4096)
            if not data:
                break
            buffer += data
            while True:
                parts, buffer = parse_resp(buffer)
                if not parts:
                    break
                response = handle_command(conn, parts)
                if response is not None:
                    conn.sendall(response)
        except ConnectionResetError:
            break
        except Exception as e:
            conn.sendall(encode_error(f"ERR {str(e)}"))
            break
    conn.close()

def blpop_watcher():
    while True:
        time.sleep(0.1)
        now = time.time()
        with lock:
            for key, clients in list(waiting_clients.items()):
                if key in store:
                    val, expiry = store[key]
                    if isinstance(val, list) and val:
                        item = val.pop(0)
                        if not val:
                            del store[key]
                        else:
                            store[key] = (val, expiry)
                        conn, _ = clients.pop(0)
                        conn.sendall(encode_array([key, item]))
                        if not clients:
                            del waiting_clients[key]
                        continue
                new_clients = []
                for conn, expire_at in clients:
                    if now > expire_at:
                        conn.sendall(encode_bulk_string(None))
                    else:
                        new_clients.append((conn, expire_at))
                if new_clients:
                    waiting_clients[key] = new_clients
                else:
                    del waiting_clients[key]

if __name__ == "__main__":
    threading.Thread(target=blpop_watcher, daemon=True).start()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", 6379))
    server.listen(5)
    print("Redis clone running on port 6379")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=client_thread, args=(conn, addr), daemon=True).start()
