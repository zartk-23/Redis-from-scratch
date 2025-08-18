import socket
import threading
import time

store = {}  # key -> (value, expiry_timestamp or list)
blpop_waiting = {}  # key -> list of (conn, condition, end_time)
lock = threading.Lock()  # To synchronize access to shared data
condition = threading.Condition(lock)  # For blocking and notifying clients

def parse_resp(buffer):
    """Parse a RESP message from the buffer, return (command_parts, remaining_buffer)."""
    if not buffer:
        return None, buffer

    try:
        decoded = buffer.decode()
    except UnicodeDecodeError:
        return None, buffer

    if not decoded.startswith("*"):
        return None, buffer

    lines = decoded.split("\r\n")
    try:
        if len(lines) < 1 or not lines[0].startswith("*"):
            return None, buffer
        count = int(lines[0][1:])
        if count < 1:
            return None, buffer
        parts = []
        idx = 1
        for _ in range(count):
            if idx + 1 >= len(lines) or not lines[idx].startswith("$"):
                return None, buffer
            strlen = int(lines[idx][1:])
            idx += 1
            if idx >= len(lines) or len(lines[idx]) != strlen:
                return None, buffer
            parts.append(lines[idx])
            idx += 1
        remaining = "\r\n".join(lines[idx:]).encode() if idx < len(lines) else b""
        return parts, remaining
    except (ValueError, IndexError):
        return None, buffer

def encode_resp(data):
    """Encode Python object into RESP format."""
    if data is None:
        return b"$-1\r\n"
    if isinstance(data, str):
        return f"+{data}\r\n".encode()
    if isinstance(data, int):
        return f":{data}\r\n".encode()
    if isinstance(data, list):
        resp = f"*{len(data)}\r\n"
        for item in data:
            if item is None:
                resp += "$-1\r\n"
            else:
                resp += f"${len(str(item))}\r\n{item}\r\n"
        return resp.encode()
    return b"-ERR Unsupported type\r\n"

def handle_client(conn, addr):
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
                command = parts[0].upper()
                if command == "PING":
                    conn.sendall(encode_resp("PONG"))
                elif command == "ECHO":
                    conn.sendall(encode_resp(parts[1]))
                elif command == "SET":
                    key, value = parts[1], parts[2]
                    expiry = None
                    if len(parts) > 3 and parts[3].upper() == "PX":
                        expiry = time.time() + int(parts[4]) / 1000.0
                    store[key] = (value, expiry)
                    conn.sendall(encode_resp("OK"))
                elif command == "GET":
                    key = parts[1]
                    if key in store:
                        value, expiry = store[key]
                        if expiry and time.time() > expiry:
                            del store[key]
                            conn.sendall(encode_resp(None))
                        else:
                            conn.sendall(encode_resp(value))
                    else:
                        conn.sendall(encode_resp(None))
                elif command == "DEL":
                    count = 0
                    for key in parts[1:]:
                        if key in store:
                            del store[key]
                            count += 1
                    conn.sendall(encode_resp(count))
                elif command == "LPUSH":
                    key, values = parts[1], parts[2:]
                    with lock:
                        if key not in store or not isinstance(store[key][0], list):
                            store[key] = ([], None)
                        store[key][0][0:0] = values
                        if key in blpop_waiting:
                            for client_conn, client_cond, _ in blpop_waiting[key][:]:
                                with client_cond:
                                    client_cond.notify()
                    conn.sendall(encode_resp(len(store[key][0])))
                elif command == "RPOP":
                    key = parts[1]
                    if key in store and isinstance(store[key][0], list) and store[key][0]:
                        val = store[key][0].pop()
                        conn.sendall(encode_resp(val))
                    else:
                        conn.sendall(encode_resp(None))
                elif command == "BLPOP":
                    keys = parts[1:-1]
                    try:
                        timeout = float(parts[-1])
                    except ValueError:
                        conn.sendall(b"-ERR value is not a valid float\r\n")
                        continue
                    end_time = time.time() + timeout if timeout > 0 else None
                    with lock:
                        for key in keys:
                            if key in store and isinstance(store[key][0], list) and store[key][0]:
                                val = store[key][0].pop(0)
                                conn.sendall(encode_resp([key, val]))
                                break
                        else:
                            if timeout == 0:
                                conn.sendall(encode_resp(None))
                                continue
                            client_cond = threading.Condition(lock)
                            for key in keys:
                                if key not in blpop_waiting:
                                    blpop_waiting[key] = []
                                blpop_waiting[key].append((conn, client_cond, end_time))
                            try:
                                with client_cond:
                                    remaining_time = end_time - time.time() if end_time else None
                                    while remaining_time is None or remaining_time > 0:
                                        for key in keys:
                                            if key in store and isinstance(store[key][0], list) and store[key][0]:
                                                val = store[key][0].pop(0)
                                                conn.sendall(encode_resp([key, val]))
                                                return  # Exit after sending response
                                        client_cond.wait(timeout=remaining_time)
                                        remaining_time = end_time - time.time() if end_time else None
                                    conn.sendall(encode_resp(None))
                            finally:
                                for k in keys:
                                    if k in blpop_waiting:
                                        blpop_waiting[k] = [
                                            (c, cond, t)
                                            for c, cond, t in blpop_waiting[k]
                                            if c != conn
                                        ]
                                        if not blpop_waiting[k]:
                                            del blpop_waiting[k]
                else:
                    conn.sendall(b"-ERR unknown command\r\n")
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
            break
    with lock:
        for key in list(blpop_waiting.keys()):
            blpop_waiting[key] = [(c, cond, t) for c, cond, t in blpop_waiting[key] if c != conn]
            if not blpop_waiting[key]:
                del blpop_waiting[key]
    conn.close()

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", 6379))
    server.listen(5)
    print("Server running on port 6379")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    start_server()