import socket
import threading
import time

store = {}  # key -> (value, expiry_timestamp or list)
blpop_waiting = {}  # key -> list of waiting clients


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
        count = int(lines[0][1:])
        parts = []
        idx = 1
        for _ in range(count):
            if not lines[idx].startswith("$"):
                return None, buffer
            strlen = int(lines[idx][1:])
            idx += 1
            parts.append(lines[idx])
            idx += 1
        remaining = "\r\n".join(lines[idx:]).encode()
        return parts, remaining
    except Exception:
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
                    if key not in store or not isinstance(store[key][0], list):
                        store[key] = ([], None)
                    store[key][0][0:0] = values
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
                    timeout = int(parts[-1])

                    # First, check if any list has items
                    found = None
                    for key in keys:
                        if key in store and isinstance(store[key][0], list) and store[key][0]:
                            val = store[key][0].pop(0)
                            found = [key, val]
                            break

                    if found:
                        conn.sendall(encode_resp(found))
                    else:
                        # If no element available → wait
                        end_time = time.time() + timeout if timeout != 0 else None
                        while True:
                            for key in keys:
                                if key in store and isinstance(store[key][0], list) and store[key][0]:
                                    val = store[key][0].pop(0)
                                    conn.sendall(encode_resp([key, val]))
                                    return
                            if timeout != 0 and time.time() >= end_time:
                                conn.sendall(encode_resp(None))
                                return
                            time.sleep(0.1)
                else:
                    conn.sendall(b"-ERR unknown command\r\n")
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
            break
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
