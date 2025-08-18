import socket
import threading
import time

store = {}  # key -> (value, expiry_timestamp or list)
lock = threading.Lock()
blocking_clients = {}  # key -> [ (conn, condition) ]


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
    if len(lines) < 3:
        return None, buffer

    parts = []
    idx = 1
    while idx < len(lines) - 1:
        if lines[idx].startswith("$"):
            length = int(lines[idx][1:])
            if idx + 1 >= len(lines) - 1:
                return None, buffer
            parts.append(lines[idx + 1])
            idx += 2
        else:
            idx += 1

    # calculate consumed bytes
    consumed = 0
    for p in parts:
        consumed += len(p) + len(str(len(p))) + 4
    consumed += len(str(len(parts))) + 3

    return parts, buffer[consumed:]


def send_resp(conn, data):
    """Send data in RESP format."""
    if isinstance(data, str):
        conn.sendall(f"+{data}\r\n".encode())
    elif isinstance(data, int):
        conn.sendall(f":{data}\r\n".encode())
    elif data is None:
        conn.sendall(b"$-1\r\n")
    elif isinstance(data, list):
        conn.sendall(f"*{len(data)}\r\n".encode())
        for item in data:
            conn.sendall(f"${len(item)}\r\n{item}\r\n".encode())


def handle_client(conn, addr):
    buffer = b""
    while True:
        try:
            data = conn.recv(4096)
            if not data:
                break
            buffer += data

            while True:
                result = parse_resp(buffer)
                if not result:
                    break
                parts, buffer = result
                if not parts:
                    continue

                command = parts[0].upper()

                if command == "PING":
                    send_resp(conn, "PONG")

                elif command == "ECHO":
                    send_resp(conn, parts[1])

                elif command == "SET":
                    key, value = parts[1], parts[2]
                    with lock:
                        store[key] = value
                    send_resp(conn, "OK")

                elif command == "GET":
                    key = parts[1]
                    with lock:
                        val = store.get(key, None)
                    if isinstance(val, list):
                        send_resp(conn, None)
                    else:
                        send_resp(conn, val)

                elif command == "RPUSH":
                    key, *values = parts[1:]
                    with lock:
                        if key not in store or not isinstance(store[key], list):
                            store[key] = []
                        store[key].extend(values)
                        length = len(store[key])

                        # wake up blocked clients
                        if key in blocking_clients:
                            for client_conn, cond in blocking_clients[key]:
                                with cond:
                                    cond.notify()
                            del blocking_clients[key]

                    send_resp(conn, length)

                elif command == "LPOP":
                    key = parts[1]
                    with lock:
                        if key not in store or not isinstance(store[key], list) or not store[key]:
                            send_resp(conn, None)
                        else:
                            val = store[key].pop(0)
                            send_resp(conn, val)

                elif cmd == "BLPOP":
                    if len(parts) < 3:
                        client.sendall(b"-ERR wrong number of arguments for 'blpop' command\r\n")
                        continue

                    keys = parts[1:-1]
                    try:
                        timeout = float(parts[-1])
                    except ValueError:
                        client.sendall(b"-ERR invalid timeout\r\n")
                        continue

                    popped = None
                    start_time = time.time()

                    while True:
                        # check all keys
                        for k in keys:
                            if k in store and isinstance(store[k], list) and store[k]:
                                popped = (k, store[k].pop(0))
                                break

                        if popped:
                            key, val = popped
                            response = f"*2\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
                            client.sendall(response.encode())
                            break

                        # timeout check
                        if timeout != 0 and (time.time() - start_time) >= timeout:
                            client.sendall(b"$-1\r\n")  # Null bulk string on timeout
                            break

                        time.sleep(0.05)  # short sleep to avoid busy loop


                        # register client for blocking
                        for key in keys:
                            cond = threading.Condition()
                            with cond:
                                blocking_clients.setdefault(key, []).append((conn, cond))
                                cond.wait(timeout=0.1)
                            # retry loop

                else:
                    send_resp(conn, f"ERR unknown command '{command}'")

        except Exception as e:
            try:
                send_resp(conn, f"ERR {str(e)}")
            except:
                pass
            break

    conn.close()


def main():
    host = "0.0.0.0"
    port = 6379
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen()
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()
