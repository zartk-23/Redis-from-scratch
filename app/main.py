import socket
import threading
import time

store = {}  # key -> (value, expiry_timestamp or list)
blocking_clients = {}  # key -> list of (conn, start_time, timeout)


def parse_resp(buffer):
    """Parse a RESP message from the buffer, return (command_parts, remaining_buffer)."""
    if not buffer:
        return None, buffer

    if buffer.startswith(b"*"):
        try:
            end = buffer.find(b"\r\n")
            num_elems = int(buffer[1:end])
            buffer = buffer[end + 2:]
            parts = []
            for _ in range(num_elems):
                if not buffer.startswith(b"$"):
                    return None, buffer
                end = buffer.find(b"\r\n")
                str_len = int(buffer[1:end])
                buffer = buffer[end + 2:]
                parts.append(buffer[:str_len].decode())
                buffer = buffer[str_len + 2:]
            return parts, buffer
        except Exception:
            return None, buffer
    return None, buffer


def handle_client(conn, addr):
    print(f"Connected by {addr}")
    buffer = b""
    while True:
        try:
            data = conn.recv(4096)
            if not data:
                break
            buffer += data
            while True:
                parsed = parse_resp(buffer)
                if parsed is None:
                    break
                parts, buffer = parsed
                if not parts:
                    continue

                cmd = parts[0].upper()

                # ----------------
                # RPUSH command
                # ----------------
                if cmd == "RPUSH":
                    if len(parts) < 3:
                        conn.sendall(b"-ERR wrong number of arguments for 'rpush' command\r\n")
                        continue
                    key = parts[1]
                    values = parts[2:]
                    if key not in store:
                        store[key] = []
                    if not isinstance(store[key], list):
                        conn.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                        continue
                    store[key].extend(values)
                    resp = f":{len(store[key])}\r\n"
                    conn.sendall(resp.encode())

                # ----------------
                # BLPOP command
                # ----------------
                elif cmd == "BLPOP":
                    if len(parts) < 3:
                        conn.sendall(b"-ERR wrong number of arguments for 'blpop' command\r\n")
                        continue

                    keys = parts[1:-1]
                    try:
                        timeout = float(parts[-1])
                    except ValueError:
                        conn.sendall(b"-ERR timeout is not a number\r\n")
                        continue

                    popped = None
                    for key in keys:
                        if key in store and isinstance(store[key], list) and store[key]:
                            popped = (key, store[key].pop(0))
                            break

                    if popped:
                        key, val = popped
                        resp = f"*2\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
                        conn.sendall(resp.encode())
                    else:
                        # no element available → block
                        blocking_clients.setdefault(tuple(keys), []).append(
                            (conn, time.time(), timeout)
                        )

                # ----------------
                # Default: error
                # ----------------
                else:
                    conn.sendall(b"-ERR unknown command\r\n")

        except ConnectionResetError:
            break
    conn.close()


def check_blocking_clients():
    """Periodically check blocking clients and wake them up if data is available or timeout."""
    while True:
        time.sleep(0.05)
        now = time.time()
        to_remove = []
        for keys, clients in list(blocking_clients.items()):
            new_clients = []
            for conn, start, timeout in clients:
                popped = None
                for key in keys:
                    if key in store and isinstance(store[key], list) and store[key]:
                        popped = (key, store[key].pop(0))
                        break

                if popped:
                    key, val = popped
                    resp = f"*2\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
                    try:
                        conn.sendall(resp.encode())
                    except Exception:
                        pass
                    continue  # don’t re-add
                elif timeout > 0 and now - start >= timeout:
                    try:
                        conn.sendall(b"$-1\r\n")  # timeout → null bulk string
                    except Exception:
                        pass
                    continue  # don’t re-add
                else:
                    new_clients.append((conn, start, timeout))
            if new_clients:
                blocking_clients[keys] = new_clients
            else:
                to_remove.append(keys)
        for k in to_remove:
            blocking_clients.pop(k, None)


def main():
    HOST = "0.0.0.0"
    PORT = 6379
    threading.Thread(target=check_blocking_clients, daemon=True).start()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        print(f"Server listening on {HOST}:{PORT}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()
