import socket
import threading
import time

store = {}  # key -> (value, expiry_timestamp or list)
blocked_clients = {}  # key -> list of (connection, timeout, start_time, lock)

lock = threading.Lock()


def parse_resp(buffer):
    ...
    # (same as before, no change)
    ...


def wake_blocked_clients(key):
    """Wake up clients blocked on a given key if data is available."""
    with lock:
        if key in blocked_clients and key in store and isinstance(store[key], list) and store[key]:
            # Pop one element for each blocked client (FIFO order)
            while store[key] and blocked_clients[key]:
                conn, timeout, start_time, wait_lock = blocked_clients[key].pop(0)
                value = store[key].pop(0)
                resp = f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
                try:
                    conn.sendall(resp.encode())
                except Exception:
                    pass
                finally:
                    wait_lock.release()  # unblock thread
            if not blocked_clients[key]:
                del blocked_clients[key]


def handle_client(connection):
    with connection:
        buffer = b""
        while True:
            chunk = connection.recv(1024)
            if not chunk:
                break  # client disconnected
            buffer += chunk

            while buffer:
                parts, buffer = parse_resp(buffer)
                if not parts:
                    break  # incomplete command

                command = parts[0].upper() if parts else None

                # ... (PING, ECHO, SET, GET etc unchanged)

                # LPUSH
                if command == "LPUSH" and len(parts) >= 3:
                    key = parts[1]
                    values = parts[2:]
                    if key not in store or not isinstance(store[key], list):
                        store[key] = []
                    for value in values:
                        store[key].insert(0, value)
                    resp = f":{len(store[key])}\r\n"
                    connection.sendall(resp.encode())
                    wake_blocked_clients(key)  # notify blocked clients
                    continue

                # RPUSH
                if command == "RPUSH" and len(parts) >= 3:
                    key = parts[1]
                    values = parts[2:]
                    if key not in store or not isinstance(store[key], list):
                        store[key] = []
                    store[key].extend(values)
                    resp = f":{len(store[key])}\r\n"
                    connection.sendall(resp.encode())
                    wake_blocked_clients(key)  # notify blocked clients
                    continue

                # BLPOP
                if command == "BLPOP" and len(parts) >= 3:
                    keys = parts[1:-1]
                    try:
                        timeout = int(parts[-1])
                    except ValueError:
                        connection.sendall(b"-ERR timeout is not an integer\r\n")
                        continue

                    # First, check if any key has data right now
                    found = False
                    for key in keys:
                        if key in store and isinstance(store[key], list) and store[key]:
                            value = store[key].pop(0)
                            resp = f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
                            connection.sendall(resp.encode())
                            found = True
                            break
                    if found:
                        continue

                    # Otherwise, block the client
                    wait_lock = threading.Lock()
                    wait_lock.acquire()

                    with lock:
                        for key in keys:
                            blocked_clients.setdefault(key, []).append(
                                (connection, timeout, time.time(), wait_lock)
                            )

                    # Block until timeout or data arrives
                    if timeout == 0:
                        wait_lock.acquire()  # block indefinitely
                    else:
                        acquired = wait_lock.acquire(timeout=timeout)
                        if not acquired:
                            # Timeout reached
                            connection.sendall(b"$-1\r\n")
                            with lock:
                                for key in keys:
                                    if key in blocked_clients:
                                        blocked_clients[key] = [
                                            c for c in blocked_clients[key] if c[0] != connection
                                        ]
                                        if not blocked_clients[key]:
                                            del blocked_clients[key]
                    continue

                # (other commands unchanged...)

                # Unknown command
                connection.sendall(b"-ERR unknown command\r\n")
                continue


def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("localhost", 6379))
    server_socket.listen()

    print("Server is running on localhost:6379")

    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
