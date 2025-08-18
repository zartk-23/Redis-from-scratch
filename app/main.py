import socket
import threading
import time

store = {}  # key -> (value, expiry_timestamp or list)


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

    try:
        n = int(lines[0][1:])
    except ValueError:
        return None, buffer

    parts = []
    i = 1
    for _ in range(n):
        if not lines[i].startswith("$"):
            return None, buffer
        try:
            length = int(lines[i][1:])
        except ValueError:
            return None, buffer
        i += 1
        if i >= len(lines):
            return None, buffer
        parts.append(lines[i])
        i += 1

    consumed = "\r\n".join(lines[:i]) + "\r\n"
    remaining = buffer[len(consumed.encode()):]
    return parts, remaining


def handle_client(conn, addr):
    buffer = b""
    while True:
        data = conn.recv(4096)
        if not data:
            break
        buffer += data

        while True:
            result = parse_resp(buffer)
            if result is None:
                break
            parts, buffer = result

            if not parts:
                continue

            cmd = parts[0].upper()

            if cmd == "PING":
                conn.sendall(b"+PONG\r\n")

            elif cmd == "ECHO":
                if len(parts) < 2:
                    conn.sendall(b"-ERR wrong number of arguments for 'echo' command\r\n")
                else:
                    msg = parts[1]
                    resp = f"${len(msg)}\r\n{msg}\r\n"
                    conn.sendall(resp.encode())

            elif cmd == "SET":
                if len(parts) < 3:
                    conn.sendall(b"-ERR wrong number of arguments for 'set' command\r\n")
                else:
                    key, value = parts[1], parts[2]
                    expiry = None
                    if len(parts) >= 5 and parts[3].upper() == "PX":
                        try:
                            expiry = time.time() + int(parts[4]) / 1000.0
                        except ValueError:
                            conn.sendall(b"-ERR invalid PX value\r\n")
                            continue
                    store[key] = (value, expiry)
                    conn.sendall(b"+OK\r\n")

            elif cmd == "GET":
                if len(parts) < 2:
                    conn.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")
                else:
                    key = parts[1]
                    if key not in store:
                        conn.sendall(b"$-1\r\n")
                    else:
                        value, expiry = store[key]
                        if expiry is not None and time.time() > expiry:
                            del store[key]
                            conn.sendall(b"$-1\r\n")
                        else:
                            resp = f"${len(value)}\r\n{value}\r\n"
                            conn.sendall(resp.encode())

            elif cmd == "LPUSH":
                if len(parts) < 3:
                    conn.sendall(b"-ERR wrong number of arguments for 'lpush' command\r\n")
                else:
                    key = parts[1]
                    values = parts[2:]
                    if key not in store or not isinstance(store[key], list):
                        store[key] = []
                    for v in values:
                        store[key].insert(0, v)
                    resp = f":{len(store[key])}\r\n"
                    conn.sendall(resp.encode())

            elif cmd == "RPOP":
                if len(parts) < 2:
                    conn.sendall(b"-ERR wrong number of arguments for 'rpop' command\r\n")
                else:
                    key = parts[1]
                    if key not in store or not isinstance(store[key], list) or not store[key]:
                        conn.sendall(b"$-1\r\n")
                    else:
                        value = store[key].pop()
                        resp = f"${len(value)}\r\n{value}\r\n"
                        conn.sendall(resp.encode())

            elif cmd == "BLPOP":
                if len(parts) != 3:
                    conn.sendall(b"-ERR wrong number of arguments for 'blpop' command\r\n")
                else:
                    key = parts[1]
                    try:
                        timeout = float(parts[2])
                    except ValueError:
                        conn.sendall(b"-ERR invalid timeout\r\n")
                        continue

                    end_time = None if timeout == 0 else time.time() + timeout

                    while True:
                        # If list exists and has elements
                        if key in store and isinstance(store[key], list) and store[key]:
                            value = store[key].pop(0)
                            resp = (
                                f"*2\r\n${len(key)}\r\n{key}\r\n"
                                f"${len(value)}\r\n{value}\r\n"
                            )
                            conn.sendall(resp.encode())
                            break

                        # Handle timeout expiration
                        if end_time is not None and time.time() >= end_time:
                            conn.sendall(b"$-1\r\n")
                            break

                        # Sleep briefly before retrying
                        time.sleep(0.05)

            else:
                conn.sendall(b"-ERR unknown command\r\n")

    conn.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("localhost", 6379))
    server.listen(5)
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()
