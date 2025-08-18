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
        num_elems = int(lines[0][1:])
    except ValueError:
        return None, buffer

    parts = []
    i = 1
    for _ in range(num_elems):
        if i >= len(lines) or not lines[i].startswith("$"):
            return None, buffer
        try:
            str_len = int(lines[i][1:])
        except ValueError:
            return None, buffer
        if i + 1 >= len(lines):
            return None, buffer
        data = lines[i + 1]
        if len(data) != str_len:
            return None, buffer
        parts.append(data)
        i += 2

    resp_len = 1 + num_elems * 2
    resp_str = "\r\n".join(lines[:resp_len]) + "\r\n"
    remaining = buffer[len(resp_str.encode()):]
    return parts, remaining


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
                parts, buffer = parse_resp(buffer)
                if parts is None:
                    break

                cmd = parts[0].upper()

                if cmd == "PING":
                    conn.sendall(b"+PONG\r\n")

                elif cmd == "ECHO" and len(parts) > 1:
                    msg = parts[1]
                    resp = f"${len(msg)}\r\n{msg}\r\n"
                    conn.sendall(resp.encode())

                elif cmd == "SET":
                    key = parts[1]
                    value = parts[2]
                    expiry = None
                    if len(parts) > 3 and parts[3].upper() == "PX" and len(parts) > 4:
                        try:
                            expiry_ms = int(parts[4])
                            expiry = time.time() + expiry_ms / 1000.0
                        except ValueError:
                            pass
                    store[key] = (value, expiry)
                    conn.sendall(b"+OK\r\n")

                elif cmd == "GET":
                    key = parts[1]
                    if key in store:
                        val, expiry = store[key]
                        if expiry and time.time() > expiry:
                            del store[key]
                            conn.sendall(b"$-1\r\n")
                        else:
                            resp = f"${len(val)}\r\n{val}\r\n"
                            conn.sendall(resp.encode())
                    else:
                        conn.sendall(b"$-1\r\n")

                elif cmd == "DEL":
                    deleted = 0
                    for key in parts[1:]:
                        if key in store:
                            del store[key]
                            deleted += 1
                    conn.sendall(f":{deleted}\r\n".encode())

                elif cmd == "LPOP":
                    key = parts[1]
                    if key in store and isinstance(store[key], list) and store[key]:
                        value = store[key].pop(0)
                        resp = f"${len(value)}\r\n{value}\r\n"
                        conn.sendall(resp.encode())
                    else:
                        conn.sendall(b"$-1\r\n")

                elif cmd == "BLPOP":
                    if len(parts) < 3:
                        conn.sendall(b"-ERR wrong number of arguments for 'blpop' command\r\n")
                        continue

                    keys = parts[1:-1]
                    try:
                        timeout = float(parts[-1])
                    except ValueError:
                        conn.sendall(b"-ERR timeout is not a float or integer\r\n")
                        continue

                    popped = None
                    start_time = time.time()

                    while True:
                        # Check lists for available items
                        for key in keys:
                            if key in store and isinstance(store[key], list) and store[key]:
                                value = store[key].pop(0)
                                popped = [key, value]
                                break
                        if popped:
                            resp = (
                                f"*2\r\n"
                                f"${len(popped[0])}\r\n{popped[0]}\r\n"
                                f"${len(popped[1])}\r\n{popped[1]}\r\n"
                            )
                            conn.sendall(resp.encode())
                            break

                        # If timeout == 0 → block forever
                        if timeout == 0:
                            time.sleep(0.1)
                            continue

                        # Non-zero timeout → check elapsed
                        if time.time() - start_time >= timeout:
                            conn.sendall(b"$-1\r\n")  # Null bulk string
                            break

                        time.sleep(0.05)

                else:
                    conn.sendall(b"-ERR unknown command\r\n")

        except ConnectionResetError:
            break
    conn.close()


def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("localhost", 6379))
        s.listen()
        print("Server listening on port 6379")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()
