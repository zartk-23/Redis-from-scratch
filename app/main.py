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

    lines = decoded.split("\r\n")
    if not lines or lines[0] == "":
        return None, buffer

    # Handle RESP array
    if lines[0].startswith("*"):
        try:
            num_args = int(lines[0][1:])
        except ValueError:
            return None, buffer

        parts = []
        idx = 1  # Start after *n
        for _ in range(num_args):
            if idx >= len(lines) or not lines[idx].startswith("$"):
                return None, buffer  # Incomplete bulk string
            try:
                str_len = int(lines[idx][1:])
                if idx + 1 >= len(lines) or len(lines[idx + 1]) != str_len:
                    return None, buffer  # Incomplete or wrong length
                parts.append(lines[idx + 1])
                idx += 2
            except ValueError:
                return None, buffer

        # Return parsed parts and remaining buffer
        remaining = "\r\n".join(lines[idx:]).encode()
        return parts, remaining

    # Fallback for simple commands
    parts = [p for p in decoded.split("\r\n") if p]
    return parts, b""


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

                # PING
                if command == "PING":
                    connection.sendall(b"+PONG\r\n")
                    continue

                # ECHO
                if command == "ECHO" and len(parts) >= 2:
                    message = parts[1]
                    resp = f"${len(message)}\r\n{message}\r\n"
                    connection.sendall(resp.encode())
                    continue

                # SET (with optional PX expiry)
                if command == "SET" and len(parts) >= 3:
                    key = parts[1]
                    value = parts[2]
                    expiry_timestamp = None
                    if len(parts) >= 5 and parts[3].upper() == "PX":
                        try:
                            px_value = int(parts[4])
                            expiry_timestamp = time.time() + (px_value / 1000.0)
                        except ValueError:
                            pass
                    store[key] = (value, expiry_timestamp)
                    connection.sendall(b"+OK\r\n")
                    continue

                # GET
                if command == "GET" and len(parts) >= 2:
                    key = parts[1]
                    if key in store and isinstance(store[key], tuple):
                        value, expiry = store[key]
                        if expiry and time.time() > expiry:
                            del store[key]
                            connection.sendall(b"$-1\r\n")
                        else:
                            resp = f"${len(value)}\r\n{value}\r\n"
                            connection.sendall(resp.encode())
                    else:
                        connection.sendall(b"$-1\r\n")
                    continue

                # RPUSH
                if command == "RPUSH" and len(parts) >= 3:
                    key = parts[1]
                    values = parts[2:]  # Support multiple values
                    if key not in store or not isinstance(store[key], list):
                        store[key] = []
                    store[key].extend(values)
                    resp = f":{len(store[key])}\r\n"
                    connection.sendall(resp.encode())
                    continue

                # LRANGE (with negative index support)
                if command == "LRANGE" and len(parts) == 4:
                    key = parts[1]
                    start = int(parts[2])
                    end = int(parts[3])

                    if key not in store or not isinstance(store[key], list):
                        connection.sendall(b"*0\r\n")
                        continue

                    lst = store[key]
                    n = len(lst)

                    # Handle negative indexes
                    if start < 0:
                        start = n + start
                    if end < 0:
                        end = n + end

                    # Clamp indexes
                    if start < 0:
                        start = 0
                    if end < 0:
                        end = 0
                    if end >= n:
                        end = n - 1
                    if start >= n or start > end:
                        connection.sendall(b"*0\r\n")
                        continue

                    elements = lst[start:end+1]

                    # RESP array response
                    resp = f"*{len(elements)}\r\n"
                    for el in elements:
                        resp += f"${len(el)}\r\n{el}\r\n"
                    connection.sendall(resp.encode())
                    continue

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
