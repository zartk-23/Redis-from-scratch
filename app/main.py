import socket
import threading
import time

store = {}  # key -> (value, expiry_timestamp) OR list of values

def handle_client(connection):
    with connection:
        buffer = b""
        while True:
            chunk = connection.recv(1024)
            if not chunk:
                break  # client disconnected
            buffer += chunk

            # Only process if we have a full command
            while b"\r\n" in buffer:
                try:
                    parts = buffer.decode().split("\r\n")
                except UnicodeDecodeError:
                    break  # wait for more data

                if len(parts) < 3:
                    break  # incomplete

                command = parts[2].upper()

                # PING
                if command == "PING":
                    connection.sendall(b"+PONG\r\n")
                    buffer = b""
                    continue

                # ECHO
                if command == "ECHO" and len(parts) >= 5:
                    message = parts[4]
                    resp = f"${len(message)}\r\n{message}\r\n"
                    connection.sendall(resp.encode())
                    buffer = b""
                    continue

                # SET (with optional PX expiry)
                if command == "SET" and len(parts) >= 6:
                    key = parts[4]
                    value = parts[6]
                    expiry_timestamp = None
                    if len(parts) >= 10 and parts[8].upper() == "PX":
                        try:
                            px_value = int(parts[10])
                            expiry_timestamp = time.time() + (px_value / 1000.0)
                        except ValueError:
                            pass
                    store[key] = (value, expiry_timestamp)
                    connection.sendall(b"+OK\r\n")
                    buffer = b""
                    continue

                # GET
                if command == "GET" and len(parts) >= 4:
                    key = parts[4]
                    if key in store:
                        value = store[key]
                        if isinstance(value, tuple):
                            value, expiry = value
                            if expiry and time.time() > expiry:
                                del store[key]
                                connection.sendall(b"$-1\r\n")
                                buffer = b""
                                continue
                        if isinstance(value, str):
                            resp = f"${len(value)}\r\n{value}\r\n"
                            connection.sendall(resp.encode())
                        else:  # trying GET on a list
                            connection.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                    else:
                        connection.sendall(b"$-1\r\n")
                    buffer = b""
                    continue

                # RPUSH (create list with single element if not exists)
                if command == "RPUSH" and len(parts) >= 6:
                    key = parts[4]
                    value = parts[6]

                    if key not in store:
                        store[key] = []  # create new list

                    if isinstance(store[key], list):
                        store[key].append(value)
                        connection.sendall(f":{len(store[key])}\r\n".encode())
                    else:
                        connection.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

                    buffer = b""
                    continue

                # Unknown command
                connection.sendall(b"-ERR unknown command\r\n")
                buffer = b""
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
