import socket
import threading
import time

store = {}  # key -> (value, expiry_timestamp or list)


def handle_client(connection):
    with connection:
        buffer = b""
        while True:
            chunk = connection.recv(1024)
            if not chunk:
                break  # client disconnected
            buffer += chunk

            while b"\r\n" in buffer:
                try:
                    parts = [p for p in buffer.decode().split("\r\n") if p]  # remove empty strings
                except UnicodeDecodeError:
                    break

                if len(parts) < 2:
                    break  # incomplete command

                command = parts[0].upper() if parts else None

                # PING
                if command == "PING":
                    connection.sendall(b"+PONG\r\n")
                    buffer = b""
                    continue

                # ECHO
                if command == "ECHO" and len(parts) >= 2:
                    message = parts[1]
                    resp = f"${len(message)}\r\n{message}\r\n"
                    connection.sendall(resp.encode())
                    buffer = b""
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
                    buffer = b""
                    continue

                # GET
                if command == "GET" and len(parts) >= 2:
                    key = parts[1]
                    if key in store:
                        value, expiry = store[key]
                        if expiry and time.time() > expiry:
                            del store[key]
                            connection.sendall(b"$-1\r\n")
                        else:
                            resp = f"${len(value)}\r\n{value}\r\n"
                            connection.sendall(resp.encode())
                    else:
                        connection.sendall(b"$-1\r\n")
                    buffer = b""
                    continue

                # RPUSH
                if command == "RPUSH" and len(parts) >= 3:
                    key = parts[1]
                    value = parts[2]
                    if key not in store or not isinstance(store[key], list):
                        store[key] = []
                    store[key].append(value)
                    resp = f":{len(store[key])}\r\n"
                    connection.sendall(resp.encode())
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
