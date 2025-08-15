import socket
import threading
import time

store={}

def handle_client(connection):
    with connection:
        buffer=b""
        while True:
            data = connection.recv(1024)
            if not data:
                break  # client disconnected

            buffer += data    

            while b"\r\n" in buffer:
                try:
                    parts = buffer.decode().strip().split("\r\n")
                except UnicodeDecodeError:
                    break  # skip invalid input

                # RESP array format: ["*2", "$4", "ECHO", "$5", "hello"]
                if len(parts) >= 3:
                    break

                command = parts[2].upper()

                if command == "PING":
                    connection.sendall(b"+PONG\r\n")
                    buffer=b""

                elif command == "ECHO" and len(parts) >= 5:
                    message = parts[4]
                    resp = f"${len(message)}\r\n{message}\r\n"
                    connection.sendall(resp.encode())
                    buffer=b""

                elif command == "SET" and len(parts) >= 6:
                    key = parts[4]
                    value = parts[6]
                    expiry_timestamp = None

                    if len(parts) >= 10 and parts[8].upper() == "PX":
                        try:
                            px_value = int(parts[10])
                            expiry_timestamp = time.time() + (px_value / 1000.0)
                        except ValueError:
                            pass  # ignore invalid PX value

                    store[key] = (value, expiry_timestamp)
                    connection.sendall(b"+OK\r\n")
                    buffer=b""
                
                elif command == "GET" and len(parts) >= 4:
                    key = parts[4]
                    if key in store:
                        value, expiry = store[key]
                        if expiry is not None and time.time() > expiry:
                            # Key expired → remove and return null bulk string
                            del store[key]
                            connection.sendall(b"$-1\r\n")
                        else:
                            resp = f"${len(value)}\r\n{value}\r\n"
                            connection.sendall(resp.encode())
                    else:
                        connection.sendall(b"$-1\r\n")
                    buffer=b""  # null bulk string

                else:
                    connection.sendall(b"-ERR unknown command\r\n")
                buffer-b""
                
def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379))
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    while True:
        connection, _ = server_socket.accept()
        threading.Thread(
            target=handle_client,
            args=(connection,),
            daemon=True
        ).start()

if __name__ == "__main__":
    main()
