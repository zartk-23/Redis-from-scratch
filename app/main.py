def handle_client(connection):
    with connection:
        buffer = b""
        while True:
            chunk = connection.recv(1024)
            if not chunk:
                break  # client disconnected

            buffer += chunk

            # Keep processing commands as long as we have enough data
            while b"\r\n" in buffer:
                try:
                    parts = buffer.decode().split("\r\n")
                except UnicodeDecodeError:
                    break  # wait for more data

                if len(parts) < 3:
                    break  # not enough for a command

                command = parts[2].upper()

                if command == "PING":
                    connection.sendall(b"+PONG\r\n")
                    buffer = b""  # no args in PING, safe to clear fully

                elif command == "ECHO" and len(parts) >= 5:
                    message = parts[4]
                    resp = f"${len(message)}\r\n{message}\r\n"
                    connection.sendall(resp.encode())
                    buffer = b""

                elif command == "SET" and len(parts) >= 6:
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

                elif command == "GET" and len(parts) >= 4:
                    key = parts[4]
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

                else:
                    # If we don't understand the command, flush buffer to avoid loop
                    connection.sendall(b"-ERR unknown command\r\n")
                    buffer = b""
