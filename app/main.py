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
    idx = 1
    for _ in range(n):
        if not lines[idx].startswith("$"):
            return None, buffer
        length = int(lines[idx][1:])
        part = lines[idx + 1]
        parts.append(part)
        idx += 2

    # compute bytes consumed
    consumed = 0
    for p in parts:
        consumed += len(p) + len(str(len(p))) + 5  # $len\r\np\r\n
    consumed += len(str(n)) + 3  # *n\r\n

    return parts, buffer[consumed:]


def handle_client(client):
    buffer = b""
    while True:
        try:
            data = client.recv(4096)
            if not data:
                break
            buffer += data

            while True:
                parts, buffer = parse_resp(buffer)
                if not parts:
                    break

                cmd = parts[0].upper()

                # ---------------- SET ----------------
                if cmd == "SET":
                    key, value = parts[1], parts[2]
                    expiry = None
                    if len(parts) > 4 and parts[3].upper() == "PX":
                        expiry = time.time() + int(parts[4]) / 1000.0
                    store[key] = (value, expiry)
                    client.sendall(b"+OK\r\n")

                # ---------------- GET ----------------
                elif cmd == "GET":
                    key = parts[1]
                    if key not in store:
                        client.sendall(b"$-1\r\n")
                    else:
                        val, expiry = store[key]
                        if expiry and time.time() > expiry:
                            del store[key]
                            client.sendall(b"$-1\r\n")
                        else:
                            resp = f"${len(val)}\r\n{val}\r\n"
                            client.sendall(resp.encode())

                # ---------------- RPUSH ----------------
                elif cmd == "RPUSH":
                    key = parts[1]
                    values = parts[2:]
                    if key not in store or not isinstance(store[key], list):
                        store[key] = []
                    store[key].extend(values)
                    resp = f":{len(store[key])}\r\n"
                    client.sendall(resp.encode())

                # ---------------- BLPOP ----------------
                elif cmd == "BLPOP":
                    if len(parts) < 3:
                        client.sendall(
                            b"-ERR wrong number of arguments for 'blpop' command\r\n"
                        )
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
                        # try popping from any key
                        for k in keys:
                            if k in store and isinstance(store[k], list) and store[k]:
                                popped = (k, store[k].pop(0))
                                break

                        if popped:
                            key, val = popped
                            response = (
                                f"*2\r\n${len(key)}\r\n{key}\r\n"
                                f"${len(val)}\r\n{val}\r\n"
                            )
                            client.sendall(response.encode())
                            break

                        # timeout expired → send NULL BULK STRING
                        if timeout != 0 and (time.time() - start_time) >= timeout:
                            client.sendall(b"$-1\r\n")
                            break

                        time.sleep(0.05)  # avoid busy loop

                # ---------------- UNKNOWN ----------------
                else:
                    client.sendall(b"-ERR unknown command\r\n")

        except Exception as e:
            print("Error:", e)
            break

    client.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("localhost", 6379))
    server.listen(5)
    print("Server listening on port 6379")

    while True:
        client, _ = server.accept()
        threading.Thread(target=handle_client, args=(client,), daemon=True).start()


if __name__ == "__main__":
    main()
