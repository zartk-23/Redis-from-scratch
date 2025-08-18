import socket
import threading
import time

store = {}  # key -> (value, expiry_timestamp or list)
block_queues = {}  # key -> list of (conn, timeout_deadline)

# RESP protocol helpers
def encode_resp(value):
    if isinstance(value, str):
        return f"${len(value)}\r\n{value}\r\n".encode()
    elif isinstance(value, list):
        resp = f"*{len(value)}\r\n"
        for v in value:
            resp += f"${len(v)}\r\n{v}\r\n"
        return resp.encode()
    elif value is None:
        return b"$-1\r\n"
    else:
        return f"+{value}\r\n".encode()

def parse_resp(buffer):
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
        data = lines[idx + 1]
        parts.append(data)
        idx += 2
    remaining = "\r\n".join(lines[idx:]).encode()
    return parts, remaining

# Command handlers
def handle_set(parts, conn):
    key, value = parts[1], parts[2]
    expiry = None
    if len(parts) > 3 and parts[3].upper() == "PX":
        expiry = time.time() + int(parts[4]) / 1000.0
    store[key] = (value, expiry)
    conn.sendall(b"+OK\r\n")

def handle_get(parts, conn):
    key = parts[1]
    if key not in store:
        conn.sendall(b"$-1\r\n")
        return
    value, expiry = store[key]
    if expiry and time.time() > expiry:
        del store[key]
        conn.sendall(b"$-1\r\n")
    else:
        conn.sendall(encode_resp(value))

def handle_del(parts, conn):
    count = 0
    for key in parts[1:]:
        if key in store:
            del store[key]
            count += 1
    conn.sendall(f":{count}\r\n".encode())

def handle_lpush(parts, conn):
    key = parts[1]
    values = parts[2:]
    if key not in store or not isinstance(store[key], list):
        store[key] = []
    store[key][0:0] = values  # prepend
    conn.sendall(f":{len(store[key])}\r\n".encode())

    # Unblock waiting clients if any
    if key in block_queues and store[key]:
        while block_queues[key] and store[key]:
            client_conn, deadline = block_queues[key].pop(0)
            val = store[key].pop(0)
            client_conn.sendall(encode_resp([key, val]))


def handle_lpop(parts, conn):
    key = parts[1]
    count = 1
    if len(parts) > 2:
        try:
            count = int(parts[2])
        except ValueError:
            conn.sendall(b"-ERR value is not an integer or out of range\r\n")
            return

    if key not in store or not isinstance(store[key], list) or not store[key]:
        conn.sendall(b"$-1\r\n")
        return

    popped = []
    for _ in range(count):
        if store[key]:
            popped.append(store[key].pop(0))
        else:
            break

    if count == 1:
        conn.sendall(encode_resp(popped[0] if popped else None))
    else:
        conn.sendall(encode_resp(popped))


def handle_blpop(parts, conn):
    keys = parts[1:-1]
    timeout = float(parts[-1])

    # Try popping immediately
    for key in keys:
        if key in store and isinstance(store[key], list) and store[key]:
            val = store[key].pop(0)
            conn.sendall(encode_resp([key, val]))
            return

    # Otherwise, block
    deadline = time.time() + timeout if timeout > 0 else None
    for key in keys:
        if key not in block_queues:
            block_queues[key] = []
        block_queues[key].append((conn, deadline))


def client_thread(conn, addr):
    buffer = b""
    while True:
        try:
            data = conn.recv(4096)
            if not data:
                break
            buffer += data
            while True:
                parts, buffer = parse_resp(buffer)
                if not parts:
                    break
                cmd = parts[0].upper()
                if cmd == "PING":
                    conn.sendall(b"+PONG\r\n")
                elif cmd == "ECHO":
                    conn.sendall(encode_resp(parts[1]))
                elif cmd == "SET":
                    handle_set(parts, conn)
                elif cmd == "GET":
                    handle_get(parts, conn)
                elif cmd == "DEL":
                    handle_del(parts, conn)
                elif cmd == "LPUSH":
                    handle_lpush(parts, conn)
                elif cmd == "LPOP":
                    handle_lpop(parts, conn)
                elif cmd == "BLPOP":
                    handle_blpop(parts, conn)
                else:
                    conn.sendall(b"-ERR unknown command\r\n")
        except ConnectionResetError:
            break
    conn.close()

def expiry_cleaner():
    while True:
        now = time.time()
        expired_keys = [k for k, (v, e) in store.items() if isinstance(v, str) and e and now > e]
        for k in expired_keys:
            del store[k]

        # Handle blocking timeouts
        for key in list(block_queues.keys()):
            new_waiters = []
            for client_conn, deadline in block_queues[key]:
                if deadline and time.time() > deadline:
                    client_conn.sendall(b"$-1\r\n")
                else:
                    new_waiters.append((client_conn, deadline))
            block_queues[key] = new_waiters

        time.sleep(0.1)

def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", 6379))
    server.listen()
    threading.Thread(target=expiry_cleaner, daemon=True).start()
    while True:
        conn, addr = server.accept()
        threading.Thread(target=client_thread, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    main()
