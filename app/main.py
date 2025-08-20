import socket
import threading
import time

store = {}  # key -> (value, expiry_timestamp, list, or stream)
expiry = {}  # key -> expiry timestamp
blocking_clients = {}  # key -> [(conn, timeout_end_time)]


def generate_stream_id(stream_key, provided_id=None):
    """Generate a unique stream ID."""
    current_time_ms = int(time.time() * 1000)
    
    if provided_id and provided_id != "*":
        # Use provided ID (validate format later if needed)
        return provided_id
    
    # Auto-generate ID: timestamp-sequence
    if stream_key not in store or not isinstance(store[stream_key], dict):
        # First entry in stream
        return f"{current_time_ms}-0"
    
    stream = store[stream_key]
    if not stream.get('entries'):
        return f"{current_time_ms}-0"
    
    # Get the last ID to ensure uniqueness
    last_id = list(stream['entries'].keys())[-1]
    last_timestamp, last_seq = map(int, last_id.split('-'))
    
    if current_time_ms > last_timestamp:
        return f"{current_time_ms}-0"
    elif current_time_ms == last_timestamp:
        return f"{current_time_ms}-{last_seq + 1}"
    else:
        # Current time is behind last timestamp, increment sequence
        return f"{last_timestamp}-{last_seq + 1}"


def parse_resp(buffer):
    """Parse RESP messages."""
    if not buffer:
        return None, buffer

    if buffer.startswith(b"*"):
        lines = buffer.split(b"\r\n")
        if len(lines) < 1:
            return None, buffer
        
        try:
            n = int(lines[0][1:])
        except (ValueError, IndexError):
            return None, buffer
            
        parts = []
        idx = 1
        for _ in range(n):
            if idx >= len(lines) or not lines[idx].startswith(b"$"):
                return None, buffer
            try:
                length = int(lines[idx][1:])
                if idx + 1 >= len(lines):
                    return None, buffer
                parts.append(lines[idx + 1].decode())
                idx += 2
            except (ValueError, IndexError):
                return None, buffer
        return parts, b"\r\n".join(lines[idx:])
    else:
        try:
            parts = buffer.decode().strip().split()
            return parts, b""
        except UnicodeDecodeError:
            return None, buffer


def encode_resp(data):
    """Encode Python object to RESP format."""
    if data is None:
        return b"$-1\r\n"
    if isinstance(data, str):
        return f"${len(data)}\r\n{data}\r\n".encode()
    if isinstance(data, int):
        return f":{data}\r\n".encode()
    if isinstance(data, list):
        out = f"*{len(data)}\r\n"
        for item in data:
            if item is None:
                out += "$-1\r\n"
            else:
                out += f"${len(str(item))}\r\n{item}\r\n"
        return out.encode()
    return b"+OK\r\n"


def handle_command(conn, command_parts):
    if not command_parts:
        return

    cmd = command_parts[0].upper()

    # PING
    if cmd == "PING":
        conn.sendall(b"+PONG\r\n")

    # ECHO
    elif cmd == "ECHO" and len(command_parts) > 1:
        conn.sendall(encode_resp(command_parts[1]))

    # SET
    elif cmd == "SET":
        key, value = command_parts[1], command_parts[2]
        store[key] = value
        if len(command_parts) > 3 and command_parts[3].upper() == "PX":
            expiry[key] = time.time() + int(command_parts[4]) / 1000.0
        conn.sendall(b"+OK\r\n")

    # GET
    elif cmd == "GET":
        key = command_parts[1]
        if key in expiry and time.time() > expiry[key]:
            del store[key]
            del expiry[key]
            conn.sendall(b"$-1\r\n")
        elif key in store and isinstance(store[key], str):
            conn.sendall(encode_resp(store[key]))
        else:
            conn.sendall(b"$-1\r\n")

    # RPUSH
    elif cmd == "RPUSH":
        key = command_parts[1]
        values = command_parts[2:]
        if key not in store or not isinstance(store[key], list):
            store[key] = []
        store[key].extend(values)
        conn.sendall(encode_resp(len(store[key])))

    # LPUSH
    elif cmd == "LPUSH":
        key = command_parts[1]
        values = command_parts[2:]
        if key not in store or not isinstance(store[key], list):
            store[key] = []
        # Insert values one by one at the beginning
        for value in values:
            store[key].insert(0, value)
        conn.sendall(encode_resp(len(store[key])))

    # LPOP
    elif cmd == "LPOP":
        key = command_parts[1]
        count = int(command_parts[2]) if len(command_parts) > 2 else 1
        if key in store and isinstance(store[key], list) and store[key]:
            popped = []
            for _ in range(min(count, len(store[key]))):
                popped.append(store[key].pop(0))
            if count == 1:
                conn.sendall(encode_resp(popped[0]))
            else:
                conn.sendall(encode_resp(popped))
        else:
            conn.sendall(b"$-1\r\n")

    # BLPOP
    elif cmd == "BLPOP":
        keys = command_parts[1:-1]
        timeout = float(command_parts[-1])

        # Special case: timeout 0 means block indefinitely
        if timeout == 0:
            timeout = float('inf')
            
        end_time = time.time() + timeout

        while time.time() < end_time:
            for k in keys:
                if k in store and isinstance(store[k], list) and store[k]:
                    value = store[k].pop(0)
                    # Return array with key and value
                    conn.sendall(encode_resp([k, value]))
                    return
            time.sleep(0.01)  # Reduced sleep time for better responsiveness

        # Timeout reached, return null array
        conn.sendall(b"*-1\r\n")

    # LRANGE
    elif cmd == "LRANGE":
        key = command_parts[1]
        start = int(command_parts[2])
        stop = int(command_parts[3])
        
        if key not in store or not isinstance(store[key], list):
            # Return empty array if key doesn't exist or isn't a list
            conn.sendall(encode_resp([]))
        else:
            lst = store[key]
            # Handle negative indices
            if start < 0:
                start = len(lst) + start
            if stop < 0:
                stop = len(lst) + stop
            
            # Clamp indices to valid range
            start = max(0, start)
            stop = min(len(lst) - 1, stop)
            
            if start <= stop and start < len(lst):
                result = lst[start:stop + 1]
                conn.sendall(encode_resp(result))
            else:
                conn.sendall(encode_resp([]))

    # LLEN
    elif cmd == "LLEN":
        key = command_parts[1]
        if key not in store or not isinstance(store[key], list):
            # Return 0 if key doesn't exist or isn't a list
            conn.sendall(encode_resp(0))
        else:
            # Return the length of the list
            conn.sendall(encode_resp(len(store[key])))

    # TYPE
    elif cmd == "TYPE":
        key = command_parts[1]
        if key not in store:
            # Key doesn't exist
            conn.sendall(encode_resp("none"))
        elif isinstance(store[key], str):
            conn.sendall(encode_resp("string"))
        elif isinstance(store[key], list):
            conn.sendall(encode_resp("list"))
        elif isinstance(store[key], dict) and 'entries' in store[key]:
            conn.sendall(encode_resp("stream"))
        else:
            # For any other type
            conn.sendall(encode_resp("none"))

    # XADD
    elif cmd == "XADD":
        if len(command_parts) < 4:
            conn.sendall(b"-ERR wrong number of arguments\r\n")
            return
            
        key = command_parts[1]
        entry_id = command_parts[2]
        
        # Parse field-value pairs (must be even number of arguments after ID)
        field_value_pairs = command_parts[3:]
        if len(field_value_pairs) % 2 != 0:
            conn.sendall(b"-ERR wrong number of arguments\r\n")
            return
        
        # Create stream if it doesn't exist
        if key not in store or not isinstance(store[key], dict):
            store[key] = {'entries': {}}
        
        # Generate ID if needed
        if entry_id == "*":
            entry_id = generate_stream_id(key)
        else:
            # Validate and potentially adjust the provided ID
            entry_id = generate_stream_id(key, entry_id)
        
        # Build entry data
        entry_data = {}
        for i in range(0, len(field_value_pairs), 2):
            field = field_value_pairs[i]
            value = field_value_pairs[i + 1]
            entry_data[field] = value
        
        # Add entry to stream
        store[key]['entries'][entry_id] = entry_data
        
        # Return the generated/used ID
        conn.sendall(encode_resp(entry_id))

    else:
        conn.sendall(b"-ERR unknown command\r\n")


def client_thread(conn):
    buffer = b""
    while True:
        try:
            data = conn.recv(4096)
            if not data:
                break
            buffer += data
            while buffer:
                command_parts, buffer = parse_resp(buffer)
                if not command_parts:
                    break
                handle_command(conn, command_parts)
        except ConnectionResetError:
            break
    conn.close()


def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("localhost", 6379))
    s.listen()
    while True:
        conn, _ = s.accept()
        threading.Thread(target=client_thread, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()