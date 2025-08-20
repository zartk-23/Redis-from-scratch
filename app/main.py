import socket
import threading
import time

store = {}  # key -> (value, expiry_timestamp, list, or stream)
expiry = {}  # key -> expiry timestamp
blocking_clients = {}  # key -> [(conn, timeout_end_time)]
blocking_clients_lock = threading.Lock()  # Lock for thread-safe access to blocking_clients
client_transactions = {}  # conn -> list of queued commands


def generate_stream_id(stream_key, provided_id=None):
    """Generate a unique stream ID."""
    current_time_ms = int(time.time() * 1000)
    
    if provided_id and provided_id != "*":
        # Use provided ID (validate format later if needed)
        return provided_id
    
    # Auto-generate full ID using current timestamp
    if (stream_key not in store or 
        not isinstance(store[stream_key], dict) or 
        not store[stream_key].get('entries')):
        # First entry in stream - use current time with sequence 0
        return f"{current_time_ms}-0"
    
    stream = store[stream_key]
    
    # Check if current timestamp already exists in stream
    max_seq_for_current_time = -1
    for entry_id in stream['entries']:
        entry_timestamp, entry_seq = map(int, entry_id.split('-'))
        if entry_timestamp == current_time_ms and entry_seq > max_seq_for_current_time:
            max_seq_for_current_time = entry_seq
    
    if max_seq_for_current_time >= 0:
        # Current timestamp exists, increment sequence
        return f"{current_time_ms}-{max_seq_for_current_time + 1}"
    else:
        # Current timestamp doesn't exist, use sequence 0
        # But make sure the ID is greater than the last entry
        last_id = list(stream['entries'].keys())[-1]
        last_timestamp, last_seq = map(int, last_id.split('-'))
        
        if current_time_ms > last_timestamp:
            return f"{current_time_ms}-0"
        elif current_time_ms == last_timestamp:
            return f"{current_time_ms}-{last_seq + 1}"
        else:
            # Current time is behind last timestamp, use last timestamp with incremented sequence
            return f"{last_timestamp}-{last_seq + 1}"


def generate_sequence_number(stream_key, timestamp):
    """Generate sequence number for a given timestamp."""
    # Special case: if timestamp is 0, start with sequence 1
    if timestamp == 0:
        if (stream_key not in store or 
            not isinstance(store[stream_key], dict) or 
            not store[stream_key].get('entries')):
            return 1
        
        # Find the highest sequence number for timestamp 0
        stream = store[stream_key]
        max_seq = 0
        for entry_id in stream['entries']:
            entry_timestamp, entry_seq = map(int, entry_id.split('-'))
            if entry_timestamp == 0 and entry_seq > max_seq:
                max_seq = entry_seq
        return max_seq + 1
    
    # For non-zero timestamps, start with sequence 0
    if (stream_key not in store or 
        not isinstance(store[stream_key], dict) or 
        not store[stream_key].get('entries')):
        return 0
    
    # Find the highest sequence number for this timestamp
    stream = store[stream_key]
    max_seq = -1  # Start with -1 so first entry gets sequence 0
    for entry_id in stream['entries']:
        entry_timestamp, entry_seq = map(int, entry_id.split('-'))
        if entry_timestamp == timestamp and entry_seq > max_seq:
            max_seq = entry_seq
    
    return max_seq + 1


def validate_stream_id(stream_key, entry_id):
    """Validate that the entry ID is greater than the last entry ID."""
    try:
        # Handle timestamp-* format
        if entry_id.endswith('-*'):
            timestamp_str = entry_id[:-2]  # Remove '-*'
            timestamp = int(timestamp_str)
            # Generate the sequence number
            sequence = generate_sequence_number(stream_key, timestamp)
            final_id = f"{timestamp}-{sequence}"
            
            # Validate the final ID
            return validate_final_id(stream_key, final_id), final_id
        else:
            # Parse the provided explicit ID
            timestamp_str, seq_str = entry_id.split('-')
            timestamp = int(timestamp_str)
            sequence = int(seq_str)
            return validate_final_id(stream_key, entry_id), entry_id
    except (ValueError, IndexError):
        return False, "Invalid ID format"


def compare_stream_ids(id1, id2):
    """Compare two stream IDs. Returns -1 if id1 < id2, 0 if equal, 1 if id1 > id2."""
    timestamp1, seq1 = map(int, id1.split('-'))
    timestamp2, seq2 = map(int, id2.split('-'))
    
    if timestamp1 < timestamp2:
        return -1
    elif timestamp1 > timestamp2:
        return 1
    else:
        # Same timestamp, compare sequence
        if seq1 < seq2:
            return -1
        elif seq1 > seq2:
            return 1
        else:
            return 0


def normalize_range_id(range_id, is_start=True):
    """Normalize range IDs for XRANGE command."""
    if range_id == "-":
        # Minimum possible ID
        return "0-0"
    elif range_id == "+":
        # Maximum possible ID (we'll handle this specially)
        return "+"
    elif '-' not in range_id:
        # Just timestamp provided, add appropriate sequence
        if is_start:
            return f"{range_id}-0"
        else:
            # For end range, we want to include all sequences for this timestamp
            # We'll use a very large sequence number
            return f"{range_id}-18446744073709551615"  # Max uint64
    else:
        # Full ID provided
        return range_id


def validate_final_id(stream_key, entry_id):
    """Validate that the final entry ID is greater than the last entry ID."""
    try:
        timestamp_str, seq_str = entry_id.split('-')
        timestamp = int(timestamp_str)
        sequence = int(seq_str)
    except (ValueError, IndexError):
        return False
    
    # Check if ID is greater than 0-0 (minimum valid ID)
    if timestamp == 0 and sequence == 0:
        return False
    
    # If stream doesn't exist or is empty, any ID > 0-0 is valid
    if (stream_key not in store or 
        not isinstance(store[stream_key], dict) or 
        not store[stream_key].get('entries')):
        return True
    
    # Get the last entry ID
    stream = store[stream_key]
    last_id = list(stream['entries'].keys())[-1]
    last_timestamp, last_sequence = map(int, last_id.split('-'))
    
    # Validate that new ID is greater than last ID
    if timestamp > last_timestamp:
        return True
    elif timestamp == last_timestamp:
        if sequence > last_sequence:
            return True
        else:
            return False
    else:
        return False


def notify_blocking_clients(stream_key):
    """Notify all blocking clients waiting on a specific stream."""
    with blocking_clients_lock:
        if stream_key in blocking_clients:
            for client_info in blocking_clients[stream_key][:]:  # Copy list to avoid modification during iteration
                conn, stream_keys, stream_ids, timeout_end = client_info
                try:
                    # Check if we have new entries for this client
                    result = []
                    for i, key in enumerate(stream_keys):
                        if key == stream_key:
                            start_id = stream_ids[i]
                            
                            # Check if stream exists and has new entries
                            if (key in store and 
                                isinstance(store[key], dict) and 
                                store[key].get('entries')):
                                
                                stream = store[key]
                                entries = stream['entries']
                                
                                # Find entries after the specified start_id
                                stream_entries = []
                                for entry_id in entries:
                                    if compare_stream_ids(entry_id, start_id) > 0:
                                        # Format entry data as [field1, value1, field2, value2, ...]
                                        entry_data = entries[entry_id]
                                        field_value_list = []
                                        for field, value in entry_data.items():
                                            field_value_list.extend([field, value])
                                        stream_entries.append([entry_id, field_value_list])
                                
                                # Only include streams that have entries
                                if stream_entries:
                                    result.append([key, stream_entries])
                    
                    if result:
                        # Send result to client and remove from blocking list
                        conn.sendall(encode_resp(result))
                        blocking_clients[stream_key].remove(client_info)
                        if not blocking_clients[stream_key]:
                            del blocking_clients[stream_key]
                        
                except Exception:
                    # Remove client if there's an error (connection closed, etc.)
                    if client_info in blocking_clients[stream_key]:
                        blocking_clients[stream_key].remove(client_info)
                    if not blocking_clients[stream_key]:
                        del blocking_clients[stream_key]


def cleanup_expired_blocking_clients():
    """Remove expired blocking clients and send timeout responses."""
    while True:
        current_time = time.time()
        with blocking_clients_lock:
            for stream_key in list(blocking_clients.keys()):
                for client_info in blocking_clients[stream_key][:]:  # Copy to avoid modification during iteration
                    conn, stream_keys, stream_ids, timeout_end = client_info
                    # Only timeout clients with finite timeout (not infinite)
                    if timeout_end != float('inf') and current_time >= timeout_end:
                        try:
                            # Send null response for timeout
                            conn.sendall(b"$-1\r\n")
                        except Exception:
                            pass  # Client connection might be closed
                        
                        # Remove client from blocking list
                        blocking_clients[stream_key].remove(client_info)
                        if not blocking_clients[stream_key]:
                            del blocking_clients[stream_key]
        
        time.sleep(0.1)  # Check every 100ms


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
            elif isinstance(item, str):
                out += f"${len(item)}\r\n{item}\r\n"
            elif isinstance(item, int):
                out += f":{item}\r\n"
            elif isinstance(item, list):
                # Recursively encode nested arrays
                nested = encode_resp(item).decode()
                out += nested
            else:
                # Convert to string if unknown type
                item_str = str(item)
                out += f"${len(item_str)}\r\n{item_str}\r\n"
        return out.encode()
    return b"+OK\r\n"


def execute_single_command(command_parts):
    """Execute a single command and return the response as a Python object."""
    if not command_parts:
        return None

    cmd = command_parts[0].upper()

    # SET
    if cmd == "SET":
        key, value = command_parts[1], command_parts[2]
        store[key] = value
        if len(command_parts) > 3 and command_parts[3].upper() == "PX":
            expiry[key] = time.time() + int(command_parts[4]) / 1000.0
        return "OK"

    # GET
    elif cmd == "GET":
        key = command_parts[1]
        if key in expiry and time.time() > expiry[key]:
            del store[key]
            del expiry[key]
            return None
        elif key in store and isinstance(store[key], str):
            return store[key]
        else:
            return None

    # INCR
    elif cmd == "INCR":
        key = command_parts[1]
        
        # Check if key exists and is expired
        if key in expiry and time.time() > expiry[key]:
            del store[key]
            del expiry[key]
        
        if key in store:
            # Key exists - check if it's a string type
            if isinstance(store[key], str):
                try:
                    # Try to convert the value to an integer
                    current_value = int(store[key])
                    # Increment by 1
                    new_value = current_value + 1
                    # Store the new value as a string
                    store[key] = str(new_value)
                    # Return the new value as an integer
                    return new_value
                except ValueError:
                    # Value is not a valid integer
                    raise ValueError("ERR value is not an integer or out of range")
            else:
                # Key exists but is not a string (could be list, stream, etc.)
                raise ValueError("ERR WRONGTYPE Operation against a key holding the wrong kind of value")
        else:
            # Key doesn't exist - treat as if value was 0, then increment to 1
            new_value = 1
            store[key] = str(new_value)
            return new_value

    # Add other commands as needed
    else:
        raise ValueError("ERR unknown command")


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

    # MULTI
    elif cmd == "MULTI":
        # Check if client is already in transaction
        if conn in client_transactions:
            conn.sendall(b"-ERR MULTI calls can not be nested\r\n")
        else:
            # Start a new transaction for this client
            client_transactions[conn] = []
            conn.sendall(b"+OK\r\n")

    # EXEC
    elif cmd == "EXEC":
        # Check if client is in transaction mode
        if conn not in client_transactions:
            conn.sendall(b"-ERR EXEC without MULTI\r\n")
        else:
            # Get the queued commands for this client
            queued_commands = client_transactions[conn]
            
            # Execute all queued commands and collect responses
            responses = []
            for command in queued_commands:
                try:
                    # Execute the command and get the response
                    response = execute_single_command(command)
                    responses.append(response)
                except ValueError as e:
                    # Handle errors by adding error string to responses
                    error_msg = str(e)
                    if error_msg.startswith("ERR "):
                        responses.append(error_msg)
                    else:
                        responses.append("ERR " + error_msg)
                except Exception:
                    # Handle unexpected errors
                    responses.append("ERR server error")
            
            # Send the array of responses
            conn.sendall(encode_resp(responses))
            
            # End the transaction by removing client from transaction state
            del client_transactions[conn]

    # DISCARD
    elif cmd == "DISCARD":
        # Check if client is in transaction mode
        if conn not in client_transactions:
            conn.sendall(b"-ERR DISCARD without MULTI\r\n")
        else:
            # Discard the transaction by removing client from transaction state
            del client_transactions[conn]
            # Return OK to indicate successful discard
            conn.sendall(b"+OK\r\n")

    # SET
    elif cmd == "SET":
        if conn in client_transactions:
            # Queue the command in transaction mode
            client_transactions[conn].append(command_parts)
            conn.sendall(b"+QUEUED\r\n")
        else:
            # Execute immediately in normal mode
            key, value = command_parts[1], command_parts[2]
            store[key] = value
            if len(command_parts) > 3 and command_parts[3].upper() == "PX":
                expiry[key] = time.time() + int(command_parts[4]) / 1000.0
            conn.sendall(b"+OK\r\n")

    # GET
    elif cmd == "GET":
        if conn in client_transactions:
            # Queue the command in transaction mode
            client_transactions[conn].append(command_parts)
            conn.sendall(b"+QUEUED\r\n")
        else:
            # Execute immediately in normal mode
            key = command_parts[1]
            if key in expiry and time.time() > expiry[key]:
                del store[key]
                del expiry[key]
                conn.sendall(b"$-1\r\n")
            elif key in store and isinstance(store[key], str):
                conn.sendall(encode_resp(store[key]))
            else:
                conn.sendall(b"$-1\r\n")

    # INCR
    elif cmd == "INCR":
        if conn in client_transactions:
            # Queue the command in transaction mode
            client_transactions[conn].append(command_parts)
            conn.sendall(b"+QUEUED\r\n")
        else:
            # Execute immediately in normal mode
            key = command_parts[1]
            
            # Check if key exists and is expired
            if key in expiry and time.time() > expiry[key]:
                del store[key]
                del expiry[key]
            
            if key in store:
                # Key exists - check if it's a string type
                if isinstance(store[key], str):
                    try:
                        # Try to convert the value to an integer
                        current_value = int(store[key])
                        # Increment by 1
                        new_value = current_value + 1
                        # Store the new value as a string
                        store[key] = str(new_value)
                        # Return the new value as an integer
                        conn.sendall(encode_resp(new_value))
                    except ValueError:
                        # Value is not a valid integer
                        conn.sendall(b"-ERR value is not an integer or out of range\r\n")
                else:
                    # Key exists but is not a string (could be list, stream, etc.)
                    conn.sendall(b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
            else:
                # Key doesn't exist - treat as if value was 0, then increment to 1
                new_value = 1
                store[key] = str(new_value)
                conn.sendall(encode_resp(new_value))

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
        
        # Handle different ID formats
        if entry_id == "*":
            # Auto-generate full ID (timestamp and sequence)
            entry_id = generate_stream_id(key)
        elif entry_id.endswith('-*'):
            # Auto-generate sequence number only
            is_valid, final_id = validate_stream_id(key, entry_id)
            if not is_valid:
                if final_id.split('-')[0] == '0' and final_id.split('-')[1] == '0':
                    conn.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                else:
                    conn.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                return
            entry_id = final_id
        else:
            # Explicit ID - validate it  
            is_valid, final_id = validate_stream_id(key, entry_id)
            if not is_valid:
                if entry_id == '0-0':
                    conn.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                else:
                    conn.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                return
        
        # Build entry data
        entry_data = {}
        for i in range(0, len(field_value_pairs), 2):
            field = field_value_pairs[i]
            value = field_value_pairs[i + 1]
            entry_data[field] = value
        
        # Add entry to stream
        store[key]['entries'][entry_id] = entry_data
        
        # Notify blocking clients waiting on this stream
        notify_blocking_clients(key)
        
        # Return the generated/used ID
        conn.sendall(encode_resp(entry_id))

    # XRANGE
    elif cmd == "XRANGE":
        if len(command_parts) < 4:
            conn.sendall(b"-ERR wrong number of arguments\r\n")
            return
            
        key = command_parts[1]
        start_id = command_parts[2]
        end_id = command_parts[3]
        
        # Check if stream exists
        if (key not in store or 
            not isinstance(store[key], dict) or 
            not store[key].get('entries')):
            # Return empty array for non-existent stream
            conn.sendall(encode_resp([]))
            return
        
        stream = store[key]
        entries = stream['entries']
        
        # Normalize range IDs
        normalized_start = normalize_range_id(start_id, is_start=True)
        normalized_end = normalize_range_id(end_id, is_start=False)
        
        # Filter entries within range
        result = []
        for entry_id in entries:
            # Check if entry_id is within range
            if normalized_end == "+":
                # End is maximum, only check start
                if compare_stream_ids(entry_id, normalized_start) >= 0:
                    # Format entry data as [field1, value1, field2, value2, ...]
                    entry_data = entries[entry_id]
                    field_value_list = []
                    for field, value in entry_data.items():
                        field_value_list.extend([field, value])
                    result.append([entry_id, field_value_list])
            else:
                # Check both start and end bounds
                if (compare_stream_ids(entry_id, normalized_start) >= 0 and 
                    compare_stream_ids(entry_id, normalized_end) <= 0):
                    # Format entry data as [field1, value1, field2, value2, ...]
                    entry_data = entries[entry_id]
                    field_value_list = []
                    for field, value in entry_data.items():
                        field_value_list.extend([field, value])
                    result.append([entry_id, field_value_list])
        
        conn.sendall(encode_resp(result))

    # XREAD
    elif cmd == "XREAD":
        if len(command_parts) < 4:
            conn.sendall(b"-ERR wrong number of arguments\r\n")
            return
        
        # Parse optional BLOCK parameter
        block_timeout = None
        args_start_index = 1
        
        if len(command_parts) > 1 and command_parts[1].upper() == "BLOCK":
            if len(command_parts) < 6:  # Need at least XREAD BLOCK timeout STREAMS key id
                conn.sendall(b"-ERR wrong number of arguments\r\n")
                return
            try:
                block_timeout = int(command_parts[2]) / 1000.0  # Convert ms to seconds
                if block_timeout == 0:
                    block_timeout = float('inf')  # 0 means block indefinitely
                args_start_index = 3
            except ValueError:
                conn.sendall(b"-ERR timeout is not an integer or out of range\r\n")
                return
        
        # Find "streams" keyword
        streams_index = -1
        for i in range(args_start_index, len(command_parts)):
            if command_parts[i].upper() == "STREAMS":
                streams_index = i
                break
        
        if streams_index == -1:
            conn.sendall(b"-ERR syntax error\r\n")
            return
        
        # Parse stream keys and IDs
        remaining_args = command_parts[streams_index + 1:]
        if len(remaining_args) % 2 != 0:
            conn.sendall(b"-ERR wrong number of arguments\r\n")
            return
        
        num_streams = len(remaining_args) // 2
        stream_keys = remaining_args[:num_streams]
        stream_ids = remaining_args[num_streams:]
        
        # Process each stream to get immediate results
        result = []
        processed_stream_ids = []  # Store the actual IDs used for comparison
        
        for i in range(num_streams):
            stream_key = stream_keys[i]
            start_id = stream_ids[i]
            
            # Handle special '$' ID - means "only new entries"
            if start_id == '$':
                # Check if stream exists and get the latest ID
                if (stream_key in store and 
                    isinstance(store[stream_key], dict) and 
                    store[stream_key].get('entries')):
                    stream = store[stream_key]
                    entries = stream['entries']
                    # Get the maximum (latest) ID in the stream
                    latest_id = max(entries.keys(), key=lambda x: (int(x.split('-')[0]), int(x.split('-')[1])))
                    actual_start_id = latest_id
                else:
                    # Stream doesn't exist, use 0-0 so any new entry will be greater
                    actual_start_id = "0-0"
            else:
                actual_start_id = start_id
            
            processed_stream_ids.append(actual_start_id)
            
            # Check if stream exists
            if (stream_key not in store or 
                not isinstance(store[stream_key], dict) or 
                not store[stream_key].get('entries')):
                continue
            
            stream = store[stream_key]
            entries = stream['entries']
            
            # Find entries after the specified start_id
            stream_entries = []
            for entry_id in entries:
                if compare_stream_ids(entry_id, actual_start_id) > 0:
                    # Format entry data as [field1, value1, field2, value2, ...]
                    entry_data = entries[entry_id]
                    field_value_list = []
                    for field, value in entry_data.items():
                        field_value_list.extend([field, value])
                    stream_entries.append([entry_id, field_value_list])
            
            # Only include streams that have entries
            if stream_entries:
                result.append([stream_key, stream_entries])
        
        # If we have immediate results or no blocking, return immediately
        if result or block_timeout is None:
            conn.sendall(encode_resp(result))
        else:
            # No immediate results and blocking requested
            timeout_end = time.time() + block_timeout
            
            # Add client to blocking list for all requested streams
            # Use the processed IDs (with $ resolved) for blocking
            with blocking_clients_lock:
                for stream_key in stream_keys:
                    if stream_key not in blocking_clients:
                        blocking_clients[stream_key] = []
                    blocking_clients[stream_key].append((conn, stream_keys, processed_stream_ids, timeout_end))

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
        except Exception:
            break
    
    # Clean up client transaction when connection closes
    if conn in client_transactions:
        del client_transactions[conn]
    
    conn.close()


def main():
    # Start cleanup thread for expired blocking clients
    cleanup_thread = threading.Thread(target=cleanup_expired_blocking_clients, daemon=True)
    cleanup_thread.start()
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("localhost", 6379))
    s.listen()
    while True:
        conn, _ = s.accept()
        threading.Thread(target=client_thread, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()