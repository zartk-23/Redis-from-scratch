import socket

def start_server():
    host = "0.0.0.0"
    port = 6379  # Redis default port

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(1)

    print(f"Server listening on {host}:{port}")

    while True:
        client_socket, addr = server_socket.accept()
        print(f"Connection from {addr}")

        while True:
            data = client_socket.recv(1024)

            if not data:
                break

            print(f"Received: {data}")

            # Always reply with a RESP2 PONG for testing
            # RESP simple string starts with "+"
            response = b"+PONG\r\n"
            client_socket.sendall(response)

        client_socket.close()

if __name__ == "__main__":
    start_server()
