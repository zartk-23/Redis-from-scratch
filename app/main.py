import socket
import threading

def handle_client(connection):
    """Handles communication with a single client."""
    pong = "+PONG\r\n"
    with connection:
        while True:
            data = connection.recv(1024)
            if not data:
                break  # Client closed connection
            connection.sendall(pong.encode())

def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379))
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    while True:
        connection, _ = server_socket.accept()
        threading.Thread(
            target=handle_client,
            args=(connection,),  # Pass connection to function
            daemon=True
        ).start()

if __name__ == "__main__":
    main()
