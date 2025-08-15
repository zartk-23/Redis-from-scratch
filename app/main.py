import socket 
import threading # noqa: F401


def handle_client():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    # Uncomment this to pass the first stage
    pong= "+PONG\r\n"
    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379))
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    connection, _ = server_socket.accept()
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
        # Start a new thread for each client
        threading.Thread(target=handle_client, args=(connection,), daemon=True).start()


if __name__ == "__main__":
    main()
