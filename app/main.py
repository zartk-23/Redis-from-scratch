import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.accept() 
    connection, _ = server_socket.accept()
    connection.sendall(b"+PONG\r\n")# wait for client


if __name__ == "__main__":
    main()
