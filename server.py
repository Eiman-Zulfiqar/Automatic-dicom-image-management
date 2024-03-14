import socket
import threading
import psycopg2
from datetime import datetime

# Function to handle client requests
def handle_client(client_socket, addr, conn):
    try:
        data = client_socket.recv(1024).decode()
        print("Received from client:", data)
        pid = data.strip()  # Assuming the client sends the PID as a string

        # Verify if the PID exists in the database
        cursor = conn.cursor()
        cursor.execute("SELECT EXISTS(SELECT 1 FROM image_metadata WHERE patient_id = %s)", (pid,))
        exists = cursor.fetchone()[0]

        if exists:
            client_socket.send("PID verified. Enter date and image data.".encode())
            # Receive date from the client
            date = client_socket.recv(1024).decode()
            print("Received date from client:", date)

            # Fetch image data from the database
            cursor.execute("SELECT image FROM image_metadata WHERE patient_id = %s", (pid,))
            image_data = cursor.fetchone()[0]

            # Send image data to the client
            client_socket.sendall(image_data)
            print("Image data sent to client.")
        else:
            client_socket.send("Invalid PID. Please enter a valid PID.".encode())
    except Exception as e:
        print(f"Error handling client request: {e}")
    finally:
        client_socket.close()

# Main function to set up the server
def main():
    host = '0.0.0.0'  # Listen on all available network interfaces
    port = 12345  # Choose a port number for the server

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print("Server listening on {}:{}".format(host, port))

    # Connect to the database
    conn = connect_to_database()

    try:
        while True:
            client_socket, addr = server_socket.accept()
            print("Connection from:", addr)
            client_thread = threading.Thread(target=handle_client, args=(client_socket, addr, conn))
            client_thread.start()
    finally:
        server_socket.close()
        conn.close()

# Function to connect to the PostgreSQL database
def connect_to_database():
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="SQLfatima@31",
            host="localhost",
            port="5432",
            database="image"
        )
        return connection
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")
        return None

if __name__ == "__main__":
    main()
