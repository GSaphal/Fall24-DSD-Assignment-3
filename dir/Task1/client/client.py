import socket
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client_socket = None
        self.eof_token = None

    def receive_message_ending_with_token(
            self, active_socket, buffer_size, eof_token
    ) -> bytearray:
        """
        Same implementation as in receive_message_ending_with_token() in server.py
        A helper method to receives a bytearray message of arbitrary size sent on the socket.
        This method returns the message WITHOUT the eof_token at the end of the last packet.
        :param active_socket: a socket object that is connected to the server
        :param buffer_size: the buffer size of each recv() call
        :param eof_token: a token that denotes the end of the message.
        :return: a bytearray message with the eof_token stripped from the end.
        """
        message = bytearray()
        data = active_socket.recv(buffer_size)
        message += data
        return message[: -len(eof_token.encode())]

    def initialize(self, host, port) -> tuple[socket.socket, str]:
        """
        1) Creates a socket object and connects to the server.
        2) receives the random token (10 bytes) used to indicate end of messages.
        3) Displays the current working directory returned from the server (output of get_working_directory_info() at the server).
        Use the helper method: receive_message_ending_with_token() to receive the message from the server.
        :param host: the ip address of the server
        :param port: the port number of the server
        :return: the created socket object
        :return: the eof_token
        """
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((host, port))
        eof_encoded = client_socket.recv(1024)
        eof_token = eof_encoded.decode()
        current_cwd = self.receive_message_ending_with_token(client_socket, 1024, eof_token)
        print(current_cwd.decode())
        return client_socket, eof_token

    def issue_cd(self, command_and_arg, client_socket, eof_token) -> None:
        """
        Sends the full cd command entered by the user to the server. The server changes its cwd accordingly and sends back
        the new cwd info.
        Use the helper method: receive_message_ending_with_token() to receive the message from the server.
        :param command_and_arg: full command (with argument) provided by the user.
        :param client_socket: the active client socket object.
        :param eof_token: a token to indicate the end of the message.
        """
        client_socket.sendall(command_and_arg.encode() + eof_token.encode())
        new_dir = self.receive_message_ending_with_token(client_socket, 1024, eof_token)
        print(new_dir.decode())

    def issue_mkdir(self, command_and_arg, client_socket, eof_token) -> None:
        """
        Sends the full mkdir command entered by the user to the server. The server creates the sub directory and sends back
        the new cwd info.
        Use the helper method: receive_message_ending_with_token() to receive the message from the server.
        :param command_and_arg: full command (with argument) provided by the user.
        :param client_socket: the active client socket object.
        :param eof_token: a token to indicate the end of the message.
        """
        client_socket.sendall(command_and_arg.encode() + eof_token.encode())
        new_dir = self.receive_message_ending_with_token(client_socket, 1024, eof_token)
        print(new_dir.decode())

    def issue_rm(self, command_and_arg, client_socket, eof_token) -> None:
        """
        Sends the full rm command entered by the user to the server. The server removes the file or directory and sends back
        the new cwd info.
        Use the helper method: receive_message_ending_with_token() to receive the message from the server.
        :param command_and_arg: full command (with argument) provided by the user.
        :param client_socket: the active client socket object.
        :param eof_token: a token to indicate the end of the message.
        """
        client_socket.sendall(command_and_arg.encode() + eof_token.encode())
        deleted_dir = self.receive_message_ending_with_token(client_socket, 1024, eof_token)
        print(deleted_dir.decode())

    def issue_ul(self, command_and_arg, client_socket, eof_token) -> None:
        """
        Sends the full ul command entered by the user to the server. Then, it reads the file to be uploaded as binary
        and sends it to the server. The server creates the file on its end and sends back the new cwd info.
        Use the helper method: receive_message_ending_with_token() to receive the message from the server.
        :param command_and_arg: full command (with argument) provided by the user.
        :param client_socket: the active client socket object.
        :param eof_token: a token to indicate the end of the message.
        """
        client_socket.sendall(command_and_arg.encode() + eof_token.encode())
        file_name = command_and_arg.split()[1]
        parent_directory = os.getcwd()
        if file_name.startswith('./'):
            file_path = os.path.join(parent_directory, file_name.lstrip('./'))
        else:
            file_path = os.path.join(parent_directory, file_name)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"ul failed")
        with open(file_path, "rb") as file:
            while True:
                file_content = file.read(1024)
                if not file_content:
                    break
                client_socket.send(file_content)
        client_socket.send(eof_token.encode())
        new_cwd = self.receive_message_ending_with_token(client_socket, 1024, eof_token)
        print(new_cwd.decode())
        return None

    def issue_dl(self, command_and_arg, client_socket, eof_token) -> None:
        """
        Sends the full dl command entered by the user to the server. Then, it receives the content of the file via the
        socket and re-creates the file in the local directory of the client. Finally, it receives the latest cwd info from
        the server.
        Use the helper method: receive_message_ending_with_token() to receive the message from the server.
        :param command_and_arg: full command (with argument) provided by the user.
        :param client_socket: the active client socket object.
        :param eof_token: a token to indicate the end of the message.
        """
        client_socket.sendall(command_and_arg.encode() + eof_token.encode())
        file_content = self.receive_message_ending_with_token(client_socket, 1024, eof_token)
        if file_content == b'dl failed':
            print(file_content.decode())
        else:
            with open(command_and_arg.split()[1], 'wb') as file:
                file.write(file_content)
            self.receive_message_ending_with_token(client_socket, 1024, eof_token)
        return None

    def issue_wordcount(self, command_and_arg, client_socket, eof_token) -> int:
        """
        Sends the full wordcount command entered by the user to the server. Then, it receives the number of words in the file via the socket. Finally, it receives the latest cwd info from
        the server.
        Use the helper method: receive_message_ending_with_token() to receive the message from the server.
        :param command_and_arg: full command (with argument) provided by the user.
        :param client_socket: the active client socket object.
        :param eof_token: a token to indicate the end of the message.
        :return: wordcount int
        """
        client_socket.sendall(command_and_arg.encode() + eof_token.encode())
        word_count = self.receive_message_ending_with_token(client_socket, 1024, eof_token)
        print(word_count.decode())
        return int(word_count.decode())

    def issue_wordsort(self, command_and_arg, client_socket, eof_token) -> list[str]:
        """
        Sends the full wordsort command entered by the user to the server. Then, it receives the list of alphabetically sorted words via the
        socket. Finally, it receives the latest cwd info from the server.
        Use the helper method: receive_message_ending_with_token() to receive the message from the server.
        :param command_and_arg: full command (with argument) provided by the user.
        :param client_socket: the active client socket object.
        :param eof_token: a token to indicate the end of the message.
        :return: list
        """

        client_socket.sendall(command_and_arg.encode() + eof_token.encode())
        sorted_words = self.receive_message_ending_with_token(client_socket, 1024, eof_token)
        print(sorted_words.decode())
        return sorted_words.decode().split()

    def issue_search(self, command_and_arg, client_socket, eof_token) -> dict[str, int]:
        """
        Sends the full search command entered by the user to the server. Then, it receives the dictionary of words their the number of matches i.e {  token1: 5, token2: 6, ...,} via the socket.
        Finally, it receives the latest cwd info from the server.
        Use the helper method: receive_message_ending_with_token() to receive the message from the server.
        :param command_and_arg: full command (with argument) provided by the user.
        :param client_socket: the active client socket object.
        :param eof_token: a token to indicate the end of the message.
        :return: dict
        """
        client_socket.sendall(command_and_arg.encode() + eof_token.encode())
        word_count = self.receive_message_ending_with_token(client_socket, 1024, eof_token)
        print(word_count.decode())
        return word_count.decode()

    def issue_split(self, command_and_arg, client_socket, eof_token) -> int:
        """
        Sends the full split command entered by the user to the server. Then, save the splits into files with naming pattern filename}_split_{split number}.txt
        then receives the number of splits via the socket. Finally, it receives the latest cwd info from the server.
        Use the helper method: receive_message_ending_with_token() to receive the message from the server.
        :param command_and_arg: full command (with argument) provided by the user.
        :param client_socket: the active client socket object.
        :param eof_token: a token to indicate the end of the message.
        :return: splitcount int
        """
        client_socket.sendall(command_and_arg.encode() + eof_token.encode())
        split_count = self.receive_message_ending_with_token(client_socket, 1024, eof_token)
        return int(len(split_count.decode()))

    def issue_exit(self, command_and_arg, client_socket, eof_token) -> None:
        """
        Sends the full exit command entered by the user to the server. Then, close the client socket and  print message “the client has exited”.
        Use the helper method: receive_message_ending_with_token() to receive the message from the server.
        :param command_and_arg: full command (with argument) provided by the user.
        :param client_socket: the active client socket object.
        :param eof_token: a token to indicate the end of the message.
        :return:
        """
        client_socket.sendall(command_and_arg.encode() + eof_token.encode())
        print("the client has exited")
        client_socket.close()
        return None

    def start(self) -> None:
        """
        1) Initialization
        2) Accepts user input and issue commands until exit.
        """
        client_socket, eof_token = self.initialize(self.host, self.port)
        client_folder="client"+os.getenv("CLIENT_ID",0)
        self.issue_mkdir("mkdir "+client_folder,client_socket, eof_token)
        self.issue_cd('cd '+client_folder, client_socket, eof_token)
        self.issue_ul('ul saphal.txt', client_socket, eof_token)
        while True:
            user_command = input("Please enter a valid command:")
            command = user_command.split()[0]
            match command:
                case "cd":
                    self.issue_cd(user_command, client_socket, eof_token)
                case "mkdir":
                    self.issue_mkdir(user_command, client_socket, eof_token)
                case "rm":
                    self.issue_rm(user_command, client_socket, eof_token)
                case "ul":
                    self.issue_ul(user_command, client_socket, eof_token)
                case "dl":
                    self.issue_dl(user_command, client_socket, eof_token)
                case "wordcount":
                    self.issue_wordcount(user_command, client_socket, eof_token)
                case "wordsort":
                    self.issue_wordsort(user_command, client_socket, eof_token)
                case "search":
                    self.issue_search(user_command, client_socket, eof_token)
                case "split":
                    self.issue_split(user_command, client_socket, eof_token)
                case "exit":
                    self.issue_exit(user_command, client_socket, eof_token)
                    print('Exiting the application.')
                    break
                case _:
                    print("Invalid command")


## Don't Modify Below ##
def run_client():
    HOST=os.getenv("HOST", "127.0.0.1")
    PORT=int(os.getenv("PORT", 65432))
    logging.info(f"Attempting to connect to {HOST}:{PORT}")
    client = Client(HOST, PORT)
    client.start()


if __name__ == "__main__":
    run_client()
