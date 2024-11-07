import socket
import random
from threading import Thread
import os
import shutil
from pathlib import Path
import time
import string
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_socket = None

    def start(self) -> None:
        """
        1) Create server, bind and start listening.
        2) Accept clinet connections and serve the requested commands.

        Note: Use ClientThread for each client connection.
        """
        logging.info(f"Server started, listening on {self.host}:{self.port}")
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        while True:
            logging.info("Waiting for a new connection ...")
            eof_token = self.generate_random_eof_token()
            client_socket, client_address = self.server_socket.accept()
            logging.info(f"Accepted connection from {client_address}")
            client_socket.sendall(str.encode(eof_token))
            try:
                client_thread = ClientThread(self, client_socket, client_address, eof_token)
                client_thread.start()
            except Exception as e:
                logging.info(f"Error: {e}")

    def get_working_directory_info(self, working_directory) -> str:
        """
        Creates a string representation of a working directory and its contents.
        :param working_directory: path to the directory
        :return: string of the directory and its contents.
        """
        dirs = "\n-- " + "\n-- ".join(
            [i.name for i in Path(working_directory).iterdir() if i.is_dir()]
        )
        files = "\n-- " + "\n-- ".join(
            [i.name for i in Path(working_directory).iterdir() if i.is_file()]
        )
        dir_info = f"Current Directory: {working_directory}:\n|{dirs}{files}"
        return dir_info

    def generate_random_eof_token(self) -> str:
        """Helper method to generates a random token that starts with '<' and ends with '>'.
        The total length of the token (including '<' and '>') should be 10.
        Examples: '<1f56xc5d>', '<KfOVnVMV>'
        return: the generated token.
        """
        random_eof_token = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
        return "<" + "".join(random_eof_token) + ">"

    def receive_message_ending_with_token(
            self, active_socket, buffer_size, eof_token
    ) -> bytearray:
        """
        Same implementation as in receive_message_ending_with_token() in client.py
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

    def handle_cd(self, current_working_directory, new_working_directory) -> str:
        """
        Handles the client cd commands. Reads the client command and changes the current_working_directory variable
        accordingly. Returns the absolute path of the new current working directory.
        :param current_working_directory: string of current working directory
        :param new_working_directory: name of the sub directory or '..' for parent
        :return: absolute path of new current working directory
        """
        if new_working_directory == "..":
            new_path = os.path.dirname(current_working_directory)
        else:
            new_path = os.path.join(current_working_directory, new_working_directory)
        if not os.path.exists(new_path):
            raise FileNotFoundError(f"cd failed")
        return os.path.abspath(new_path)

    def handle_mkdir(self, current_working_directory, directory_name) -> None:
        """
        Handles the client mkdir commands. Creates a new sub directory with the given name in the current working directory.
        :param current_working_directory: string of current working directory
        :param directory_name: name of new sub directory
        """
        logging.info(f"Creating directory {directory_name}")
        new_dir = Path(current_working_directory) / directory_name
        if new_dir.exists():
            raise FileExistsError(f"mkdir failed")
        new_dir.mkdir()
        return None

    def handle_rm(self, current_working_directory, object_name) -> None:
        """
        Handles the client rm commands. Removes the given file or sub directory. Uses the appropriate removal method
        based on the object type (directory/file).
        :param current_working_directory: string of current working directory
        :param object_name: name of sub directory or file to remove
        """
        object_path = Path(current_working_directory) / object_name
        if object_path.is_dir():
            print(object_path)
            shutil.rmtree(object_path)
        elif object_path.is_file():
            object_path.unlink()
        else:
            raise FileNotFoundError(f"rm failed")
        return None

    def handle_ul(
            self, current_working_directory, file_name, service_socket, eof_token
    ) -> None:
        """
        Handles the client ul commands. First, it reads the payload, i.e. file content from the client, then creates the
        file in the current working directory.
        Use the helper method: receive_message_ending_with_token() to receive the message from the client.
        :param current_working_directory: string of current working directory
        :param file_name: name of the file to be created.
        :param service_socket: active socket with the client to read the payload/contents from.
        :param eof_token: a token to indicate the end of the message.
        """
        file_name = os.path.basename(file_name[2:]) if file_name.startswith('./') else file_name
        file_path = os.path.join(current_working_directory, file_name)
        file_data = bytearray()

        while True:
            data = service_socket.recv(1024)
            if not data:
                break
            file_data.extend(data)
            if eof_token.encode() in data:
                file_data = file_data[:-len(eof_token)]
                break

        if file_name.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp')):
            with open(file_path, "wb") as file:
                file.write(file_data)
        elif file_name.lower().endswith(('.txt', '.md', '.csv')):
            decoded_data = file_data.decode('utf-8', errors='ignore')
            with open(file_path, "w", encoding='utf-8') as file:
                file.write(decoded_data)
        else:
            raise ValueError("Unsupported file type")
        return None

    def handle_dl(
            self, current_working_directory, file_name, service_socket, eof_token
    ) -> None:
        """
        Handles the client dl commands. First, it loads the given file as binary, then sends it to the client via the
        given socket.
        :param current_working_directory: string of current working directory
        :param file_name: name of the file to be sent to client
        :param service_socket: active service socket with the client
        :param eof_token: a token to indicate the end of the message.
        """
        parent_directory = os.path.dirname(current_working_directory)
        if file_name.startswith('./'):
            file_path = os.path.join(parent_directory, file_name.lstrip('./'))
        else:
            file_path = os.path.join(current_working_directory, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"dl failed")
        else:
            with open(file_path, "rb") as file:
                file_content = file.read()
            service_socket.sendall(file_content + eof_token.encode())
        return None

    def handle_search(
            self, current_working_directory, file_name, wordslist, service_socket, eof_token
    ) -> None:
        """
        Handles the search  commands. First, it opens the file and  perform search, then sends  the dictionary of words with their the number of case-insensitive matches i.e {  token1: 5, token2: 6, ...,}. to the client via the given socket.
        :param current_working_directory: string of current working directory
        :param file_name: name of the file to be sent to client
        :param wordslist: list of search words
        :param service_socket: active service socket with the client
        :param eof_token: a token to indicate the end of the message.
        """
        file_path = os.path.join(current_working_directory, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"search failed")
        with open(file_path, 'r') as file:
            text = file.read()
        search_dict = {}
        for word in wordslist:
            search_dict[word] = text.lower().count(word.lower())
        service_socket.sendall(str.encode(str(search_dict) + eof_token))
        return None

    def handle_split(
            self, current_working_directory, file_name, splitlist, service_socket, eof_token
    ) -> None:
        """
        Handles the split  commands. First, it opens the file and perform search, then save the splits into files with naming pattern {filename}_split_{split number}.txt
        then sends the number of splits to the client via the given socket.
        :param current_working_directory: string of current working directory
        :param file_name: name of the file to be sent to client
        :param splitlist: list of split words
        :param service_socket: active service socket with the client
        :param eof_token: a token to indicate the end of the message.
        """
        file_path = os.path.join(current_working_directory, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"split failed")

        with open(file_path, 'r') as file:
            text = file.read()

        split_texts = []
        start = 0
        for split_word in splitlist:
            index = text.find(split_word, start)
            if index != -1:
                split_texts.append(text[start:index])
            start = index + len(split_word)
        split_texts.append(text[start:])

        base_name, ext = os.path.splitext(file_name)
        for i, split_text in enumerate(split_texts):
            split_file_name = f"{base_name}_split_{i + 1}{ext}"
            split_file_path = os.path.join(current_working_directory, split_file_name)
            with open(split_file_path, 'w') as split_file:
                split_file.write(split_text)
        service_socket.sendall(str.encode(f"{len(split_texts)}" + eof_token))
        return None

    def handle_wordsort(
            self, current_working_directory, file_name, service_socket, eof_token
    ) -> None:
        """
        Handles the wordsort commands. First, it opens the file and perform unique listing for words then sort them,  then sends the list of alphabetically sorted words via the
        to the client via the given socket.
        :param current_working_directory: string of current working directory
        :param file_name: name of the file to be sent to client
        :param splitlist: list of split words
        :param service_socket: active service socket with the client
        :param eof_token: a token to indicate the end of the message.
        """

        file_path = os.path.join(current_working_directory, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"wordsort failed")

        with open(file_path, 'r') as file:
            text = file.read()

        words = text.split()
        unique_words = sorted(set(words))

        sorted_words_str = "\n".join(unique_words)
        service_socket.sendall(str.encode(sorted_words_str + eof_token))
        return None

    def handle_wordcount(
            self, current_working_directory, file_name, service_socket, eof_token
    ) -> None:
        """
        Handles the wordcount commands. First, it opens the file and perform unique listing for words and count them,  then sends the count of unique words to the client via the given socket.
        :param current_working_directory: string of current working directory
        :param file_name: name of the file to be sent to client
        :param splitlist: list of split words
        :param service_socket: active service socket with the client
        :param eof_token: a token to indicate the end of the message.
        """
        file_path = os.path.join(current_working_directory, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"wordcount failed")

        with open(file_path, 'r') as file:
            text = file.read()

        words = text.split()
        unique_words = set(words)
        word_count = len(unique_words)

        service_socket.sendall(str.encode(f"{word_count}" + eof_token))
        return None


class ClientThread(Thread):
    def __init__(
            self,
            server: Server,
            service_socket: socket.socket,
            address: str,
            eof_token: str,
    ):
        Thread.__init__(self)
        self.server_obj = server
        self.service_socket = service_socket
        self.address = address
        self.eof_token = eof_token

    def run(self):
        print("Connection from : ", self.address)
        cwd = os.getcwd()
        self.service_socket.sendall(str.encode(self.server_obj.get_working_directory_info(cwd) + self.eof_token))
        while True:
            user_command = self.server_obj.receive_message_ending_with_token(self.service_socket, 1024, self.eof_token)
            if not user_command:
                break
            command_and_arg = user_command.decode()
            if command_and_arg == "exit":
                break
            try:
                command = command_and_arg.split()[0]
                arg = command_and_arg.split()[1]
                match command:
                    case "cd":
                        cwd = self.server_obj.handle_cd(cwd, arg)
                    case "mkdir":
                        self.server_obj.handle_mkdir(cwd, arg)
                    case "rm":
                        self.server_obj.handle_rm(cwd, arg)
                    case "ul":
                        self.server_obj.handle_ul(cwd, arg, self.service_socket, self.eof_token)
                    case "dl":
                        self.server_obj.handle_dl(cwd, arg, self.service_socket, self.eof_token)
                    case "search":
                        wordslist = arg.split()
                        self.server_obj.handle_search(cwd, arg, wordslist, self.service_socket, self.eof_token)
                    case "split":
                        splitlist = arg.split()
                        self.server_obj.handle_split(cwd, arg, splitlist, self.service_socket, self.eof_token)
                    case "wordsort":
                        self.server_obj.handle_wordsort(cwd, arg, self.service_socket, self.eof_token)
                    case "wordcount":
                        self.server_obj.handle_wordcount(cwd, arg, self.service_socket, self.eof_token)
                    case _:
                        print("Invalid command")
                # time.sleep(1)
                self.service_socket.sendall(
                    str.encode(self.server_obj.get_working_directory_info(cwd) + self.eof_token))
            except Exception as e:
                self.service_socket.sendall(str.encode(f"{e}" + self.eof_token))
        self.service_socket.close()


def run_server():
    HOST=os.getenv("HOST", "0.0.0.0")
    PORT=int(os.getenv("PORT", 65432))
    server = Server(HOST, PORT)
    server.start()


if __name__ == "__main__":
    run_server()
