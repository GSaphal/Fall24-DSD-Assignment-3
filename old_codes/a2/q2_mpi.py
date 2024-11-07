"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
from mpi4py import MPI
import pandas as pd
import numpy as np

class MPISolution:
    """
    You are allowed to implement as many methods as you wish
    """

    def get_book_count(self, start_row, chunk_size):
        try:
            column_names = pd.read_csv(self.dataset_path, nrows=0).columns
            chunk = pd.read_csv(self.dataset_path, skiprows=start_row, nrows=chunk_size, header=None)
            chunk.columns = column_names.values
            avg_score = chunk.groupby('BId')['RScore'].mean()
            books_score = avg_score[avg_score == 5].index
            filtered_books = chunk[(chunk['BId'].isin(books_score)) & (chunk['BPrice'] == 1)]
            count_books = filtered_books['BId'].nunique()
            return count_books
        except Exception as e:
            print(f"Error: {str(e)}")
            return f"Error: {str(e)}"

    def __init__(self, dataset_path=None, dataset_size=None):
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self)->tuple[int,list,list,float]:
        """
        Returns the tuple of computed result and time taken. eg., ("I am final Result", 3.455)
        """
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()
        print(size,rank,comm)
        start_time = MPI.Wtime()
        chunk_size_per_process = self.dataset_size // (size - 1) if size > 1 else self.dataset_size
        additional_size = self.dataset_size % (size - 1) if size > 1 else 0
        chunk_size_list = [0] * size

        if rank == 0:
            for i in range(1, size):
                start_row = (i - 1) * chunk_size_per_process + 1
                if i == size - 1:
                    chunk_size = chunk_size_per_process + additional_size
                else:
                    chunk_size = chunk_size_per_process
                chunk_size_list[i] = chunk_size
                comm.send((start_row, chunk_size), dest=i, tag=99)
            answer_per_process = []
            for i in range(1, size):
                count = comm.recv(source=i, tag=100)
                answer_per_process.append(count.item() if isinstance(count, np.int64) else count)
            total_count = sum(answer_per_process)
            total_time = MPI.Wtime() - start_time
            return total_count, chunk_size_list[1:], answer_per_process, total_time

        else:
            start_row, chunk_size = comm.recv(source=0, tag=99)
            chunk_size_list[rank] = chunk_size
            count = self.get_book_count(start_row, chunk_size)
            comm.send(count, dest=0, tag=100)
            return 0, list(), list(), 0


if __name__ == '__main__':
    solution = MPISolution(dataset_path="Books_rating.csv", dataset_size=3000000)
    final_answer,chunkSizePerThread,answerPerThread,totalTimeTaken = solution.run()
    if MPI.COMM_WORLD.Get_rank() == 0:
        print({
            "final_answer": final_answer,
            "chunkSizePerThread": chunkSizePerThread,
            "answerPerThread": answerPerThread,
            "totalTimeTaken": totalTimeTaken
        })
