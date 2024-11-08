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

    def process_data(self, start_row, chunk_size):
        try:
            header = pd.read_csv(self.dataset_path, nrows=0).columns
            df = pd.read_csv(self.dataset_path, skiprows=start_row, nrows=chunk_size, header=None)
            df.columns = header.values
            grouped_books = df.groupby('BTitle').agg({
                'RScore': 'mean',  # Calculate average RScore
                'BPrice': 'first'  # Get the price of the book
            })
            filtered_books = grouped_books[(grouped_books['RScore'] < 3)]
            sorted_books = filtered_books.sort_values(by='BPrice', ascending=False).head(10)
            top_books = sorted_books[['BPrice']].to_dict()['BPrice']
            return top_books
        except Exception as e:
            print(f"Error: {str(e)}")
            return f"Error: {str(e)}"

    def __init__(self, dataset_path=None, dataset_size=None):
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self)->tuple[dict[str:float],list,list,float]:
        """
        Returns the tuple of (final_answer,chunkSizePerThread,answerPerThread,totalTimeTaken)
        """
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()
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
            combined_books = {}
            for result in answer_per_process:
                combined_books.update(result)
            top_10_books = dict(sorted(combined_books.items(), key=lambda item: item[1], reverse=True)[:10])
            total_time = MPI.Wtime() - start_time
            return top_10_books, chunk_size_list, answer_per_process, total_time

        else:
            start_row, chunk_size = comm.recv(source=0, tag=99)
            chunk_size_list[rank] = chunk_size
            count = self.process_data(start_row, chunk_size)
            comm.send(count, dest=0, tag=100)
            return dict, list(), list(), 0

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

