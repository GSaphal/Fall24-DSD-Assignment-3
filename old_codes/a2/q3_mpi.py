from mpi4py import MPI
import pandas as pd

class MPISolution:
    def get_book_count(self, start_row, chunk_size):
        try:
            chunk = pd.read_csv(self.dataset_path, skiprows=start_row, nrows=chunk_size, header=None)
            return chunk
        except Exception as e:
            print(f"Error: {str(e)}")
            return f"Error: {str(e)}"

    def __init__(self, dataset_path=None, dataset_size=None):
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self) -> tuple[str, list, list, float]:
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
                chunk = comm.recv(source=i, tag=100)
                answer_per_process.append(chunk)
            full_data = pd.concat(answer_per_process)
            full_data.columns = pd.read_csv(self.dataset_path, nrows=0).columns
            avg_score = full_data.groupby('UId')['RScore'].mean().reset_index()
            user_avg_score = avg_score[avg_score['RScore'] == 5]
            user_review_count = full_data[full_data['UId'].isin(user_avg_score['UId'])].groupby('UId')['BId'].count().reset_index()
            user_review_count.columns = ['UId', 'ReviewCount']
            max_review_count = user_review_count['ReviewCount'].max()
            users_with_max_reviews = user_review_count[user_review_count['ReviewCount'] == max_review_count]
            result = pd.merge(users_with_max_reviews, full_data[['UId', 'UName']].drop_duplicates(), on='UId')
            total_time = MPI.Wtime() - start_time
            answer_per_process = [str(data) for data in answer_per_process]
            return str(result['UName'].values[0]), chunk_size_list[1:],answer_per_process, total_time

        else:
            start_row, chunk_size = comm.recv(source=0, tag=99)
            chunk_size_list[rank] = chunk_size
            chunk = self.get_book_count(start_row, chunk_size)
            comm.send(chunk, dest=0, tag=100)
            return "", list(), list(), 0

if __name__ == '__main__':
    solution = MPISolution(dataset_path="Books_rating.csv", dataset_size=3000000)
    final_answer, chunkSizePerThread, answerPerThread, totalTimeTaken = solution.run()

    if MPI.COMM_WORLD.Get_rank() == 0:
        print({
            "final_answer": final_answer,
            "chunkSizePerThread": chunkSizePerThread,
            "answerPerThread": answerPerThread,
            "totalTimeTaken": totalTimeTaken
        })
