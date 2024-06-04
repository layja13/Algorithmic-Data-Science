# Run this code
# It takes several minutes to complete the execution

from concurrent.futures import ProcessPoolExecutor
import time
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def timeit(somefunc, *args, repeats=1, **kwargs):
    times = []
    for i in range(repeats):
        starttime = time.time()
        ans = somefunc(*args, **kwargs)
        endtime = time.time()
        timetaken = endtime - starttime
        times.append(timetaken)

    mean = np.mean(times)
    stdev = np.std(times)

    return mean, stdev


def maketotal(intersection):
    total = 0
    for i in range(len(intersection)):
        total += intersection[i]
    return total


def jaccard(document1, document2):
    intersection = []
    for i in range(len(document1)):
        intersection.append(min(document1[i], document2[i]))

    intersectiontot = maketotal(intersection)
    union = maketotal(document1) + maketotal(document2) - intersectiontot
    return intersectiontot / union


def jaccard_all_pairs_similarites(data, docs):
    for i in range(1, docs):
        for j in range(1, docs):
            jaccard(data[f"doc{i}"], data[f"doc{j}"])


def jaccard_all_pairs_similarites_all(data, amount_docs):
    a = [jaccard_all_pairs_similarites(data, i) for i in amount_docs]


def parallel_function(DataFrame, amount_docs):
    num_processes = len(amount_docs)
    dataFrames = [DataFrame] * num_processes
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        executor.map(jaccard_all_pairs_similarites, dataFrames, amount_docs)


if __name__ == "__main__":
    data = pd.read_csv("data2023.csv", index_col=0)
    amount_docs = [i for i in range(1, 14)]

    time_parallel = [timeit(parallel_function, data, amount_docs)[0] for i in range(5)]
    time_non_parallel = [timeit(jaccard_all_pairs_similarites_all, data, amount_docs)[0] for i in range(5)]

    size = [i for i in range(len(time_parallel))]

    plt.scatter(size, time_parallel, label='Parallel')
    plt.scatter(size, time_non_parallel, label='Non parallel')

    plt.xlabel('Number of tests')
    plt.ylabel('Time (s)')
    plt.title("Time-consuming parallelism vs non-parallelism ")
    plt.legend()

    plt.show()

    mean_time_parallel = sum(time_parallel) / len(time_parallel)
    mean_time_non_parallel = sum(time_non_parallel) / len(time_non_parallel)

    print("\nParallel function average time:", mean_time_parallel, "s")
    print("\nNon-parallel function average time:", mean_time_non_parallel, "s")

    percentage_parallel = ((mean_time_parallel * 100) / mean_time_non_parallel)
    percentage_time_difference = 100 - percentage_parallel

    print("\nParallel function is", round(percentage_time_difference, 2), "% faster than non-parallel function")