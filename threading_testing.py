"""
Script requests 50 wiki pages, count all words
input: 50 urls
output: map(url -> wordcount)
"""
import requests
import time
import threading
import multiprocessing
import queue

urls = [
    'https://en.wikipedia.org/wiki/John_Wick_(film)',
    'https://en.wikipedia.org/wiki/John_Wick:_Chapter_2',
    'https://en.wikipedia.org/wiki/John_Wick:_Chapter_3_%E2%80%93_Parabellum',
    'https://en.wikipedia.org/wiki/John_Wick:_Chapter_4',
]

# ### Threading method ###
# def get_word_count(url, word_count):
#     r = requests.get(url)
#     if not r.ok:
#         raise RuntimeError(f'Failed to get URL, status code {r.status_code}')
#
#     word_count.append(len(r.text.split(' ')))
#
# start_time = time.time()
# word_count = []
# threads = []

# # global interpreter lock (GIL), threading vs multiprocessing, multiprocessing queue
# for url in urls:
#     thread = threading.Thread(target=get_word_count, args=(url, word_count))
#     threads.append(thread)
#     thread.start()
#
# # make sure all thread in array is done
# for thread in threads:
#     print('not done')
#     thread.join()
#     print(f'{thread} is done!')


"""
Multiprocessing method
multi-producer exactly the number of tasks

Industry standard - where does the threading logic lives? is it in the controller or the view? 
Is there a difference using sentinel or using queue.join()?
"""
#
# def get_word_count(q, url):
#     r = requests.get(url)
#     if not r.ok:
#         raise RuntimeError(f'Failed to get URL, status code {r.status_code}')
#
#     q.put(len(r.text.split(' ')))
#
#
#
# if __name__ == "__main__":
#     start_time = time.time()
#     word_count = []
#     processes = []
#
#     # spawn multi producers
#     q = multiprocessing.Queue()
#
#     for url in urls:
#         process = multiprocessing.Process(target=get_word_count, args=(q, url))
#         processes.append(process)
#         process.start()
#
#     # double checking processes are closed
#     for process in processes:
#         process.join()
#
#     q.put(None)
#     print(q.qsize())
#
#     # process
#     print(word_count)
#     print(f'time: {time.time() - start_time}')
#     print([q.get() for _ in range(q.qsize())])

"""
Threading method
Using single producer and multiple consumer, using join and sentinel
"""


def submit_word_count(q, urls):
    for url in urls:
        q.put(url)


def get_word_count(q, word_count):
    while True:
        try:
            url = q.get(timeout=0.1)
            if not url:
                q.put(None)
                break

            r = requests.get(url)
            if not r.ok:
                raise RuntimeError(f'Failed to get URL, status code {r.status_code}')

            word_count.append(len(r.text.split(' ')))
            q.task_done()  # tell the queue that task is done

        except queue.Empty:
            continue


if __name__ == "__main__":
    start_time = time.time()
    q = queue.Queue()
    word_count = []
    producer = threading.Thread(target=submit_word_count, args=(q, urls))
    producer.start()
    for _ in range(len(urls)):
        consumer = threading.Thread(target=get_word_count, args=(q, word_count))
        consumer.start()

    print('everything starting')
    producer.join()
    print('producer ended')
    q.join()  # wait for queue is empty
    print('queue ended')
    q.put(None)  # kill the consumer threads

    print(word_count)
    print(f'time: {time.time() - start_time}')
    print(q.qsize())
