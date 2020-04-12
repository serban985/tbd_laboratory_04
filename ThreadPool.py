import threading
import time

class ThreadPool:
    def __init__(self, nrThreads):
        self.__nrThreads = nrThreads
        self.__semaphore = threading.Semaphore(value=self.__nrThreads)
        self.__pool = {}

    def __threadWrapper(self, sem, func, argsDict):
        sem.acquire()
        func(argsDict)
        sem.release()

    def startWorker(self, func, argsDict):
        self.__semaphore.acquire()
        newTh = threading.Thread(target=self.__threadWrapper, kwargs={'sem': self.__semaphore, 'func': func, 'argsDict': argsDict})
        self.__pool[newTh] = True
        newTh.start()
        self.__semaphore.release()

    def joinAll(self):
        for th in self.__pool: # should investigate thread safe access to __pool
            th.join()

def printSomething(argsDict):
    print(argsDict['a'], argsDict['b'], argsDict['c'])
    time.sleep(2)

def demo():
    print('Running 10 workers with a threadpool of size 2')
    tp = ThreadPool(2)
    for i in range(5):
        tp.startWorker(printSomething, {'a': str(i), 'b': str(i*10), 'c': str(i*20)})
    tp.joinAll()
