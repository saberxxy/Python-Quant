import multiprocessing as mp
import time
import sys


# 1.Process
# 1.1 创建函数并将其作为单个进程
def worker(interval):
    n = 5
    while n > 0:
        print("The time is {0}".format(time.ctime()))
        time.sleep(interval)
        n -= 1


# 1.2 创建函数并将其作为多个进程
def worker_1(interval):
    print("worker_1", time.ctime())
    time.sleep(interval)
    print( "end worker_1", time.ctime())


def worker_2(interval):
    print("worker_2", time.ctime())
    time.sleep(interval)
    print("end worker_2", time.ctime())


def worker_3(interval):
    print("worker_3", time.ctime())
    time.sleep(interval)
    print("end worker_3", time.ctime())


# 1.3 将进程定义为类
class ClockProcess(mp.Process):
    def __init__(self, interval):
        mp.Process.__init__(self)
        self.interval = interval

    def run(self):
        n = 5
        while n > 0:
            print("the time is {0}".format(time.ctime()))
            time.sleep(self.interval)
            n -= 1


# 1.4 daemon程序对比结果
def worker4(interval):
    print("work start:{0}".format(time.ctime()));
    time.sleep(interval)
    print("work end:{0}".format(time.ctime()));


# 2 Lock
def worker_with(lock, f):
    with lock:
        fs = open(f, 'a+')
        n = 10
        while n > 1:
            fs.write("Locad acquired via with"+ str(time.ctime()) + "\n")
            n -= 1
        fs.close()


def worker_no_with(lock, f):
    lock.acquire()
    try:
        fs = open(f, 'a+')
        n = 10
        while n > 1:
            fs.write("Lock acquired directly" + str(time.ctime()) + "\n")
            n -= 1
        fs.close()
    finally:
        lock.release()


# 3. Semaphore
def worker5(s, i):
    s.acquire()
    print(mp.current_process().name + "acquire", time.ctime());
    time.sleep(i)
    print(mp.current_process().name + "release", time.ctime(), "\n");
    s.release()


# 4. Event 用来实现进程间同步通信
def wait_for_event(e):
    print("wait_for_event: starting")
    e.wait()
    print("wairt_for_event: e.is_set()->" + str(e.is_set()))


def wait_for_event_timeout(e, t):
    print("wait_for_event_timeout:starting")
    e.wait(t)
    print("wait_for_event_timeout:e.is_set->" + str(e.is_set()))


# 7. Pool
def func(msg):
    print("msg:", msg)
    time.sleep(3)
    print("end")


def main():
    # 1.1
    # p = mp.Process(target=worker, args=(3,))
    # p.start()
    # print("p.pid", p.pid)
    # print("p.name", p.name)
    # print("p.is_alive", p.is_alive())

    # 1.2
    # p1 = mp.Process(target=worker_1, args=(2,))
    # p2 = mp.Process(target=worker_2, args=(3,))
    # p3 = mp.Process(target=worker_3, args=(4,))
    # p1.start()
    # p2.start()
    # p3.start()
    # print("The number of CPU is:" + str(mp.cpu_count()))
    # for p in mp.active_children():
    #     print("child   p.name:"+p.name+"\tp.id" + str(p.pid))
    # print("END!!!!!!!!!!!!!!!!!")

    # 1.3
    # p = ClockProcess(3)
    # p.start()

    # 1.4.1 不加daemon属性
    # p = mp.Process(target=worker4, args=(3,))
    # # 1.4.2 加上daemon属性
    # p.daemon = True
    # p.start()
    # # 1.4-3 设置daemon执行完结束的方法
    # p.join()
    # print("END!!")

    # 2
    # lock = mp.Lock()
    # f = "file.txt"
    # w = mp.Process(target=worker_with, args=(lock, f))
    # nw = mp.Process(target=worker_no_with, args=(lock, f))
    # w.start()
    # nw.start()
    # print("end")

    # 3. Semaphore用来控制对共享资源的访问数量，例如池的最大连接数。
    # s = mp.Semaphore(3)
    # for i in range(5):
    #     p = mp.Process(target=worker5, args=(s, i * 2))
    #     p.start()


    # 4
    # e = mp.Event()
    # w1 = mp.Process(name="block", target=wait_for_event, args=(e,))
    # w2 = mp.Process(name="non-block", target=wait_for_event_timeout, args=(e, 2))
    # w1.start()
    # w2.start()
    # time.sleep(3)
    # e.set()
    # print("main: event is set")

    #7 Pool
    pool = mp.Pool(processes=3)
    for i in range(4):
        msg = "hello %d" % (i)
        pool.apply_async(func, (msg,))  # 维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去

    print("Mark~ Mark~ Mark~~~~~~~~~~~~~~~~~~~~~~")

    pool.close()
    pool.join()  # 调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束
    print("Sub-process(es) done.")


if __name__ == "__main__":
    main()
