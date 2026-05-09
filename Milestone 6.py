import csv
import math
import time
import json
import os
import asyncio
from threading import Thread, Lock
from queue import Queue
from multiprocessing import Pool, cpu_count


class SystemError(Exception):
    pass


class DataLoadError(SystemError):
    pass


class Serialization(SystemError):
    pass


class EventBus:
    def __init__(self):
        self.subscribers = []

    def subscribe(self, fn):
        self.subscribers.append(fn)

    def publish(self, event, data=None):
        for sub in self.subscribers:
            sub(event, data)


def log_events(event, data):
    print(f"{event}: {data}")


class JSONCache:
    def __init__(self, path="cache1.json"):
        self.path = path
        self.lock = Lock()

    def load_all(self):
        if not os.path.exists(self.path):
            return {}
        with self.lock:
            with open(self.path, mode="r") as f:
                return json.load(f)

    def load(self, key):
        return self.load_all().get(key)

    def save(self, key, value):
        with self.lock:
            if not os.path.exists(self.path):
                data = {}
            else:
                with open(self.path, mode="r") as f:
                    data = json.load(f)
            data[key] = value

            with open(self.path, mode="w") as f:
                json.dump(data, f)


class MergeableStats:
    def __init__(self, n=0, s=0, s2=0, mx=None, mn=None):
        self.n = n
        self.s = s
        self.s2 = s2
        self.mx = mx
        self.mn = mn

    def add(self, x):
        self.n += 1
        self.s += x
        self.s2 = x * x

        if self.mx is None or x > self.mx:
            self.mx = x

        if self.mn is None or x < self.mn:
            self.mn = x

    def merge(self, other):
        return MergeableStats(
            self.n + other.n,
            self.s + other.s,
            self.s2 + other.s2,
            max(self.mx, other.mx) if self.mx is not None else other.mx,
            min(self.mn, other.mn) if self.mn is not None else other.mn
        )

    def finalize(self):
        if self.n == 0:
            return {}

        mean = self.s / self.n
        variance = (self.s2 / self.n) - mean ** 2

#        variance = max(0.0, variance)

        return {
            "Count": self.n,
            "Total": self.s,
            "Mean": mean,
            "Variance": variance,
            "Stddev": math.sqrt(variance),
            "Maximum": self.mx,
            "Minimum": self.mn
        }


class DataRepository:
    def __init__(self, filepath, bus):
        self.filepath = filepath
        self.bus = bus
        self._data = []

    def load(self):
        try:
            with open(self.filepath, mode="r", encoding="utf-8") as f:
                self._data = list(csv.DictReader(f))
            self.bus.publish("DATA LOADED", self.filepath)
        except Exception as e:
            self.bus.publish("DATA LOADING FAILED", str(e))
            raise DataLoadError(e)

    def stream(self, column):
        for row in self._data:
            try:
                val = float(row[column])
                if val > 0:
                    yield val
            except:
                continue


def process_chunk(chunk):
    stats = MergeableStats()
    for x in chunk:
        stats.add(x)
    return stats


class PerformanceModel:
    def __init__(self):
        self.history = []

    def record(self, size, time_taken, mode):
        self.history.append((size, time_taken, mode))

    def predict_best_mode(self, size):
        if size < 10000:
            return "sequential"
        elif size < 100000:
            return "thread"
        return "multiprocess"


class ExecutionPlanner:
    def __init__(self):
        self.model = PerformanceModel()

    def choose(self, data_size):
        return self.model.predict_best_mode(data_size)


class IntelligentSystem:
    def __init__(self, filepath):
        self.bus = EventBus()
        self.bus.subscribe(log_events)

        self.repo = DataRepository(filepath, self.bus)
        self.cache = JSONCache()
        self.planner = ExecutionPlanner()

        self.repo.load()

    def run(self, column):
        data = list(self.repo.stream(column))
        size = len(data)

        mode = self.planner.choose(size)
        self.bus.publish("\nEXECUTION MODE", mode)

        cache_key = f"{column}"
        cached = self.cache.load(cache_key)

        if cached:
            self.bus.publish("CACHE HIT", cache_key)
            return cached, 0, mode

        start = time.time()

        if mode == "sequential":
            result = self._sequential(data)

        elif mode == "thread":
            result = self._thread_pipeline(data)

        else:
            result = self._multiprocess(data)

        elapsed = time.time() - start

        self.cache.save(cache_key, result)
        self.planner.model.record(size, elapsed, mode)

        return result, elapsed, mode

    def _sequential(self, data):
        stats = MergeableStats()
        for x in data:
            stats.add(x)
        return stats.finalize()

    def _thread_pipeline(self, column, chunk_size=500):
        q = Queue()
        results = []
        lock = Lock()

        def producer():
            chunk = []
            for val in self.repo.stream(column):
                chunk.append(val)
                if len(chunk) >= chunk_size:
                    q.put(chunk)
                    chunk = []
            if chunk:
                q.put(chunk)
            for _ in range(4):
                q.put(None)

        def consumer():
            while True:
                chunk = q.get()
                if chunk is None:
                    break
                stats = process_chunk(chunk)
                with lock:
                    results.append(stats)

        producer_thread = Thread(target=producer)
        workers = [Thread(target=consumer) for _ in range(4)]

        producer_thread.start()
        for w in workers:
            w.start()

        producer_thread.join()
        for w in workers:
            w.join()

        final = MergeableStats()
        for r in results:
            final = final.merge(r)

        return final.finalize()

    def _multiprocess(self, data, chunk_size=2000):
        chunks = [data[i:i + chunk_size]
                  for i in range(0, len(data), chunk_size)]

        with Pool(cpu_count()) as p:
            results = p.map(process_chunk, chunks)

        final = MergeableStats()
        for r in results:
            final = final.merge(r)

        return final.finalize()

    async def run_async(self, column):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.run, column)


if __name__ == "__main__":
    sys = IntelligentSystem("social_media_impact.csv")
    result, t, mode = sys.run("Deaths")

    print("\nRESULTS\n")
    for key, value in result.items():
        print(f"{key} : {value}")
    print(f"\nEXECUTION TIME: {t:.4f} seconds")

    asy_r, asy_t, asy_md = asyncio.run(sys.run_async("Confirmed"))

    print("\nASYNC RESULTS\n")
    for key, value in asy_r.items():
        print(f"{key} : {value}")

    print(f"\nASYNC TIME: {asy_t:.4f} seconds")
