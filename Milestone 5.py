import csv
import time
import json
import os
import asyncio
import statistics
from functools import reduce
from threading import Thread, Lock
from queue import Queue
from multiprocessing import Pool, cpu_count

# ERRORS


class SystemError(Exception):
    pass


class DataLoadError(SystemError):
    pass


class Serialization(SystemError):
    pass

# OBSERVER


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

# THREAD - SAFE CACHE


class JSONCache:
    def __init__(self, path="cache.json"):
        self.path = path
        self.lock = Lock()

    def load_all(self):
        if not os.path.exists(self.path):
            return {}
        with open(self.path, mode="r") as f:
            return json.load(f)

    def load(self, key):
        return self.load_all().get(key)

    def save(self, key, value):
        with self.lock:
            data = self.load_all()
            data[key] = value
            with open(self.path, mode="w") as f:
                json.dump(data, f)

# STRATEGY


class StatsStrategy:
    def process(self, data):
        data = list(data)
        if not data:
            return {}

        count = len(data)
        total = reduce(lambda x, y: x + y, data)
        mean = total / count
        variance = statistics.variance(data) if len(data) > 1 else 0
        stddev = statistics.stdev(data) if len(data) > 1 else 0
        maximum = max(data)
        minimum = min(data)

        return {
            "count ": count,
            "sum ": total,
            "mean ": mean,
            "variance ": variance,
            "std dev ": stddev,
            "Max ": maximum,
            "Min": minimum
        }

# DATA REPOSITORY


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

# MULTIPROCESSING WORKER


def process_chunk(chunk):
    return StatsStrategy().process(chunk)

# SYSTEM


class ScalableSystem:
    def __init__(self, filepath):
        self.bus = EventBus()
        self.bus.subscribe(log_events)

        self.repo = DataRepository(filepath, self.bus)
        self.cache = JSONCache()

        self.repo.load()

    # BASELINE (SEQUENTIAL)
    def run_sequential(self, column):
        start = time.time()

        data = list(self.repo.stream(column))
        result = StatsStrategy().process(data)

        result["time"] = time.time() - start
        return result

    # PARALLEL PIPELINE
    def run_parallel(self, column, chunk_size=500):
        start = time.time()

        data = list(self.repo.stream(column))
        chunks = [data[i:i + chunk_size]
                  for i in range(0, len(data), chunk_size)]

        q = Queue()
        results = []

        # PRODUCER
        def worker():
            while True:
                chunk = q.get()
                if chunk is None:
                    break
                results.append(process_chunk(chunk))

        for c in chunks:
            q.put(c)

        for _ in range(4):
            q.put(None)

        threads = [Thread(target=worker) for _ in range(4)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        final = self.aggregate(results)
        final["time"] = time.time() - start

        return final

    # MULTIPROCESSING
    def run_multiprocess(self, column, chunk_size=1000):
        start = time.time()

        data = list(self.repo.stream(column))
        chunks = [data[i:i + chunk_size]
                  for i in range(0, len(data), chunk_size)]

        with Pool(cpu_count()) as pool:
            results = pool.map(process_chunk, chunks)

        final = self.aggregate(results)
        final["time"] = time.time() - start

        return final

    # ASYNC WRAPPER
    async def run_async(self, column):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.run_parallel, column)

    # AGGREGATION
    def aggregate(self, results):
        if not results:
            return {}

        total_count = sum(r.get("count", 0) for r in results)
        total_sum = sum(r.get("sum", 0) for r in results)

        return {

            "Count": total_count,
            "Total_Sum": total_sum,
            "Mean": total_sum / len(results) if total_sum else 0,
            "Chunks": len(results),
        }

    # PRESENTATION LAYER


def print_results(title, result):
    print(f"{title}: ")

    keys = {"count", "Sum", "Mean", "Variance",
            "std dev", "max", "min", "chunks", "time"}

    for k, v in keys.items():
        #        if k in result:
        print(f"{k.capitalize()} : {result[k]}")


def display_results(seql, par, mp, async_result):
    print()
    print("---- RESULTS ----")
    print("Sequential:", seql)
    print("Parallel:", par)
    print("Multi-Processing:", mp)
    print("Async:", async_result)


# MAIN EXECUTION
if __name__ == "__main__":
    sys = ScalableSystem("social_media_impact.csv")

    seql = sys.run_sequential(column="Deaths")
    par = sys.run_parallel(column="Deaths")
    mp = sys.run_multiprocess(column="Deaths")
    async_result = asyncio.run(sys.run_async(column="Deaths"))

    display_results(seql, par, mp, async_result)
