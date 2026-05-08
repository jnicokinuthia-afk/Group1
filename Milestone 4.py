import csv
import time
import statistics
import json
import os
from functools import reduce


class SystemError(Exception):
    pass


class DataLoadError(SystemError):
    pass


class ProcessingError(SystemError):
    pass


class SerializationError(SystemError):
    pass

# OBSERVER PATTERN


class EventBus:
    def __init__(self):
        self.subscribers = []

    def subscribe(self, fn):
        self.subscribers.append(fn)

    def publish(self, event, data=None):
        for sub in self.subscribers:
            sub(event, data)


def log_events(event, data):
    print(f"{event} --> {data}")

# DECORATOR PATTERN


def profile(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        print(f"Run time: {time.time() - start:.4f} seconds")
        return result

    return wrapper

# SERIALIZATION LAYER


class JSONCache:
    def __init__(self, path="cache.json"):
        self.path = path

    def load_all(self):
        if not os.path.exists(self.path):
            return {}
        with open(self.path, "r") as f:
            return json.load(f)

    def load(self, key):
        return self.load_all().get(key)

    def save(self, key, value):
        try:
            data = self.load_all()
            data[key] = value
            with open(self.path, "w") as f:
                json.dump(data, f)
        except Exception as e:
            raise SerializationError(e)

# STRATEGY PATTERN


class ProcessingStrategy:
    def process(self, data):
        raise NotImplementedError(
            "All concrete classes must implement this method")


class StatsStrategy(ProcessingStrategy):
    def process(self, data):
        data = list(data)
        if not data:
            return {}

        count = len(data)
        total = reduce(lambda a, b: a + b, data),
        mean = sum(data) / len(data),
        variance = statistics.variance(data) if len(data) > 1 else 0,
        stddev = statistics.stdev(data) if len(data) > 1 else 0,
        maximum = max(data),
        minimum = min(data),

        return {
            "count": count,
            "sum": total,
            "mean": mean,
            "variance": variance,
            "stddev": stddev,
            "max": maximum,
            "min": minimum,
        }


class PositiveFilterStrategy(ProcessingStrategy):
    def process(self, data):
        return [x for x in data if x > 0]

# DATA REPOSITORY


class DataRepository:
    def __init__(self, filepath, bus):
        self.filepath = filepath
        self.bus = bus
        self._data = []

    def load(self):
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                self._data = list(csv.DictReader(f))
            self.bus.publish("DATA_LOADED", self.filepath)
        except Exception as e:
            self.bus.publish("LOAD_FAILED", str(e))
            raise DataLoadError(e)

    def stream(self, column):
        for row in self._data:
            try:
                yield float(row[column])
            except Exception as e:
                continue

# MAIN SYSTEM ORCHESTRATOR


class System:
    def __init__(self, file):
        self.bus = EventBus()
        self.bus.subscribe(log_events)

        self.repo = DataRepository(file, self.bus)
        self.cache = JSONCache()

        self.repo.load()

    @profile
    def run(self, column, strategy: ProcessingStrategy):
        try:
            cache_key = f"{self.repo.filepath}:{column}"

            cached = self.cache.load(cache_key)
            if cached:
                self.bus.publish("CACHE DATA", cache_key)
                return cached

            stream = self.repo.stream(column)

            filtered = PositiveFilterStrategy().process(stream)
            result = strategy.process(filtered)

            self.cache.save(cache_key, result)

            self.bus.publish("PROCESS_COMPLETE", column)
            return result

        except SystemError as e:
            self.bus.publish("SYSTEM_ERROR", str(e))
            return None


if __name__ == "__main__":
    sys = System("social_media_impact.csv")
    sys2 = System('worldometer_data.csv')
    sys3 = System('country_wise_latest.csv')

    result = sys.run("Age", StatsStrategy())
    result2 = sys2.run("TotalCases", StatsStrategy())
    result3 = sys3.run("Confirmed", StatsStrategy())

    print("\nRESULT:")
    for key, value in result.items():
        print(f"{key.capitalize()} : {value}")

    print("\nRESULT2:")
    for key, value in result2.items():
        print(f"{key.capitalize()} : {value}")

    print("\nRESULT3:")
    for key, value in result3.items():
        print(f"{key.capitalize()} : {value}")
