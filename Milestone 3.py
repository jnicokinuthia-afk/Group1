import csv
import time
import statistics
from functools import reduce
import matplotlib.pyplot as plt


class SystemBaseException(Exception):
    pass


class DataIntegrityError(SystemBaseException):
    pass


class ConfigError(SystemBaseException):
    pass


class PerformanceProfiler:
    def __init__(self):
        self.logs = []

    def log_time(self, task_name, start_time):
        duration = time.time() - start_time
        self.logs.append(f"Task '{task_name}' took {duration:6f} seconds")

    def display_report(self):
        print("\n" + "-" * 30)
        print("Performance Report")
        print("-" * 30)
        for log in self.logs:
            print(log)


class DataHandler:
    def __init__(self, filepath):
        self.filepath = filepath
        self.name = filepath
        self.profiler = PerformanceProfiler()
        self._raw_data = []
        self._processed_cache = {}

    def load(self):
        start = time.time()
        try:
            with open(self.filepath, mode='r', encoding='utf-8') as f:
                # Advanced Structure: List of Dictionaries
                self._raw_data = list(csv.DictReader(f))

            self.profiler.log_time("CSV Loading", start)

        except Exception as e:
            raise DataIntegrityError(f"I/O Error: {e}")

    def cached_result(self, column, result):
        self._processed_cache[column] = result

    def get_cached(self, column):
        return self._processed_cache.get(column)

    def get_iterator(self, column):
        for row in self._raw_data:
            try:
                yield float(row[column])
            except (ValueError, KeyError, TypeError):
                continue


class FunctionalPipeline:
    def __init__(self, data_stream):
        self.data = list(data_stream)

    def apply_filter(self, criteria_func):
        self.data = list(filter(criteria_func, self.data))
        return self

    def calculate_metrics(self):
        if not self.data:
            return {}
        # Paradigms
        count = len(self.data)
        total = reduce(lambda x, y: x + y, self.data)
        mean = total / count
        variance = statistics.variance(self.data)
        stddev = statistics.stdev(self.data)
        maximum = max(self.data)
        minimum = min(self.data)

        return {
            "count ": count,
            "sum ": total,
            "mean ": mean,
            "variance ": variance,
            "std dev ": stddev,
            "Max ": maximum,
            "Min": minimum
        }

    def plot(self, chart_type="hist"):
        if not self.data:
            print("No data to plot")
            return

        plt.figure()

        if chart_type == "hist":
            plt.hist(self.data)
            plt.title("Histogram")

        elif chart_type == "line":
            plt.plot(self.data)
            plt.title("Line")

        elif chart_type == "box":
            plt.boxplot(self.data)
            plt.title("Boxplot")

        else:
            print("Unsupported chart type")
            return

        plt.xlabel("Index / Bins")
        plt.ylabel("Values")
        plt.show()

        plt.show(block=False)
        plt.pause(0.5)


class M3System:
    def __init__(self, file):
        self.handler = DataHandler(file)
        self.handler.load()

    def run_analysis(self, column):
        print(f"\nProcessing File: {self.handler.filepath}")
        print(f"\nProcessing Column: {column}")
        start = time.time()

        col = self.handler.get_iterator(column)
        pipeline = FunctionalPipeline(col)

        cached = self.handler.get_cached(column)
        if cached:
            print("Using cached data")
            results = cached
        else:
            pipeline.apply_filter(lambda x: x > 0)
            results = pipeline.calculate_metrics()
            self.handler.cached_result(column, results)

        self.handler.profiler.log_time("Data Processing", start)

        print("-"*20)
        for key, value in results.items():
            print(f"{key.upper()}:{value:,.2f}")

        self.handler.profiler.display_report()


if __name__ == "__main__":
    sys = M3System('country_wise_latest.csv')
    sys2 = M3System('worldometer_data.csv')
    # sys3 = M3System('social_media_impact.csv')

    sys.run_analysis('Confirmed')
    sys.run_analysis('Deaths')

    sys2.run_analysis('Population')
    sys2.run_analysis('TotalCases')

#    sys3.run_analysis('Mental_Health_Score')
#    sys3.run_analysis('Age')
