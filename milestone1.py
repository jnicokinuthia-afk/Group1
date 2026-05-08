class Dataset:
    def _init_(self, data):
        if not data:
            raise ValueError("Dataset cannot be empty")
        self._data = data

    def get_data(self):
        return self._data


class StatisticEngine:
    def mean(self, data):
        return sum(data) / len(data)

    def variance(self, data):
        mean = self.mean(data)
        return sum((x - mean) ** 2 for x in data) / len(data)

    def median(self, data):
        sorted_data = sorted(data)
        n = len(data)
        mid = n // 2

        if n % 2 == 0:
            return (sorted_data[mid - 1] + sorted_data[mid]) / 2
        return sorted_data[mid]

    def minimum(self, data):
        return min(data)

    def maximum(self, data):
        return max(data)


class AnalysisSystem:
    def _init_(self):
        self.engine = StatisticEngine()

    def run(self, data):
        dataset = Dataset(data)
        values = dataset.get_data()

        print("Mean:", self.engine.mean(values))
        print("Variance:", self.engine.variance(values))
        print("Median:", self.engine.median(values))
        print("Minimum:", self.engine.minimum(values))
        print("Maximum:", self.engine.maximum(values))


if __name__ == "_main_":
    try:
        raw = input("Enter numbers separated by a space: ").strip()
        data = list(map(float, raw.split()))

        system = AnalysisSystem()
        system.run(data)

    except ValueError as e:
        print("Error:", e)
