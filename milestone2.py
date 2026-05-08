class Dataset:
    def _init_(self, data):
        if not data:
            raise ValueError("Dataset cannot be empty")
        self._data = data

    def get_data(self):
        return self._data


class StatisticOperation:
    def compute(self, data):
        raise NotImplementedError("Subclasses must implement this method")


class MeanOperation(StatisticOperation):
    def compute(self, data):
        mean = sum(data) / len(data)
        return mean


class VarianceOperation(StatisticOperation):
    def compute(self, data):
        mean = sum(data) / len(data)
        return sum((x - mean) ** 2 for x in data) / len(data)


class MedianOperation(StatisticOperation):
    def compute(self, data):
        sorted_data = sorted(data)
        n = len(data)
        mid = n // 2

        if n % 2 == 0:
            return (sorted_data[mid - 1] + sorted_data[mid]) / 2
        return sorted_data[mid]


class MinOperation(StatisticOperation):
    def compute(self, data):
        return min(data)


class MaxOperation(StatisticOperation):
    def compute(self, data):
        return max(data)


class AnalysisSystem:
    def _init_(self):
        self.operations = {
            "Mean": MeanOperation(),
            "Variance": VarianceOperation(),
            "Median": MedianOperation(),
            "Minimum": MinOperation(),
            "Maximum": MaxOperation(),
        }

    def run(self, data):
        dataset = Dataset(data)
        values = dataset.get_data()

        print("\n ----- Analysis Output -----")
        for name, operation in self.operations.items():
            result = operation.compute(values)
            print(f"{name}: {result}")


if __name__ == "_main_":
    try:
        raw = input("Enter numbers separated by a space: ").strip()
        data = list(map(float, raw.split()))

        system = AnalysisSystem()
        system.run(data)

    except ValueError as e:
        print("Error:", e)
