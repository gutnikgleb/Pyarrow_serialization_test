import time
import os
import pickle
import pyarrow as pa
from functools import wraps


def measure_execution_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Время выполнения {func.__name__}: {round(execution_time, 8)} секунд.")
        return result
    return wrapper


class ModelParams:

    def load_from_file(self):
        d = {}
        with open(os.path.join(self.file.path, self.file.file_name), 'r') as file:
            level = 0
            data = []
            for line in file:
                line = line.strip().split()
                if len(line) == 0:
                    level = 0
                    data = []
                elif line[0].startswith('--'):
                    pass
                elif level > 0:
                    data.extend(line)
                    if line[-1].endswith('/'):
                        data = []
                        level = 0
                else:
                    d[line[0]] = data
                    level += 1
        return d

    def extract_from_dict(self, d: dict):
        for k, v in d.items():
            self.__setattr__(k, v)
        self.show_len()
        return self

    @property
    def params(self):
        return tuple(self.__dict__.keys())

    def show_len(self):
        for k, v in self.__dict__.items():
            print(k, len(v))

    def __getitem__(self, item: str):
        return self.__dict__[item]

    def __str__(self):
        return f"ModelParams: {', '.join(self.params)}"


class PetrelFile:
    def __init__(self, path: str):
        path, name = os.path.split(path)
        if not name.lower().endswith('.grdecl'):
            raise Exception('Некорректное расширение переданного файла. Должно быть ".GRDECL".')

        self.path = path
        self.file_name = name
        self.model = ModelParams()

    @measure_execution_time
    def pickle_serialize_to_disk(self) -> None:
        os.chdir(self.path)
        if not os.path.exists('results'):
            os.mkdir('results')
        with open('results/example.pickle', 'wb') as file:
            # pickle.dump(self, file)
            pickle.dump(self.model.__dict__, file)

    @measure_execution_time
    def pickle_deserialize_from_disk(self) -> dict:
        with open('results/example.pickle', 'rb') as file:
            result = pickle.load(file)
        return result

    @measure_execution_time
    def pyarrow_data_prepare(self):
        data = [[v] for v in self.model.__dict__.values()]
        batch = pa.record_batch(data, names=list(self.model.__dict__.keys()))
        return batch

    @measure_execution_time
    def pyarrow_serialize_to_disk(self, batch) -> None:
        os.chdir(self.path)
        if not os.path.exists('results'):
            os.mkdir('results')
        with pa.ipc.new_file('results/example.pyarrow', batch.schema) as writer:
            writer.write(batch)

    @measure_execution_time
    def pyarrow_deserialize_from_disk(self) -> dict:
        with pa.memory_map('results/example.pyarrow', 'r') as source:
            loaded_arrays = pa.ipc.open_file(source).read_all()
        result = self.create_dict_from_patable(loaded_arrays)
        return result

    @staticmethod
    def create_dict_from_patable(table: pa.Table) -> dict:
        d = {}
        for col_name in table.column_names:
            values = table.column(col_name)
            d[col_name] = values[0]
        return d

    def __str__(self):
        return f"PetrelFile: {self.file_name}\n" \
               f"Path: {self.path}\n" \
               f"{str(self.model)}\n"


if __name__ == '__main__':
    pf = PetrelFile('example.GRDECL')
    pf.model.extract_from_dict(pf.model.load_from_file())
