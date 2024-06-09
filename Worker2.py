import os
from PyQt5 import QtCore
from File import measure_execution_time


class Worker(QtCore.QObject):
    load_signal = QtCore.pyqtSignal(dict)
    pickle_export_signal = QtCore.pyqtSignal(int)
    pickle_import_signal = QtCore.pyqtSignal(dict)
    pyarrow_export_signal = QtCore.pyqtSignal(int)
    pyarrow_import_signal = QtCore.pyqtSignal(dict)

    def __init__(self, window, file):
        super(Worker, self).__init__()
        # self.window = window
        self.file = file

    @measure_execution_time
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
        # self.window.label_file_info.setText(f"{self.file.file_name}")
        self.load_signal.emit(d)

    def export_pickle(self):
        self.file.pickle_serialize_to_disk()
        self.pickle_export_signal.emit(1)

    def import_pickle(self):
        result = self.file.pickle_deserialize_from_disk()
        self.pickle_import_signal.emit(result)

    def export_pyarrow(self):
        batch = self.file.pyarrow_data_prepare()
        self.file.pyarrow_serialize_to_disk(batch)
        self.pyarrow_export_signal.emit(1)

    def import_pyarrow(self):
        result = self.file.pyarrow_deserialize_from_disk()
        self.pyarrow_import_signal.emit(result)