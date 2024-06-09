import os
from Worker import Worker
from File import PetrelFile, ModelParams
from PyQt5 import QtCore, QtWidgets


class Ui_GridEclipseSerialization(object):
    def setupUi(self, GridEclipseSerialization):
        GridEclipseSerialization.setObjectName("GridEclipseSerialization")
        GridEclipseSerialization.resize(600, 170)

        self.centralwidget = QtWidgets.QWidget(GridEclipseSerialization)
        self.centralwidget.setObjectName("centralwidget")

        self.btn_find_file = QtWidgets.QPushButton(self.centralwidget)
        self.btn_find_file.setGeometry(QtCore.QRect(40, 20, 200, 30))
        self.btn_find_file.setObjectName("btn_find_file")

        self.btn_export_pickle = QtWidgets.QPushButton(self.centralwidget)
        self.btn_export_pickle.setGeometry(QtCore.QRect(40, 70, 200, 30))
        self.btn_export_pickle.setObjectName("btn_export_fake_cereal")

        self.btn_import_pickle = QtWidgets.QPushButton(self.centralwidget)
        self.btn_import_pickle.setGeometry(QtCore.QRect(40, 120, 200, 30))
        self.btn_import_pickle.setObjectName("btn_import_fake_cereal")

        self.btn_export_pyarrow = QtWidgets.QPushButton(self.centralwidget)
        self.btn_export_pyarrow.setGeometry(QtCore.QRect(360, 70, 200, 30))
        self.btn_export_pyarrow.setObjectName("btn_export_pyarrow")

        self.btn_import_pyarrow = QtWidgets.QPushButton(self.centralwidget)
        self.btn_import_pyarrow.setGeometry(QtCore.QRect(360, 120, 200, 30))
        self.btn_import_pyarrow.setObjectName("btn_import_pyarrow")

        self.label_file_info = QtWidgets.QLabel(self.centralwidget)
        self.label_file_info.setGeometry(QtCore.QRect(360, 20, 200, 30))
        self.label_file_info.setAlignment(QtCore.Qt.AlignRight|QtCore.Qt.AlignTrailing|QtCore.Qt.AlignVCenter)
        self.label_file_info.setObjectName("label_file_info")

        GridEclipseSerialization.setCentralWidget(self.centralwidget)
        self.retranslateUi(GridEclipseSerialization)
        QtCore.QMetaObject.connectSlotsByName(GridEclipseSerialization)

        self.file = None

        self.btn_find_file.clicked.connect(self.open_file)
        self.btn_export_pickle.clicked.connect(self.export_pickle_worker)
        self.btn_import_pickle.clicked.connect(self.import_pickle_worker)
        self.btn_export_pyarrow.clicked.connect(self.export_pyarrow_worker)
        self.btn_import_pyarrow.clicked.connect(self.import_pyarrow_worker)

    def retranslateUi(self, GridEclipseSerialization):
        _translate = QtCore.QCoreApplication.translate
        GridEclipseSerialization.setWindowTitle(_translate("GridEclipseSerialization", "GRDECL serializer"))
        self.btn_find_file.setText(_translate("GridEclipseSerialization", "Выбрать файл"))
        self.btn_export_pickle.setText(_translate("GridEclipseSerialization", "Экспорт сетки Pickle"))
        self.btn_import_pickle.setText(_translate("GridEclipseSerialization", "Импорт сетки Pickle"))
        self.btn_export_pyarrow.setText(_translate("GridEclipseSerialization", "Экспорт сетки (Apache Arrow)"))
        self.btn_import_pyarrow.setText(_translate("GridEclipseSerialization", "Импорт сетки (Apache Arrow)"))
        self.label_file_info.setText(_translate("GridEclipseSerialization", "Файл не выбран"))

    def open_file(self) -> None:
        file_path, _ = QtWidgets.QFileDialog.getOpenFileName(None, "Выберите файл", "./", "Text Files (*.GRDECL)")
        if file_path:
            self.read_file_worker(file_path)

    def read_file_worker(self, path: str) -> None:
        self.file = PetrelFile(path)
        self.obj = Worker(window=self, file=self.file)
        self.thread = QtCore.QThread()
        self.obj.moveToThread(self.thread)
        self.thread.started.connect(self.obj.load_from_file)
        self.obj.load_signal.connect(self.thread.quit)
        self.obj.load_signal.connect(self.fill_file_model)
        self.thread.start()

    def fill_file_model(self, d: dict) -> None:
        self.file.model.extract_from_dict(d)
        print(self.file.model)

    def check_file_is_available(self) -> bool:
        return self.file is not None

    def export_pickle_worker(self) -> None:
        if not self.check_file_is_available():
            print("Должен быть выбран файл.")
        else:
            self.obj = Worker(window=self, file=self.file)
            self.thread = QtCore.QThread()
            self.obj.moveToThread(self.thread)
            self.thread.started.connect(self.obj.export_pickle)
            self.obj.pickle_export_signal.connect(self.thread.quit)
            self.thread.start()

    def import_pickle_worker(self) -> ModelParams:
        if not self.check_file_is_available():
            print("Должен быть выбран файл.")
        elif not os.path.exists(os.path.join(self.file.path, "results/example.pickle")):
            print("Сначала нужно выполнить сериализацию Pickle.")
        else:
            self.obj = Worker(window=self, file=self.file)
            self.thread = QtCore.QThread()
            self.obj.moveToThread(self.thread)
            self.thread.started.connect(self.obj.import_pickle)
            self.obj.pickle_import_signal.connect(self.thread.quit)
            obj = self.obj.pickle_import_signal.connect(self.create_model_from_dict)
            self.thread.start()
            return obj

    @staticmethod
    def create_model_from_dict(d: dict) -> ModelParams:
        obj = ModelParams()
        obj.extract_from_dict(d)
        return obj

    def export_pyarrow_worker(self) -> None:
        if not self.check_file_is_available():
            print("Должен быть выбран файл.")
        else:
            self.obj = Worker(window=self, file=self.file)
            self.thread = QtCore.QThread()
            self.obj.moveToThread(self.thread)
            self.thread.started.connect(self.obj.export_pyarrow)
            self.obj.pyarrow_export_signal.connect(self.thread.quit)
            self.thread.start()

    def import_pyarrow_worker(self) -> ModelParams:
        if not self.check_file_is_available():
            print("Должен быть выбран файл.")
        elif not os.path.exists(os.path.join(self.file.path, "results/example.pyarrow")):
            print("Сначала нужно выполнить сериализацию APACHE ARROW.")
        else:
            self.obj = Worker(window=self, file=self.file)
            self.thread = QtCore.QThread()
            self.obj.moveToThread(self.thread)
            self.thread.started.connect(self.obj.import_pyarrow)
            self.obj.pyarrow_import_signal.connect(self.thread.quit)
            obj = self.obj.pyarrow_import_signal.connect(self.create_model_from_dict)
            self.thread.start()
            return obj


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    GridEclipseSerialization = QtWidgets.QMainWindow()
    ui = Ui_GridEclipseSerialization()
    ui.setupUi(GridEclipseSerialization)
    GridEclipseSerialization.show()
    sys.exit(app.exec_())
