from abc import abstractmethod, ABC


class OcrService(ABC):

    @abstractmethod
    def ocr(self, image):
        pass
