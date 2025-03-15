import os

from rapidocr import RapidOCR

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(BASE_DIR, "ocr_config.yaml")

ocr_engine = RapidOCR(config_path=config_path)

class OcrService:
    def ocr(self, image):
        return ocr_engine(image)
