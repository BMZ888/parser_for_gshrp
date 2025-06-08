# run_parser.py
import sys
import os

# Добавляем папку src в системные пути, чтобы Python мог найти наши модули
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Теперь импортируем функцию запуска парсера из нашего модуля
from raw_data_parser.main_parser import run_petrovich_parser

if __name__ == "__main__":
    print("--- Запуск модуля сбора сырых данных ---")
    run_petrovich_parser()
    print("--- Работа модуля сбора сырых данных завершена ---")
