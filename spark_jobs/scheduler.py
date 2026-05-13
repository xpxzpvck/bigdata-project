import time
import subprocess
from datetime import datetime

def run_batch_jobs():
    print(f"[{datetime.now()}] Починаємо запуск батч-джобів...")
    try:
        # Запускаємо генерацію погодинного звіту
        # Зверни увагу на --master - ми відправляємо джобу на наш Spark Master
        subprocess.run([
            "spark-submit", 
            "--master", "spark://spark-master:7077", 
            "batch/hourly_report_job.py"
        ], check=True)
        
        # Запускаємо аналіз патернів поведінки авторів
        subprocess.run([
            "spark-submit", 
            "--master", "spark://spark-master:7077", 
            "batch/editor_patterns_job.py"
        ], check=True)
        
        print(f"[{datetime.now()}] Всі батч-джоби успішно завершені!")
    except subprocess.CalledProcessError as e:
        print(f"[{datetime.now()}] Помилка під час виконання Spark-джоби: {e}")

if __name__ == "__main__":
    print("Батч-скедулер запущено. Запускаємо перший прогін через 10 секунд...")
    time.sleep(10) # Даємо кластеру Spark час на повне підняття
    
    while True:
        run_batch_jobs()
        
        print(f"[{datetime.now()}] Спимо 1 годину до наступного запуску...")
        # Чекаємо 3600 секунд (1 година). 
        # Під час розробки та тестування можеш замінити на 300 (5 хвилин).
        time.sleep(3600)