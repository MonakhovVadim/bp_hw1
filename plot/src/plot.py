import logging
import time
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

log_dir = Path.cwd() / "logs"
if not log_dir.exists():
    log_dir.mkdir()


while True:
    try:
        # Чтение данных
        df = pd.read_csv(log_dir / "metric_log.csv")

        # Построение гистограммы
        sns.displot(df["absolute_error"], kde=True)

        # Сохранение графика
        plt.savefig(log_dir / "error_distribution.png")

    except Exception as e:
        logging.exception(
            "Не удалось сформировать график метрик, возможно отсутствуют данные"
        )
    finally:
        time.sleep(2)
