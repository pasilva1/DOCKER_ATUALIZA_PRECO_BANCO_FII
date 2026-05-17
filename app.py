import requests
import psycopg2
import time
import logging
from psycopg2.extras import RealDictCursor
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone

# ==================================================
# LOGGING
# ==================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ==================================================
# CONFIGURAÇÕES
# ==================================================
DB_CONFIG = {
    "host": "192.168.1.8",   # ip do banco
    "port": 5432,
    "dbname": "mvp_db",
    "user": "mvp_user",
    "password": "mvp_password"
}

API_URL = "http://192.168.1.8:5000/quote"
TIMEOUT_SECONDS = 10
SLEEP_BETWEEN_CALLS = 1

TZ = timezone("America/Recife")

# ==================================================
# JOB PRINCIPAL
# ==================================================
def atualizar_precos():
    logging.info("🚀 Início do job de atualização de preços")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT
            id,
            symbol,
            price AS current_price
        FROM vw_asset_price_current
        ORDER BY symbol
    """)

    assets = cur.fetchall()
    logging.info(f"📊 {len(assets)} ativos encontrados")

    for asset in assets:
        asset_id = asset["id"]
        symbol = asset["symbol"]
        old_price = asset["current_price"]

        try:
            response = requests.get(
                f"{API_URL}/{symbol}",
                timeout=TIMEOUT_SECONDS
            )

            if response.status_code != 200:
                logging.warning(f"{symbol} | HTTP {response.status_code}")
                continue

            payload = response.json()
            new_price = payload["data"]["regularMarketPrice"]

            logging.info(f"{symbol} | {old_price} → {new_price}")

            cur.execute(
                """
                INSERT INTO price_history (asset_id, price, price_date)
                VALUES (%s, %s, CURRENT_DATE)
                """,
                (asset_id, new_price)
            )

        except Exception as e:
            logging.error(f"{symbol} | erro | {e}")

        time.sleep(SLEEP_BETWEEN_CALLS)

    conn.commit()
    cur.close()
    conn.close()

    logging.info("✅ Job finalizado com sucesso")

# ==================================================
# SCHEDULER
# ==================================================
if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone=TZ)

    scheduler.add_job(
        atualizar_precos,
        trigger=CronTrigger(
            day_of_week="mon,tue,wed,thu,fri",
            hour=10,
            minute=55
        ),
        id="atualiza_preco_banco_fii",
        max_instances=1,
        misfire_grace_time=900  # 15 minutos
    )

    logging.info("🕘 Scheduler ativo | Seg–Sex | 09:00")
    scheduler.start()
