#!/usr/bin/env python3
"""
WB Position Parser.

Для каждой строки в Google-таблице "ПРОГРЕВЫ":
  1. Извлекает артикул конкурента из ссылки (колонка G).
  2. Запрашивает у API Wildberries список "Смотрите также" для этой карточки.
  3. Ищет наш артикул (колонка B строки 2 листа) в первых 100 позициях.
  4. Пишет результат в колонку I и таймстамп в колонку H.

Запускается из GitHub Actions по расписанию или вручную.
Креды Service Account берутся из переменной окружения GOOGLE_CREDENTIALS.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
import requests
from google.oauth2.service_account import Credentials

# ---------- Конфиг ----------
SHEET_ID = os.environ.get(
    "SHEET_ID",
    "1m_anO1SgQaSTUVEcresy-_IWSCfBZDjnsFwnr6kT20w",
)

# Адаптивный темп: целимся в 90 минут (5400 с), но парсим до конца если не успеваем.
PLAN_WINDOW_SECONDS = 90 * 60
MIN_INTERVAL_SECONDS = 2.0
SEARCH_DEPTH = 100

TIMEZONE = ZoneInfo("Europe/Moscow")
REQUEST_TIMEOUT = 15
RETRY_COUNT = 1  # один повтор при сетевой ошибке

# Колонки (1-indexed)
COL_OUR_ARTICLE = "B"  # наш артикул, берём из B2
COL_COMPETITOR_URL = "G"  # ссылка на карточку конкурента
COL_TIMESTAMP = "H"  # дата проверки
COL_POSITION = "I"  # место / результат

START_ROW = 2

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("wb-parser")


# ---------- Google Sheets ----------
def get_google_client() -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_raw = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_raw:
        creds_info = json.loads(creds_raw)
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    elif os.path.exists("credentials.json"):
        creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
    else:
        raise RuntimeError(
            "Не найдены креды: задайте переменную окружения GOOGLE_CREDENTIALS "
            "или положите credentials.json рядом со скриптом."
        )
    return gspread.authorize(creds)


# ---------- WB API ----------
def extract_nm_id(url: str) -> int | None:
    if not url:
        return None
    m = re.search(r"/catalog/(\d+)/", url)
    if m:
        return int(m.group(1))
    m = re.search(r"[?&]nm=(\d+)", url)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d{7,})", url)
    if m:
        return int(m.group(1))
    return None


def _parse_nm_ids(data) -> list[int] | None:
    if isinstance(data, dict):
        for key in ("nmIds", "nms", "ids"):
            if key in data and isinstance(data[key], list):
                return [int(x) for x in data[key] if x]
        inner = data.get("data")
        if isinstance(inner, dict):
            for key in ("nmIds", "nms", "products"):
                if key in inner and isinstance(inner[key], list):
                    items = inner[key]
                    if items and isinstance(items[0], dict):
                        return [
                            int(p.get("id") or p.get("nmId") or 0)
                            for p in items
                            if (p.get("id") or p.get("nmId"))
                        ]
                    return [int(x) for x in items if x]
    elif isinstance(data, list):
        if not data:
            return []
        if isinstance(data[0], dict):
            return [
                int(p.get("id") or p.get("nmId") or 0)
                for p in data
                if (p.get("id") or p.get("nmId"))
            ]
        return [int(x) for x in data if x]
    return None


def fetch_similar_nm_ids(nm_id: int, session: requests.Session) -> list[int] | None:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://www.wildberries.ru",
        "Referer": f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx",
    }
    endpoints = [
        f"https://similar-goods.wildberries.ru/api/v3/similar?nm={nm_id}",
        f"https://similar-goods.wildberries.ru/api/v2/search/similar?nm={nm_id}",
    ]
    last_error: str | None = None
    got_404 = False
    for url in endpoints:
        try:
            resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            last_error = f"{type(e).__name__}: {e}"
            continue
        if resp.status_code == 404:
            got_404 = True
            continue
        if resp.status_code >= 400:
            last_error = f"HTTP {resp.status_code} от {url}"
            continue
        try:
            data = resp.json()
        except ValueError as e:
            last_error = f"Невалидный JSON от {url}: {e}"
            continue
        ids = _parse_nm_ids(data)
        if ids is not None:
            return ids
        last_error = f"Не удалось найти nm_id в ответе {url}"
    if got_404 and last_error is None:
        return None
    raise RuntimeError(last_error or "Неизвестная ошибка API WB")


def verify_card_exists(nm_id: int, session: requests.Session) -> bool:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
    }
    urls = [
        f"https://card.wb.ru/cards/v2/detail?appType=1&curr=rub&dest=-1257786&nm={nm_id}",
        f"https://card.wb.ru/cards/v1/detail?appType=1&curr=rub&dest=-1257786&nm={nm_id}",
    ]
    for url in urls:
        try:
            r = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if r.status_code >= 400:
                continue
            data = r.json()
            products = (data.get("data") or {}).get("products") or []
            if products:
                return True
        except Exception:
            continue
    return False


# ---------- Основной пайплайн ----------
def now_msk_str() -> str:
    return datetime.now(TIMEZONE).strftime("%d.%m.%Y %H:%M")


def collect_tasks(spreadsheet: gspread.Spreadsheet) -> list[dict]:
    tasks: list[dict] = []
    for ws in spreadsheet.worksheets():
        try:
            values = ws.get_all_values()
        except Exception as e:
            logger.warning(f"Не удалось прочитать лист {ws.title}: {e}")
            continue
        if len(values) < 2:
            logger.info(f"Лист {ws.title}: пустой, пропускаю")
            continue
        row2 = values[1]
        our_article = row2[1].strip() if len(row2) > 1 else ""
        if not our_article or not our_article.isdigit():
            logger.info(
                f"Лист {ws.title}: в B2 нет числового артикула ({our_article!r}), пропускаю"
            )
            continue
        added = 0
        for idx, row in enumerate(values[1:], start=2):
            url = row[6].strip() if len(row) > 6 else ""
            if not url:
                continue
            tasks.append(
                {
                    "sheet_title": ws.title,
                    "row": idx,
                    "our_article": our_article,
                    "competitor_url": url,
                }
            )
            added += 1
        logger.info(f"Лист {ws.title}: добавлено {added} строк к обработке")
    return tasks


def process_task(task: dict, session: requests.Session) -> tuple[str, str]:
    ts = now_msk_str()
    nm_id = extract_nm_id(task["competitor_url"])
    if not nm_id:
        logger.warning(f"Не извлёк nm_id из URL: {task['competitor_url']}")
        return "ошибка", ts

    last_exc: Exception | None = None
    ids: list[int] | None = None
    got_none = False
    for attempt in range(RETRY_COUNT + 1):
        try:
            ids = fetch_similar_nm_ids(nm_id, session)
            if ids is None:
                got_none = True
            break
        except Exception as e:
            last_exc = e
            if attempt < RETRY_COUNT:
                time.sleep(2)
            continue

    if got_none or (ids is None and last_exc is None):
        if not verify_card_exists(nm_id, session):
            return "Конкурент не найден", now_msk_str()
        return "ошибка", now_msk_str()

    if ids is None:
        logger.warning(f"Ошибка API WB для nm={nm_id}: {last_exc}")
        return "ошибка", now_msk_str()

    our_id = int(task["our_article"])
    if our_id in ids:
        position = ids.index(our_id) + 1
        if position > SEARCH_DEPTH:
            return "Дальше 100 поз.", now_msk_str()
        return str(position), now_msk_str()
    return "Дальше 100 поз.", now_msk_str()


def flush_updates(
    spreadsheet: gspread.Spreadsheet,
    updates_by_sheet: dict[str, list[dict]],
) -> None:
    for sheet_title, ups in updates_by_sheet.items():
        if not ups:
            continue
        try:
            ws = spreadsheet.worksheet(sheet_title)
            ws.batch_update(ups, value_input_option="USER_ENTERED")
            logger.info(f"Лист {sheet_title}: записано {len(ups)} обновлений")
        except Exception as e:
            logger.error(f"Ошибка записи в лист {sheet_title}: {e}")
    updates_by_sheet.clear()


def main() -> int:
    logger.info("=== WB Position Parser запущен ===")
    gc = get_google_client()
    spreadsheet = gc.open_by_key(SHEET_ID)
    logger.info(f"Открыта таблица: {spreadsheet.title}")

    tasks = collect_tasks(spreadsheet)
    total = len(tasks)
    logger.info(f"Всего строк к обработке: {total}")
    if total == 0:
        logger.info("Нечего делать, выхожу.")
        return 0

    interval = max(MIN_INTERVAL_SECONDS, PLAN_WINDOW_SECONDS / total)
    estimated_min = (interval * total) / 60
    logger.info(
        f"Интервал между запросами: {interval:.1f} с "
        f"(ожидаемая длительность ≈ {estimated_min:.0f} мин)"
    )
    if estimated_min > 90:
        logger.warning(
            f"Прогнозируемое время ({estimated_min:.0f} мин) превышает целевое окно 90 мин. "
            "Парсер продолжит работу до конца."
        )

    session = requests.Session()
    updates_by_sheet: dict[str, list[dict]] = {}
    started_all = time.monotonic()

    for i, task in enumerate(tasks, start=1):
        iter_started = time.monotonic()
        position_text, ts = process_task(task, session)
        logger.info(
            f"[{i}/{total}] {task['sheet_title']}!{task['row']} "
            f"→ позиция: {position_text}"
        )
        updates_by_sheet.setdefault(task["sheet_title"], []).append(
            {
                "range": f"{COL_TIMESTAMP}{task['row']}:{COL_POSITION}{task['row']}",
                "values": [[ts, position_text]],
            }
        )
        if i % 20 == 0 or i == total:
            flush_updates(spreadsheet, updates_by_sheet)

        if i < total:
            elapsed = time.monotonic() - iter_started
            sleep_for = max(0.0, interval - elapsed)
            jitter = sleep_for * random.uniform(-0.1, 0.1)
            sleep_for = max(0.5, sleep_for + jitter)
            time.sleep(sleep_for)

    total_min = (time.monotonic() - started_all) / 60
    logger.info(f"=== Готово. Всего обработано {total} строк за {total_min:.1f} мин ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
