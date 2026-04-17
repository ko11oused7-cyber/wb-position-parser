#!/usr/bin/env python3
"""
WB Position Parser (browser-based).

Открывает карточки конкурентов из Google-таблицы "ПРОГРЕВЫ" в headless Chromium,
находит блок "Смотрите также" и ищет в нём ваш артикул.
Пишет результат в колонку I, таймстамп в колонку H.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import random
import re
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import (
    Browser,
    BrowserContext,
    Page,
    TimeoutError as PWTimeout,
    sync_playwright,
)

SHEET_ID = os.environ.get(
    "SHEET_ID",
    "1m_anO1SgQaSTUVEcresy-_IWSCfBZDjnsFwnr6kT20w",
)

PLAN_WINDOW_SECONDS = 90 * 60
MIN_INTERVAL_SECONDS = 12.0
SEARCH_DEPTH = 100
MAX_ITEMS_TO_COLLECT = 120

TIMEZONE = ZoneInfo("Europe/Moscow")
NAV_TIMEOUT_MS = 35000
DOM_TIMEOUT_MS = 20000

FAILURE_DIR = pathlib.Path("failures")
MAX_FAILURE_DUMPS = 10

COL_OUR_ARTICLE = "B"
COL_COMPETITOR_URL = "G"
COL_TIMESTAMP = "H"
COL_POSITION = "I"

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
            "Не найдены креды: задайте GOOGLE_CREDENTIALS или положите credentials.json рядом."
        )
    return gspread.authorize(creds)


SIMILAR_EXTRACTION_JS = r"""
() => {
    const RE_SIMILAR = /смотрите\s+также|похожие\s+товары|с\s+этим\s+(?:товаром|смотрят)|похожие|рекоменд|вместе\s+с\s+этим/i;
    const RE_CATALOG = /\/catalog\/(\d+)\//;

    function findHeader() {
        const candidates = document.querySelectorAll('h1, h2, h3, h4, h5, h6, div, span, p');
        for (const el of candidates) {
            if (el.children.length > 3) continue;
            const t = (el.textContent || '').trim();
            if (t.length < 5 || t.length > 60) continue;
            if (RE_SIMILAR.test(t)) return el;
        }
        return null;
    }

    function containerOf(el) {
        let node = el;
        for (let i = 0; i < 6 && node.parentElement; i++) {
            node = node.parentElement;
            const links = node.querySelectorAll('a[href*="/catalog/"]');
            if (links.length >= 4) return node;
        }
        return node;
    }

    function collectIds(root) {
        const links = root.querySelectorAll('a[href*="/catalog/"]');
        const ids = [];
        const seen = new Set();
        for (const a of links) {
            const href = a.getAttribute('href') || '';
            const m = href.match(RE_CATALOG);
            if (!m) continue;
            const id = parseInt(m[1], 10);
            if (!id || seen.has(id)) continue;
            seen.add(id);
            ids.push(id);
        }
        return ids;
    }

    const header = findHeader();
    if (!header) return { found: false, ids: [] };
    const cont = containerOf(header);
    return { found: true, ids: collectIds(cont) };
}
"""


def ensure_consent(page: Page) -> None:
    try:
        page.evaluate(
            """
            () => {
                const btns = [...document.querySelectorAll('button, a')];
                for (const b of btns) {
                    const t = (b.textContent || '').trim().toLowerCase();
                    if (t === 'принять' || t === 'ок' || t === 'хорошо' ||
                        t.includes('подтвердить') || t.includes('accept')) {
                        b.click();
                    }
                }
            }
            """
        )
    except Exception:
        pass


_failure_dumps_saved = 0


def save_failure_artifact(page: Page, nm_id: int | None, reason: str) -> None:
    global _failure_dumps_saved
    if _failure_dumps_saved >= MAX_FAILURE_DUMPS:
        return
    try:
        FAILURE_DIR.mkdir(exist_ok=True)
        tag = f"{nm_id or 'unknown'}_{reason}"
        page.screenshot(path=str(FAILURE_DIR / f"{tag}.png"), full_page=True)
        try:
            html = page.content()
            (FAILURE_DIR / f"{tag}.html").write_text(html[:600_000], encoding="utf-8")
        except Exception:
            pass
        _failure_dumps_saved += 1
        logger.info(f"Сохранён артефакт: failures/{tag}.png")
    except Exception as e:
        logger.warning(f"Не удалось сохранить скриншот: {e}")


def fetch_similar_nm_ids(
    page: Page, url: str, nm_id: int | None = None
) -> tuple[list[int] | None, str | None]:
    try:
        resp = page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
    except PWTimeout:
        return None, "timeout"
    except Exception as e:
        return None, f"error:{type(e).__name__}"

    if resp is not None and resp.status in (404, 410):
        return None, "404"
    if resp is not None and resp.status >= 500:
        return None, f"error:HTTP{resp.status}"

    try:
        page.wait_for_load_state("networkidle", timeout=8000)
    except PWTimeout:
        pass

    try:
        title = (page.title() or "").strip()
        final_url = page.url
        logger.info(f"  → страница: {final_url} | title: {title!r}")
        low = title.lower()
        if "не найден" in low or "404" in low or "not found" in low:
            return None, "404"
    except Exception:
        pass

    ensure_consent(page)

    header_visible = False
    last_h = 0
    for _ in range(30):
        try:
            found = page.evaluate(
                """
                () => {
                    const RE = /смотрите\\s+также|похожие\\s+товары|с\\s+этим\\s+(?:товаром|смотрят)|похожие|рекоменд|вместе\\s+с\\s+этим/i;
                    const all = document.querySelectorAll('h1, h2, h3, h4, h5, h6, div, span, p');
                    for (const el of all) {
                        if (el.children.length > 3) continue;
                        const t = (el.textContent || '').trim();
                        if (t.length < 5 || t.length > 60) continue;
                        if (RE.test(t)) {
                            el.scrollIntoView({block: 'center', behavior: 'instant'});
                            return t;
                        }
                    }
                    return null;
                }
                """
            )
            if found:
                header_visible = True
                logger.info(f"  → найден заголовок блока: {found!r}")
                break
        except Exception:
            pass
        page.mouse.wheel(0, 1500)
        page.wait_for_timeout(600)
        try:
            cur_h = page.evaluate("document.body.scrollHeight")
        except Exception:
            cur_h = last_h
        last_h = cur_h

    if not header_visible:
        title = ""
        try:
            title = page.title() or ""
        except Exception:
            pass
        if "не найден" in title.lower() or "404" in title:
            save_failure_artifact(page, nm_id, "404")
            return None, "404"
        try:
            cnt = page.evaluate(
                "document.querySelectorAll('a[href*=\"/catalog/\"]').length"
            )
            logger.info(f"  → блок не найден; всего ссылок /catalog/: {cnt}")
        except Exception:
            pass
        save_failure_artifact(page, nm_id, "no_block")
        return None, "no_block"

    prev_count = -1
    stable = 0
    ids: list[int] = []
    for _ in range(40):
        try:
            data = page.evaluate(SIMILAR_EXTRACTION_JS)
        except Exception:
            data = {"found": True, "ids": []}
        ids = data.get("ids", []) if isinstance(data, dict) else []
        if len(ids) >= MAX_ITEMS_TO_COLLECT:
            return ids[:MAX_ITEMS_TO_COLLECT], None
        if len(ids) == prev_count:
            stable += 1
            if stable >= 4:
                break
        else:
            stable = 0
        prev_count = len(ids)
        page.mouse.wheel(0, 900)
        page.wait_for_timeout(550)

    if not ids:
        return None, "no_block"
    return ids, None


def now_msk_str() -> str:
    return datetime.now(TIMEZONE).strftime("%d.%m.%Y %H:%M")


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


def process_task(page: Page, task: dict) -> tuple[str, str]:
    ts = now_msk_str()
    nm_id = extract_nm_id(task["competitor_url"])
    if not nm_id:
        logger.warning(f"Некорректный URL: {task['competitor_url']}")
        return "ошибка", ts

    ids, reason = fetch_similar_nm_ids(page, task["competitor_url"], nm_id)
    if ids is None and reason and (reason == "timeout" or reason.startswith("error:")):
        logger.info(f"Повтор для {task['competitor_url']} (причина: {reason})")
        time.sleep(3)
        ids, reason = fetch_similar_nm_ids(page, task["competitor_url"], nm_id)

    if ids is None:
        if reason == "404":
            return "Конкурент не найден", now_msk_str()
        logger.warning(f"Не получил блок для {task['competitor_url']}: {reason}")
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
            logger.error(f"Ошибка записи в {sheet_title}: {e}")
    updates_by_sheet.clear()


def build_context(browser: Browser) -> BrowserContext:
    ua = random.choice(USER_AGENTS)
    ctx = browser.new_context(
        user_agent=ua,
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        viewport={"width": 1440, "height": 900},
        extra_http_headers={
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    )
    ctx.set_default_timeout(DOM_TIMEOUT_MS)
    ctx.set_default_navigation_timeout(NAV_TIMEOUT_MS)
    def _route(route):
        if route.request.resource_type in {"image", "media", "font"}:
            return route.abort()
        return route.continue_()
    ctx.route("**/*", _route)
    return ctx


def main() -> int:
    logger.info("=== WB Position Parser (browser) запущен ===")
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
    est_min = (interval * total) / 60
    logger.info(
        f"Интервал между карточками: {interval:.1f} с "
        f"(ожидаемая длительность ≈ {est_min:.0f} мин)"
    )

    started_all = time.monotonic()
    updates_by_sheet: dict[str, list[dict]] = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        ctx = build_context(browser)
        page = ctx.new_page()

        for i, task in enumerate(tasks, start=1):
            iter_started = time.monotonic()
            try:
                position_text, ts = process_task(page, task)
            except Exception as e:
                logger.error(
                    f"Непредвиденная ошибка на {task['sheet_title']}!{task['row']}: {e}"
                )
                position_text, ts = "ошибка", now_msk_str()
                try:
                    page.close()
                    ctx.close()
                except Exception:
                    pass
                ctx = build_context(browser)
                page = ctx.new_page()

            logger.info(
                f"[{i}/{total}] {task['sheet_title']}!{task['row']} → {position_text}"
            )
            updates_by_sheet.setdefault(task["sheet_title"], []).append(
                {
                    "range": f"{COL_TIMESTAMP}{task['row']}:{COL_POSITION}{task['row']}",
                    "values": [[ts, position_text]],
                }
            )
            if i % 10 == 0 or i == total:
                flush_updates(spreadsheet, updates_by_sheet)

            if i < total:
                elapsed = time.monotonic() - iter_started
                sleep_for = max(0.0, interval - elapsed)
                jitter = sleep_for * random.uniform(-0.1, 0.1)
                sleep_for = max(0.3, sleep_for + jitter)
                time.sleep(sleep_for)

        try:
            page.close()
            ctx.close()
            browser.close()
        except Exception:
            pass

    total_min = (time.monotonic() - started_all) / 60
    logger.info(f"=== Готово. Обработано {total} строк за {total_min:.1f} мин ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
