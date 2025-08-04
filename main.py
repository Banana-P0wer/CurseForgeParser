#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import contextlib
import csv
import datetime as dt
import os
import random
import re
import sys
import traceback
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup

BASE_URL = "https://www.curseforge.com"
SEARCH_PATH = "/minecraft/search"

DEFAULT_CSV_PATH = "curseforge_dataset.csv"
DEFAULT_LOG_PATH = "curseforge.log"

CSV_HEADERS = [
    "id",  # числовой идентификатор проекта на CurseForge (может быть пустым при парсинге листинга)
    "slug",  # человекочитаемый идентификатор (для URL)
    "name",  # название проекта
    "description",  # краткое описание проекта
    "created_at",  # дата создания проекта, ISO 8601 (YYYY-MM-DD)
    "updated_at",  # дата последнего обновления, ISO 8601 (YYYY-MM-DD)
    "downloads",  # общее число загрузок, integer
    "size",  # размер последнего релиза, строка "1.38 MB"
    "game_version",  # основная/последняя поддерживаемая версия
    "is_forge",  # зарезервировано, пока не собирается
    "is_fabric",  # зарезервировано, пока не собирается
    "is_neoforge",  # зарезервировано, пока не собирается
    "is_quilt",  # зарезервировано, пока не собирается
    "authors",  # "A; B; C"
    "categories",  # "Utility; Performance"
    "license",  # тип лицензии, если указан (в листинге чаще недоступен)
    "project_url",  # ссылка на страницу проекта
    "crawled_at"  # время сбора данных, ISO 8601 (UTC, Z)
]

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
}


def log(msg: str, file) -> None:
    print(msg)
    file.write(msg + "\n")
    file.flush()


def parse_mmddyyyy(text: str) -> Optional[str]:
    if not text:
        return None
    raw = text.strip()
    m = re.match(r"^([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})$", raw)
    if not m:
        return None
    month_name, day, year = m.group(1).lower(), int(m.group(2)), int(m.group(3))
    month = MONTHS.get(month_name)
    if not month:
        return None
    try:
        return f"{year:04d}-{month:02d}-{day:02d}"
    except ValueError:
        return None


def parse_downloads(text: str) -> Optional[int]:
    if not text:
        return None
    raw = text.strip().replace(",", "")
    m = re.match(r"^(\d+(?:\.\d+)?)([KkMmBb])?$", raw)
    if m:
        val = float(m.group(1))
        suffix = m.group(2)
        mult = 1
        if suffix:
            s = suffix.upper()
            if s == "K":
                mult = 1_000
            elif s == "M":
                mult = 1_000_000
            elif s == "B":
                mult = 1_000_000_000
        return int(round(val * mult))
    m2 = re.search(r"\d+", raw)
    return int(m2.group(0)) if m2 else None


def now_utc_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def extract_slug(href: str) -> str:
    if not href:
        return ""
    parts = href.strip("/").split("/")
    return parts[-1] if parts else ""


def parse_card(card) -> Dict[str, Any]:
    name_node = card.select_one("a.name span") or card.select_one("a.name")
    name = name_node.get_text(strip=True) if name_node else ""

    author_nodes = card.select("span.author a.author-name span, span.author a.author-name")
    authors = "; ".join({n.get_text(strip=True) for n in author_nodes if n.get_text(strip=True)})

    description = ""
    desc_node = card.select_one("p.description")
    if desc_node:
        description = desc_node.get_text(strip=True)

    created_raw = card.select_one("ul.details-list li.detail-created span")
    updated_raw = card.select_one("ul.details-list li.detail-updated span")
    created_at = parse_mmddyyyy(created_raw.get_text(strip=True)) if created_raw else None
    updated_at = parse_mmddyyyy(updated_raw.get_text(strip=True)) if updated_raw else None

    downloads_node = card.select_one("ul.details-list li.detail-downloads")
    downloads = parse_downloads(downloads_node.get_text(strip=True)) if downloads_node else None

    size_node = card.select_one("ul.details-list li.detail-size")
    size = size_node.get_text(strip=True) if size_node else ""

    gv_node = card.select_one("ul.details-list li.detail-game-version")
    game_version = gv_node.get_text(strip=True) if gv_node else ""

    stop_cats = {"mods"}
    raw_cats = [a.get_text(strip=True) for a in card.select("ul.categories li a")]
    norm_cats, seen = [], set()
    for c in raw_cats:
        if not c:
            continue
        if c.strip().lower() in stop_cats:
            continue
        if c not in seen:
            norm_cats.append(c)
            seen.add(c)
    categories = "; ".join(norm_cats)

    link = card.select_one("a.name") or card.select_one("a.overlay-link")
    project_url = urljoin(BASE_URL, link["href"]) if link and link.has_attr("href") else ""
    slug = extract_slug(link["href"]) if link and link.has_attr("href") else ""

    record = {
        "id": "",
        "slug": slug,
        "name": name,
        "description": description,
        "created_at": created_at or "",
        "updated_at": updated_at or "",
        "downloads": downloads if downloads is not None else "",
        "size": size,
        "game_version": game_version,
        "is_forge": "",
        "is_fabric": "",
        "is_neoforge": "",
        "is_quilt": "",
        "authors": authors,
        "categories": categories,
        "license": "",
        "project_url": project_url,
        "crawled_at": now_utc_iso()
    }
    return record


def parse_search_html(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div.project-card")
    return [parse_card(card) for card in cards]


class Fetcher:
    def __init__(self, session: ClientSession, log_file, concurrency: int, base_delay: float = 0.25,
                 jitter: float = 0.35):
        self.session = session
        self.semaphore = asyncio.Semaphore(concurrency)
        self.base_delay = base_delay
        self.jitter = jitter
        self.log = log_file

    async def polite_sleep(self):
        await asyncio.sleep(self.base_delay + random.random() * self.jitter)

    async def fetch_html(self, url: str, max_attempts: int = 4) -> Optional[str]:
        attempt = 0
        backoff = 0.8
        while attempt < max_attempts:
            attempt += 1
            async with self.semaphore:
                try:
                    async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        if resp.status == 404:
                            return None
                        if resp.status == 200:
                            text = await resp.text()
                            if text:
                                return text
                        if resp.status in {429, 500, 502, 503, 504}:
                            await self._warn(url, f"retryable status {resp.status}, attempt {attempt}")
                        else:
                            await self._warn(url, f"bad status {resp.status}, attempt {attempt}")
                except asyncio.TimeoutError:
                    await self._warn(url, f"timeout, attempt {attempt}")
                except aiohttp.ClientError as e:
                    await self._warn(url, f"client_error={repr(e)}, attempt {attempt}")

            await asyncio.sleep(backoff + random.random() * 0.5)
            backoff *= 1.7
        return None

    async def _warn(self, url: str, msg: str):
        log(f"[WARN] {url} — {msg}", self.log)


def load_existing_slugs(csv_path: str) -> set:
    if not os.path.exists(csv_path):
        return set()
    slugs = set()
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            slug = row.get("slug")
            if slug:
                slugs.add(slug)
    return slugs


async def producer(fetcher: Fetcher,
                   page_from: int,
                   pages: int,
                   page_size: int,
                   out_q: "asyncio.Queue[Tuple[int, Optional[List[Dict[str, Any]]], Optional[str]]]"):
    page = page_from
    while page < page_from + pages:
        params = f"?page={page}&pageSize={page_size}&sortBy=total+downloads&class=mc-mods"
        url = urljoin(BASE_URL, SEARCH_PATH + params)
        html = await fetcher.fetch_html(url)
        if html is None:
            await out_q.put((page, None, None))
            await fetcher.polite_sleep()
            page += 1
            continue
        try:
            rows = parse_search_html(html)
            if not rows:
                log(f"[END]  page={page} — пустая страница, завершаем", fetcher.log)
                break
            await out_q.put((page, rows, None))
        except Exception as e:
            await out_q.put((page, None, f"{repr(e)}\n{traceback.format_exc()}"))
        await fetcher.polite_sleep()
        page += 1
    await out_q.put((-1, None, None))


async def consumer(out_q, writer, log_file, seen_slugs: set):
    total_rows = 0
    pages_ok = 0
    pages_skip = 0
    while True:
        page, rows, err = await out_q.get()
        if page == -1:
            out_q.task_done()
            break

        if err:
            log(f"[ERROR] page={page} — {err}", log_file)
            out_q.task_done()
            continue

        if rows is None:
            pages_skip += 1
            log(f"[SKIP]  page={page} — нет данных или 404", log_file)
            out_q.task_done()
            continue

        for r in rows:
            if r["slug"] in seen_slugs:
                continue
            seen_slugs.add(r["slug"])
            writer.writerow(r)
            total_rows += 1

        pages_ok += 1
        log(f"[OK]    page={page} — записей: {len(rows)}; всего: {total_rows}", log_file)
        out_q.task_done()

    log(f"[DONE]  страниц ok={pages_ok}, skip={pages_skip}, строк={total_rows}", log_file)


def ensure_csv_writer(csv_path: str) -> Tuple[csv.DictWriter, bool, any]:
    file_exists = os.path.exists(csv_path)
    f_csv = open(csv_path, "a", newline="", encoding="utf-8-sig")
    writer = csv.DictWriter(
        f_csv,
        fieldnames=CSV_HEADERS,
        quoting=csv.QUOTE_ALL,
        escapechar='\\'
    )
    if not file_exists:
        writer.writeheader()
    return writer, file_exists, f_csv


async def main_async(args):
    csv_path = args.csv
    log_path = args.log
    page_from = max(1, args.page_from)
    try:
        pages = int(args.pages)
        infinite = False
    except ValueError:
        if args.pages.strip() == "*":
            infinite = True
            pages = float("inf")
        else:
            raise ValueError(f"Неверное значение --pages: {args.pages}")
    page_size = max(1, min(50, args.page_size))
    concurrency = max(1, args.concurrency)

    writer, _, f_csv = ensure_csv_writer(csv_path)
    seen_slugs = load_existing_slugs(csv_path)

    with open(log_path, "a", encoding="utf-8") as f_log:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }
        connector = aiohttp.TCPConnector(limit=concurrency * 4, ttl_dns_cache=300)

        log(f"[START] from page={page_from}, pages={'∞' if infinite else pages}, page_size={page_size}, concurrency={concurrency}", f_log)

        async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
            fetcher = Fetcher(session=session, log_file=f_log, concurrency=concurrency)
            out_q: asyncio.Queue = asyncio.Queue(maxsize=concurrency * 8)
            prod_task = asyncio.create_task(producer(fetcher, page_from, pages, page_size, out_q))
            cons_task = asyncio.create_task(consumer(out_q, writer, f_log, seen_slugs))

            try:
                await prod_task
                await cons_task
            except KeyboardInterrupt:
                log("[ABORT] Получен KeyboardInterrupt, корректное завершение…", f_log)
            finally:
                if not prod_task.done():
                    prod_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await prod_task
                f_csv.flush()
                f_csv.close()


def main():
    parser = argparse.ArgumentParser(prog="curseforge-mods-parser",
                                     description="CurseForge Minecraft mods listing parser (async)")
    parser.add_argument("--pages", type=str, default="1", help="сколько страниц обработать (число или *); дефолт 1")
    parser.add_argument("--page-from", type=int, default=1, help="с какой страницы начинать; дефолт 1")
    parser.add_argument("--page-size", type=int, default=20, help="размер страницы; дефолт 20")
    parser.add_argument("--concurrency", type=int, default=4, help="число одновременных запросов; дефолт 4")
    parser.add_argument("--csv", type=str, default=DEFAULT_CSV_PATH, help=f"путь к CSV; дефолт {DEFAULT_CSV_PATH}")
    parser.add_argument("--log", type=str, default=DEFAULT_LOG_PATH, help=f"путь к логу; дефолт {DEFAULT_LOG_PATH}")
    args = parser.parse_args()

    try:
        asyncio.run(main_async(args))
    except Exception as e:
        sys.stderr.write(f"Fatal: {repr(e)}\n")
        sys.stderr.write(traceback.format_exc() + "\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
