#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DiscoverCars: многопоточный BFS-краулер с отдельным прокси на поток.
"""


from __future__ import annotations
import requests, csv, json, html, re, string, time, pathlib, pickle, sys, queue, threading
from collections import Counter
from datetime import datetime, timedelta
from typing import Dict, Any, Iterable, List
from requests.adapters import HTTPAdapter, Retry
from rich.console import Console
from rich.table import Table
from rich.live import Live
from tqdm import tqdm


# ───── константы ──────────────────────────────────────────────────────────
ROOT  = "https://www.discovercars.com"
UA    = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36")
OUT        = pathlib.Path("out");   OUT.mkdir(exist_ok=True)
CACHE_DIR  = pathlib.Path("cache"); CACHE_DIR.mkdir(exist_ok=True)
CSV_PATH   = OUT / "discovercars_live.csv"
JSON_PATH  = OUT / "discovercars_live.jsonl"
STATE_PATH = CACHE_DIR / "queue_state.pkl"


DELAY        = 0.12
MAX_DEPTH    = 4
BACKUP_EVERY = timedelta(minutes=2)
N_THREADS    = 8                      # потоков ≈ числу прокси
PROXY_LIST   = [l.strip() for l in open("proxies.txt") if l.strip()]


console = Console()
stats    = Counter()


# ───── Session factory ────────────────────────────────────────────────────
def make_session(proxy: str | None) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    retry = Retry(total=5, backoff_factor=0.5,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET"], raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    if proxy:
        s.proxies.update({"http": proxy, "https": proxy})
    return s


# ───── CSV helpers (writer-поток) ─────────────────────────────────────────
csv_lock = threading.Lock()          # гарантируем атомарность записи


def ensure_csv_header(fieldnames: List[str]):
    with csv_lock:
        if CSV_PATH.exists():
            return
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames,
                           extrasaction="ignore").writeheader()


def append_rows(rows: List[Dict[str,Any]], fieldnames: List[str]):
    with csv_lock:
        with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames,
                           extrasaction="ignore").writerows(rows)
        with JSON_PATH.open("a", encoding="utf-8") as jf:
            for r in rows:
                jf.write(json.dumps(r, ensure_ascii=False) + "\n")


# ───── API helpers (как в соло-версии) ────────────────────────────────────
def get_csrf(sess: requests.Session) -> str:
    html_text = sess.get(ROOT, timeout=30).text
    return html.unescape(re.search(
        r'<meta name="csrf-token"\s+content="([^"]+)"',
        html_text)[1])


def api_call(sess: requests.Session, prefix: str, token: str,
             retries: int = 3) -> list[Dict[str, Any]]:
    url = f"{ROOT}/en/search/autocomplete/{prefix}"
    hdr = {
        "x-csrf-token": token,
        "x-kl-ajax-request": "Ajax_Request",
        "x-requested-with": "XMLHttpRequest",
        "referer": ROOT + "/",
        "accept": "application/json, text/plain, */*",
    }
    try:
        r = sess.get(url, headers=hdr, timeout=60)
        stats[str(r.status_code)] += 1
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json()
    except (requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectionError) as e:
        if retries:
            time.sleep(2)
            return api_call(sess, prefix, token, retries-1)
        tqdm.write(f"[TIMEOUT] {prefix}: {e}")
        return []


# ───── сохранение / загрузка состояния ───────────────────────────────────
def load_previous():
    if not STATE_PATH.exists():
        return set(), list(string.ascii_lowercase), 0
    with STATE_PATH.open("rb") as fh:
        state = pickle.load(fh)
    console.print("[yellow]⏪  Продолжаю прошлый запуск…[/]")
    return state["seen_ids"], state["queue"], state["processed"]


def save_state(seen, queue, processed):
    pickle.dump({"seen_ids": seen, "queue": queue,
                 "processed": processed}, STATE_PATH.open("wb"))


# ───── worker + writer поток ─────────────────────────────────────────────
def writer_thread(rows_q: "queue.Queue[list[Dict]]", fieldnames_ref):
    """Забирает списки строк из очереди и пишет в файлы."""
    while True:
        rows = rows_q.get()
        if rows is None:          # сигнал остановки
            break
        if rows and fieldnames_ref[0] is None:
            fieldnames = list({k for r in rows for k in r})
            first = ["country","countryID","city","cityID","location","place",
                     "placeID","lat","lng"]
            fieldnames = first + [k for k in fieldnames if k not in first]
            fieldnames_ref[0] = fieldnames
            ensure_csv_header(fieldnames)
        append_rows(rows, fieldnames_ref[0])
        rows_q.task_done()


def worker(thread_id: int, prefix_q: "queue.Queue[str]",
           rows_q: "queue.Queue[list[Dict]]",
           seen_ids: set[str], seen_lock: threading.Lock,
           processed_counter, processed_lock: threading.Lock):
    sess = make_session(PROXY_LIST[thread_id % len(PROXY_LIST)])
    token = get_csrf(sess)
    while True:
        try:
            prefix = prefix_q.get(timeout=3)  # timeout → выходим
        except queue.Empty:
            return
        data = api_call(sess, prefix, token)
        with processed_lock:
            processed_counter[0] += 1
        new_rows = []
        with seen_lock:
            for obj in data:
                uid = f"{obj['location']}:{obj['placeID']}"
                if uid not in seen_ids:
                    seen_ids.add(uid)
                    new_rows.append(obj)
        if new_rows:
            rows_q.put(new_rows)


        # углубляем префикс
        if len(data) == 10 and len(prefix) < MAX_DEPTH:
            for ch in string.ascii_lowercase:
                prefix_q.put(prefix + ch)


        prefix_q.task_done()
        time.sleep(DELAY)


# ───── многопоточный crawl ───────────────────────────────────────────────
def crawl():
    seen_ids, queue_list, processed = load_previous()
    prefix_q: "queue.Queue[str]" = queue.Queue()
    for p in queue_list:
        prefix_q.put(p)


    seen_lock   = threading.Lock()
    processed_lock = threading.Lock()
    processed_counter = [processed]         # обёртка-список ⇒ byref
    rows_q: "queue.Queue[list[Dict]]" = queue.Queue(maxsize=1000)
    fieldnames_ref = [None]                 # by-reference контейнер


    writer = threading.Thread(target=writer_thread,
                              args=(rows_q, fieldnames_ref), daemon=True)
    writer.start()


    workers = [threading.Thread(
        target=worker,
        args=(i, prefix_q, rows_q, seen_ids, seen_lock,
              processed_counter, processed_lock),
        daemon=True)
        for i in range(N_THREADS)]
    for w in workers: w.start()


    last_backup = datetime.utcnow()
    with Live(console=console, auto_refresh=False) as live, \
         tqdm(total=prefix_q.qsize(),
              bar_format="{l_bar}{bar}| {n_fmt} префиксов {elapsed}") as pbar:
        prev_processed = processed_counter[0]
        while any(w.is_alive() for w in workers):
            # обновляем прогресс-бар
            new_proc = processed_counter[0] - prev_processed
            if new_proc:
                pbar.update(new_proc)
                pbar.total = prefix_q.qsize() + processed_counter[0]
                prev_processed = processed_counter[0]
            live.update(Table().add_row(
                f"✓ {processed_counter[0]:,}",
                f"★ {len(seen_ids):,}",
                f"⏳ {prefix_q.qsize():,}"), refresh=True)


            # периодический backup
            if datetime.utcnow() - last_backup >= BACKUP_EVERY:
                save_state(seen_ids, list(prefix_q.queue),
                           processed_counter[0])
                console.print(f"[cyan]💾 backup "
                              f"({processed_counter[0]} префиксов)[/]")
                last_backup = datetime.utcnow()
            time.sleep(1)


    # финальные штрихи
    rows_q.put(None)       # стоп-сигнал writer-потоку
    writer.join()
    save_state(seen_ids, [], processed_counter[0])
    return len(seen_ids), processed_counter[0]


# ───── ENTRY ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    start = time.time()
    try:
        uniq, proc = crawl()
        console.print(f"\n[bold green]✔ Завершено: {uniq:,} точек, "
                      f"{proc:,} префиксов, {(time.time()-start)/60:.1f} мин.[/]")
        console.print(f"[green]CSV[/]  → {CSV_PATH}\n"
                      f"[green]JSONL[/]→ {JSON_PATH}")
    except KeyboardInterrupt:
        console.print("[red]\n⏹ Остановлено пользователем.[/]")
