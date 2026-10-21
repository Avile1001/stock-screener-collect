# -*- coding: utf-8 -*-
"""국내 시세를 모아 스크리너 JSON으로 저장한다.

깃허브 Actions에서 평일 장중에 실행한다. 크레온은 쓰지 않는다.
"""

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))
HEADERS = {"User-Agent": "Mozilla/5.0"}
BASE = "https://m.stock.naver.com/api"
MARKETS = {"0": "코스피", "1": "코스닥"}


def parse_number(value):
    if value is None:
        return None
    text = str(value).replace(",", "").replace("+", "").strip()
    if text in ("", "-", "N/A"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def sma(closes, period):
    if len(closes) < period:
        return None
    return sum(closes[:period]) / period


def alignment_of(ma5, ma20, ma60, ma120):
    values = (ma5, ma20, ma60, ma120)
    if any(value is None for value in values):
        return "데이터부족"
    if ma5 > ma20 > ma60 > ma120:
        return "정배열"
    if ma5 < ma20 < ma60 < ma120:
        return "역배열"
    return "혼합"


def fetch_json(url, params=None):
    last_error = None
    for attempt in range(4):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=25)
        except requests.RequestException as exc:
            last_error = str(exc)
            time.sleep(1.2 * (attempt + 1))
            continue
        if response.status_code in (429, 500, 502, 503):
            last_error = "HTTP %s" % response.status_code
            time.sleep(1.2 * (attempt + 1))
            continue
        if response.status_code >= 400:
            raise RuntimeError("HTTP %s %s" % (response.status_code, url))
        try:
            return response.json()
        except ValueError as exc:
            last_error = str(exc)
            time.sleep(1.2 * (attempt + 1))
    raise RuntimeError(last_error or url)


def each_page(url, key):
    page = 1
    seen = 0
    while page <= 200:
        payload = fetch_json(url, {"page": page, "pageSize": 100})
        items = payload.get(key) or []
        if not items:
            break
        for item in items:
            yield item
        seen += len(items)
        total = payload.get("totalCount")
        if isinstance(total, int) and seen >= total:
            break
        if len(items) < 100:
            break
        page += 1


def quote_of(item, sector):
    code = str(item.get("itemCode") or "").strip()
    if item.get("stockEndType") != "stock" or item.get("sosok") not in MARKETS:
        return None
    if len(code) != 6 or not code.isdigit():
        return None
    price = parse_number(item.get("closePriceRaw", item.get("closePrice")))
    volume = parse_number(item.get("accumulatedTradingVolumeRaw", item.get("accumulatedTradingVolume")))
    change = parse_number(item.get("fluctuationsRatio"))
    if price is None or price <= 0 or volume is None:
        return None
    return {
        "code": "A" + code,
        "itemCode": code,
        "name": str(item.get("stockName") or code).strip(),
        "market": MARKETS[item["sosok"]],
        "sector": sector or "미분류",
        "price": int(round(price)),
        "changePct": round(change, 2) if change is not None else 0,
        "volume": int(round(volume)),
    }


def collect_quotes():
    quotes = {}
    for group in each_page(BASE + "/stocks/industry", "groups"):
        name = str(group.get("name") or "").strip() or "미분류"
        number = group.get("no")
        if number is None:
            continue
        for item in each_page(BASE + "/stocks/industry/%s" % number, "stocks"):
            row = quote_of(item, name)
            if row and row["code"] not in quotes:
                quotes[row["code"]] = row
    for item in each_page(BASE + "/stocks/marketValue/all", "stocks"):
        row = quote_of(item, "미분류")
        if row and row["code"] not in quotes:
            quotes[row["code"]] = row
    return list(quotes.values())


def history_of(item_code):
    bars = []
    for page in (1, 2):
        payload = fetch_json(BASE + "/stock/%s/price" % item_code, {"page": page, "pageSize": 60})
        if not isinstance(payload, list) or not payload:
            break
        for item in payload:
            traded = str(item.get("localTradedAt") or "")[:10]
            close = parse_number(item.get("closePrice"))
            volume = parse_number(item.get("accumulatedTradingVolume"))
            if not traded or close is None or close <= 0 or volume is None:
                continue
            bars.append((traded, close, volume, parse_number(item.get("fluctuationsRatio"))))
    bars.sort(key=lambda bar: bar[0], reverse=True)
    return bars


def row_from(quote, bars):
    closes = [bar[1] for bar in bars]
    ma5, ma20, ma60, ma120 = (sma(closes, period) for period in (5, 20, 60, 120))
    volume = int(round(bars[0][2])) if bars else quote["volume"]
    prev_volume = int(round(bars[1][2])) if len(bars) > 1 else 0
    ratio = round(volume / prev_volume, 2) if prev_volume else None
    change = bars[0][3] if bars and bars[0][3] is not None else quote["changePct"]
    price = int(round(bars[0][1])) if bars else quote["price"]
    return {
        "code": quote["code"],
        "name": quote["name"],
        "market": quote["market"],
        "sector": quote["sector"],
        "price": price,
        "changePct": round(change, 2),
        "volume": volume,
        "prevVolume": prev_volume,
        "volumeRatio": ratio,
        "value": price * volume,
        "ma5": None if ma5 is None else int(round(ma5)),
        "ma20": None if ma20 is None else int(round(ma20)),
        "ma60": None if ma60 is None else int(round(ma60)),
        "ma120": None if ma120 is None else int(round(ma120)),
        "alignment": alignment_of(ma5, ma20, ma60, ma120),
    }


def build(limit=0, workers=4):
    quotes = collect_quotes()
    quotes.sort(key=lambda row: row["code"])
    if limit:
        quotes = quotes[:limit]
    rows = []
    failed = 0
    market_date = ""
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(history_of, quote["itemCode"]): quote for quote in quotes}
        done = 0
        for future in as_completed(futures):
            done += 1
            quote = futures[future]
            try:
                bars = future.result()
            except Exception:
                failed += 1
                bars = []
            rows.append(row_from(quote, bars))
            if bars and bars[0][0] > market_date:
                market_date = bars[0][0]
            if done % 200 == 0 or done == len(quotes):
                print("charts %s/%s failed %s" % (done, len(quotes), failed), flush=True)
    rows.sort(key=lambda row: (-1 if row["volumeRatio"] is None else -row["volumeRatio"], row["name"]))
    now = datetime.now(KST).replace(microsecond=0)
    if not market_date:
        market_date = now.date().isoformat()
    return {
        "collectedAt": now.isoformat(),
        "marketDate": market_date,
        "count": len(rows),
        "failedCharts": failed,
        "rows": rows,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--out", default="stock_screener.json")
    args = parser.parse_args()
    started = time.time()
    payload = build(limit=args.limit, workers=max(1, args.workers))
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
    print(
        "wrote %s rows=%s failed=%s seconds=%s"
        % (args.out, payload["count"], payload["failedCharts"], round(time.time() - started)),
        flush=True,
    )


if __name__ == "__main__":
    main()
