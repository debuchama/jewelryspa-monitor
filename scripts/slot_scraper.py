"""
Caskan 予約ページ スロットスクレイパー

r.caskan.jp/jsnishiarai から5分刻みの予約スロットデータを抽出。
1リクエストで全セラピストのスロットが取得できる。
"""

import re
import json
import httpx
from datetime import datetime, timedelta, timezone

JST = timezone(timedelta(hours=9))
RESERVE_URL = "https://r.caskan.jp/jsnishiarai"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "ja,en;q=0.9",
}


def scrape_slots(target_date: str) -> list[dict]:
    """
    指定日の全セラピストの5分スロット空き/埋まり状況を取得。

    Returns: list of {
        therapist_id, schedule_date, total_slots, booked_slots,
        occupancy_pct, first_slot, last_slot, booked_ranges, slot_detail
    }
    """
    url = f"{RESERVE_URL}?date={target_date}"
    r = httpx.get(url, headers=HEADERS, timeout=30, follow_redirects=True)
    r.raise_for_status()
    html = r.text

    results = []

    # 各セラピストの cast-hour-{id} セクションを抽出
    pattern = r'cast-hour-(\d+)"[^>]*>(.*?)</table>'
    matches = re.findall(pattern, html, re.DOTALL)

    for cast_id_str, section in matches:
        cast_id = int(cast_id_str)

        # ヘッダ行: タイムスロット
        headers = re.findall(r'<th[^>]*>([\d:]+)</th>', section)
        if not headers:
            continue

        # データ行: radio=空き, それ以外=予約済み
        tds = re.findall(r'<td[^>]*>(.*?)</td>', section, re.DOTALL)
        if len(tds) != len(headers):
            # ヘッダとデータの数が合わない場合はスキップ
            continue

        slots = []
        for time_str, td_content in zip(headers, tds):
            is_booked = 'radio' not in td_content
            slots.append({"time": time_str, "booked": is_booked})

        total = len(slots)
        booked = sum(1 for s in slots if s["booked"])
        occupancy = round(100.0 * booked / total, 1) if total > 0 else 0.0

        # 予約済みの連続区間を検出
        booked_ranges = []
        range_start = None
        for i, s in enumerate(slots):
            if s["booked"] and range_start is None:
                range_start = s["time"]
            elif not s["booked"] and range_start is not None:
                booked_ranges.append([range_start, slots[i - 1]["time"]])
                range_start = None
        if range_start is not None:
            booked_ranges.append([range_start, slots[-1]["time"]])

        results.append({
            "therapist_id": cast_id,
            "schedule_date": target_date,
            "total_slots": total,
            "booked_slots": booked,
            "occupancy_pct": occupancy,
            "first_slot": headers[0] if headers else None,
            "last_slot": headers[-1] if headers else None,
            "booked_ranges": booked_ranges,
            "slot_detail": slots,
        })

    return results


def scrape_slots_today() -> list[dict]:
    today = datetime.now(JST).strftime("%Y-%m-%d")
    return scrape_slots(today)


def scrape_slots_tomorrow() -> list[dict]:
    tomorrow = (datetime.now(JST) + timedelta(days=1)).strftime("%Y-%m-%d")
    return scrape_slots(tomorrow)


def scrape_slots_range(days: int = 14) -> dict[str, list[dict]]:
    """
    今日から最大 days 日分のスロットデータを取得。
    データがある日だけ返す。
    Returns: { 'YYYY-MM-DD': [therapist_slots, ...], ... }
    """
    today = datetime.now(JST)
    all_data = {}

    for d in range(days):
        target = (today + timedelta(days=d)).strftime("%Y-%m-%d")
        try:
            result = scrape_slots(target)
            if result:  # データがある日だけ格納
                all_data[target] = result
                print(f"  📅 {target}: {len(result)} therapists, "
                      f"avg occ {sum(r['occupancy_pct'] for r in result)/max(len(result),1):.0f}%")
            else:
                # データなし = これ以降もなし（シフト未公開）→ 打ち切り
                print(f"  📅 {target}: no data (stopping)")
                break
        except Exception as e:
            print(f"  ❌ {target}: {e}")

    return all_data


if __name__ == "__main__":
    data = scrape_slots_today()
    print(f"📋 Today slots: {len(data)} therapists")
    for d in data:
        bar = "█" * int(d["occupancy_pct"] / 5) + "░" * (20 - int(d["occupancy_pct"] / 5))
        ranges_str = ", ".join(f"{r[0]}-{r[1]}" for r in d["booked_ranges"]) or "none"
        print(f"  ID={d['therapist_id']:5d} | {bar} {d['occupancy_pct']:5.1f}% "
              f"({d['booked_slots']}/{d['total_slots']}) | booked: {ranges_str}")
