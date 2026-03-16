"""
ジュエリースパ スケジュールスクレイパー (v3)

httpx + BeautifulSoup。Caskan CMS DOM構造に正確に対応。
カード要素: div.therapist-datas-each
"""

import re
import httpx
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup

BASE_URL = "https://jewelryspa-nishiarai.com/schedule"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ja,en;q=0.9",
}

JST = timezone(timedelta(hours=9))
def _now_jst(): return datetime.now(JST)


def _clean_name(raw: str) -> str:
    return re.sub(r"[💎\s]+", "", raw).strip()


def _parse_time_range(text: str):
    text = text.translate(str.maketrans("０１２３４５６７８９：〜", "0123456789:~"))
    m = re.search(r"(\d{1,2}:\d{2})\s*[~～〜\-]\s*(\d{1,2}:\d{2})", text)
    if not m:
        return None, None
    start, end = m.group(1), m.group(2)
    sh = int(start.split(":")[0])
    eh, em = int(end.split(":")[0]), int(end.split(":")[1])
    if eh < 6:
        end = f"{eh + 24}:{em:02d}"
    if sh < 6:
        sm = int(start.split(":")[1])
        start = f"{sh + 24}:{sm:02d}"
    return start, end


def scrape_day(target_date: str) -> list[dict]:
    today = _now_jst().strftime("%Y-%m-%d")
    url = f"{BASE_URL}?day={target_date}&from={today}"

    r = httpx.get(url, headers=HEADERS, timeout=30, follow_redirects=True)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    results = []

    # カードコンテナ: div.therapist-datas-each
    cards = soup.select("div.therapist-datas-each")

    for card in cards:
        # therapist ID をリンクから取得
        link = card.select_one('a[href*="/therapist/"]')
        if not link:
            continue
        href = link.get("href", "")
        tid_match = re.search(r"/therapist/(\d+)", href)
        if not tid_match:
            continue
        therapist_id = int(tid_match.group(1))

        # カード全テキスト
        card_text = card.get_text(separator="|", strip=True)

        # 名前: therapistリンクのテキストまたは2つ目のリンク
        name_raw = None
        name_links = card.select('a[href*="/therapist/"]')
        for nl in name_links:
            t = nl.get_text(strip=True)
            if t and "💎" in t:
                name_raw = t
                break
            elif t and len(t) > 0:
                name_raw = t
                break
        if not name_raw:
            # テキストから名前を推定
            m = re.search(r"(💎[^💎]+💎)", card_text)
            if m:
                name_raw = m.group(1)
        if not name_raw:
            continue

        name = _clean_name(name_raw)
        if not name:
            continue

        # 年齢
        age = None
        age_m = re.search(r"(\d{2})歳", card_text)
        if age_m:
            age = int(age_m.group(1))

        # 身長
        height = None
        h_m = re.search(r"(\d{3})㎝", card_text)
        if h_m:
            height = int(h_m.group(1))

        # カップ
        cup = None
        cup_m = re.search(r"\(([A-K])\)", card_text)
        if cup_m:
            cup = cup_m.group(1)

        # 勤務時間
        start_time, end_time = None, None
        time_m = re.search(r"(\d{1,2}:\d{2})\s*[〜～~\-]\s*(\d{1,2}:\d{2})", card_text)
        if time_m:
            start_time, end_time = _parse_time_range(time_m.group(0))

        # 店舗
        location = None
        for loc in ["赤羽", "王子", "西新井"]:
            if loc in card_text:
                location = loc
                break
        # 予約リンクのroomパラメータからも取得
        if not location:
            reserve_link = card.select_one('a[href*="reserve"]')
            if reserve_link:
                room_m = re.search(r"room=([^&]+)", reserve_link.get("href", ""))
                if room_m:
                    location = room_m.group(1)

        # 予約満了
        is_fully_booked = "予約満了" in card_text

        # プロフィール
        profile_parts = []
        skip_pats = [name_raw, "歳", "㎝", "予約", "icon", "赤羽", "王子", "西新井"]
        for seg in card_text.split("|"):
            seg = seg.strip()
            if any(p in seg for p in skip_pats):
                continue
            if re.match(r"^\d{1,2}:\d{2}", seg):
                continue
            if re.match(r"^\(\w\)$", seg):
                continue
            if len(seg) > 10:
                profile_parts.append(seg)
        profile_text = " ".join(profile_parts[:2]) if profile_parts else None

        results.append({
            "therapist_id": therapist_id,
            "name": name,
            "name_raw": name_raw,
            "age": age,
            "height_cm": height,
            "cup_size": cup,
            "profile_text": profile_text,
            "schedule_date": target_date,
            "location": location or "不明",
            "start_time": start_time,
            "end_time": end_time,
            "is_fully_booked": is_fully_booked,
        })

    return results


def scrape_week(start_date: str = None) -> dict[str, list[dict]]:
    if start_date is None:
        start = _now_jst()
    else:
        start = datetime.strptime(start_date, "%Y-%m-%d")

    dates = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
    all_data = {}

    for d in dates:
        try:
            day_data = scrape_day(d)
            all_data[d] = day_data
            print(f"  📅 {d}: {len(day_data)} staff")
        except Exception as e:
            print(f"  ❌ {d}: {e}")
            all_data[d] = []

    return all_data


def scrape_today() -> list[dict]:
    return scrape_day(_now_jst().strftime("%Y-%m-%d"))


if __name__ == "__main__":
    data = scrape_today()
    print(f"\n📋 本日の出勤: {len(data)} staff")
    for d in data:
        status = "🔴満了" if d["is_fully_booked"] else "🟢空き"
        print(f"  {d['name']:6s} | {d['location']:4s} | {d['start_time']}〜{d['end_time']} | {status}")
