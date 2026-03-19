"""
全セラピスト名簿 + 14日スケジュール スクレイパー

r.caskan.jp/jsnishiarai/cast から在籍全員のプロフィールを取得。
個別ページ /cast/{id} から14日分の出勤予定を取得。
"""

import re
import httpx
from datetime import datetime, timedelta, timezone

JST = timezone(timedelta(hours=9))
CAST_LIST_URL = "https://r.caskan.jp/jsnishiarai/cast"
CAST_PAGE_URL = "https://r.caskan.jp/jsnishiarai/cast/{}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "ja,en;q=0.9",
}


def _clean_name(raw: str) -> str:
    return re.sub(r"[💎\s]+", "", raw).strip()


def scrape_roster() -> list[dict]:
    """全セラピスト名簿を取得。退職者含む全在籍者リスト。"""
    from bs4 import BeautifulSoup

    r = httpx.get(CAST_LIST_URL, headers=HEADERS, timeout=30, follow_redirects=True)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    results = []
    links = soup.find_all("a", href=re.compile(r"/cast/\d+"))
    seen = set()

    for a in links:
        cid_m = re.search(r"/cast/(\d+)", a["href"])
        if not cid_m:
            continue
        cid = int(cid_m.group(1))
        if cid in seen:
            continue
        seen.add(cid)

        card = a.find_parent("li") or a.find_parent("div")
        if not card:
            continue

        text = card.get_text(separator="|", strip=True)

        # Name
        name_m = re.search(r"💎([^💎]+)💎", text)
        name = name_m.group(1).strip() if name_m else None
        if not name:
            continue

        # Age
        age = None
        age_m = re.search(r"(\d{2})歳", text)
        if age_m:
            age = int(age_m.group(1))

        # Height
        height = None
        h_m = re.search(r"(\d{3})㎝", text)
        if h_m:
            height = int(h_m.group(1))

        # Cup
        cup = None
        cup_m = re.search(r"\(([A-K])\)", text)
        if cup_m:
            cup = cup_m.group(1)

        # Image (high-res from cast_tmb)
        img = card.find("img", src=re.compile(r"cast"))
        photo_url = img.get("src") if img else None

        # SNS
        sns_links = []
        for sns_a in card.find_all("a", href=re.compile(r"twitter|x\.com|bsky|bluesky|instagram")):
            sns_links.append(sns_a.get("href"))

        results.append({
            "therapist_id": cid,
            "name": name,
            "age": age,
            "height_cm": height,
            "cup_size": cup,
            "photo_url": photo_url,
            "sns_links": sns_links,
        })

    return results


def scrape_14day_schedule(therapist_id: int) -> list[dict]:
    """
    個別キャストページから14日分のスケジュールを取得。
    出勤がある日のみ返す。
    """
    from bs4 import BeautifulSoup

    url = CAST_PAGE_URL.format(therapist_id)
    r = httpx.get(url, headers=HEADERS, timeout=30, follow_redirects=True)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    results = []
    table = soup.find("table")
    if not table:
        return results

    today = datetime.now(JST)
    year = today.year

    for row in table.find_all("tr"):
        tds = row.find_all("td")
        if not tds:
            continue

        date_text = tds[0].get_text(strip=True)
        # Parse "3/19 木" format
        date_m = re.match(r"(\d{1,2})/(\d{1,2})\s*([月火水木金土日])", date_text)
        if not date_m:
            continue

        month, day = int(date_m.group(1)), int(date_m.group(2))
        # Handle year rollover (Dec→Jan)
        sched_year = year
        if month < today.month - 1:
            sched_year = year + 1
        try:
            sched_date = datetime(sched_year, month, day).strftime("%Y-%m-%d")
        except ValueError:
            continue

        # Get shift info from remaining cells
        cell_text = "|".join(td.get_text(strip=True) for td in tds[1:])

        location = None
        for loc in ["赤羽", "王子", "西新井"]:
            if loc in cell_text:
                location = loc
                break

        time_m = re.search(r"(\d{1,2}:\d{2})[〜～~\-](\d{1,2}:\d{2})", cell_text)
        start_time, end_time = None, None
        if time_m:
            start_time = time_m.group(1)
            end_time = time_m.group(2)
            # Normalize late-night
            eh = int(end_time.split(":")[0])
            em = int(end_time.split(":")[1])
            if eh < 6:
                end_time = f"{eh + 24}:{em:02d}"

        if not location and not start_time:
            continue  # No shift this day

        results.append({
            "therapist_id": therapist_id,
            "schedule_date": sched_date,
            "location": location or "不明",
            "start_time": start_time,
            "end_time": end_time,
        })

    return results


def scrape_all_14day() -> dict:
    """全セラピストの14日スケジュールを収集。

    Returns: {
        "roster": [...],
        "schedules": [...],
        "roster_ids": set of all cast IDs on /cast page,
    }
    """
    print("  📋 Fetching full roster...")
    roster = scrape_roster()
    print(f"  ✅ Roster: {len(roster)} therapists")

    all_schedules = []
    active_count = 0

    for i, member in enumerate(roster):
        try:
            scheds = scrape_14day_schedule(member["therapist_id"])
            if scheds:
                active_count += 1
                all_schedules.extend(scheds)
        except Exception as e:
            pass  # Skip failed individual pages silently

        if (i + 1) % 20 == 0:
            print(f"  ... {i + 1}/{len(roster)} checked")

    print(f"  ✅ Schedules: {len(all_schedules)} shifts from {active_count} active therapists")

    return {
        "roster": roster,
        "schedules": all_schedules,
        "roster_ids": {m["therapist_id"] for m in roster},
    }


if __name__ == "__main__":
    data = scrape_all_14day()
    print(f"\n📋 Roster: {len(data['roster'])} total")

    # Show who has shifts
    active_ids = {s["therapist_id"] for s in data["schedules"]}
    for m in data["roster"]:
        status = "✅ active" if m["therapist_id"] in active_ids else "⚪ no shifts"
        print(f"  {m['name']:6s} (ID={m['therapist_id']}) | {status}")
