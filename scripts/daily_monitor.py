"""
当日空き状況モニター（同期版）

当日の予約可否をスナップショットとして記録。
cron: 15〜30分ごとに実行 or --loop で継続監視。

  python daily_monitor.py           # 1回チェック
  python daily_monitor.py --loop 15 # 15分間隔で継続
"""

import argparse, sys, os, time
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

from db_setup import get_connection, init_db
from scraper import scrape_today


def record_snapshot(conn, records):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for rec in records:
        status = "fully_booked" if rec["is_fully_booked"] else "available"
        conn.execute("""
            INSERT INTO availability_snapshots
                (checked_at, therapist_id, schedule_date, location, status, start_time, end_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (now, rec["therapist_id"], rec["schedule_date"],
              rec["location"], status, rec["start_time"], rec["end_time"]))


def detect_changes(conn, current):
    changes = []
    today = datetime.now().strftime("%Y-%m-%d")
    for rec in current:
        row = conn.execute("""
            SELECT status FROM availability_snapshots
            WHERE therapist_id = ? AND schedule_date = ?
            ORDER BY checked_at DESC LIMIT 1
        """, (rec["therapist_id"], today)).fetchone()

        cur_status = "fully_booked" if rec["is_fully_booked"] else "available"
        if row and row["status"] != cur_status:
            changes.append(
                f"  🔔 {rec['name']}({rec['location']}): {row['status']} → {cur_status}"
            )
    return changes


def run(loop_minutes=0):
    init_db()
    conn = get_connection()

    while True:
        print(f"\n⏰ チェック: {datetime.now():%H:%M:%S}")
        try:
            records = scrape_today()
            print(f"  📋 {len(records)} staff today")

            changes = detect_changes(conn, records)
            if changes:
                print("  ── 状態変化 ──")
                for c in changes:
                    print(c)
            else:
                print("  (変化なし)")

            record_snapshot(conn, records)

            for rec in records:
                conn.execute("""
                    INSERT INTO therapists (therapist_id, name, name_raw, age, height_cm, cup_size, profile_text)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(therapist_id) DO UPDATE SET
                        last_seen = datetime('now','localtime'), is_active = 1
                """, (rec["therapist_id"], rec["name"], rec["name_raw"],
                      rec["age"], rec["height_cm"], rec["cup_size"], rec["profile_text"]))

            conn.execute("""
                INSERT INTO scrape_logs (task_type, target_date, records_found, success)
                VALUES ('daily_monitor', ?, ?, 1)
            """, (datetime.now().strftime("%Y-%m-%d"), len(records)))
            conn.commit()

            for loc in ["赤羽", "王子", "西新井"]:
                staff = [r for r in records if r["location"] == loc]
                avail = [r for r in staff if not r["is_fully_booked"]]
                print(f"  {loc}: {len(avail)}/{len(staff)} available")

        except Exception as e:
            print(f"  ❌ {e}")
            conn.execute("""
                INSERT INTO scrape_logs (task_type, target_date, success, error_message)
                VALUES ('daily_monitor', ?, 0, ?)
            """, (datetime.now().strftime("%Y-%m-%d"), str(e)))
            conn.commit()

        if loop_minutes <= 0:
            break
        print(f"  💤 次: {loop_minutes}分後")
        time.sleep(loop_minutes * 60)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", type=int, default=0)
    args = parser.parse_args()
    run(args.loop)
