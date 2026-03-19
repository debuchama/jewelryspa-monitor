"""
予約スロットモニター

Caskan予約ページから5分刻みの空き状況を取得し、
slot_summaries テーブルに時系列記録する。
10分ごとにGitHub Actionsで実行する想定。

Usage:
  python slot_monitor.py            # 当日+翌日を1回チェック
  python slot_monitor.py --loop 10  # 10分間隔で継続監視
"""

import argparse
import json
import sys
import os
import time

sys.path.insert(0, os.path.dirname(__file__))

from db_setup import get_connection, init_db
from slot_scraper import scrape_slots_today, scrape_slots_tomorrow
from tz import now_jst, now_str, today_str


def record_slot_summary(conn, data: list[dict]):
    jst = now_str()
    recorded = 0
    for d in data:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO slot_summaries
                    (checked_at, therapist_id, schedule_date,
                     total_slots, booked_slots, occupancy_pct,
                     first_slot, last_slot, booked_ranges)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                jst, d["therapist_id"], d["schedule_date"],
                d["total_slots"], d["booked_slots"], d["occupancy_pct"],
                d["first_slot"], d["last_slot"],
                json.dumps(d["booked_ranges"], ensure_ascii=False),
            ))
            recorded += 1
        except Exception as e:
            print(f"  ⚠️ Skip {d['therapist_id']}: {e}")
    return recorded


def detect_velocity(conn, current: list[dict]) -> list[str]:
    """前回スナップショットとの占有率変化を検出"""
    alerts = []
    for d in current:
        row = conn.execute("""
            SELECT occupancy_pct, checked_at FROM slot_summaries
            WHERE therapist_id = ? AND schedule_date = ?
            ORDER BY checked_at DESC LIMIT 1
        """, (d["therapist_id"], d["schedule_date"])).fetchone()

        if row:
            prev_occ = row["occupancy_pct"]
            delta = d["occupancy_pct"] - prev_occ
            if delta >= 10:
                alerts.append(
                    f"  🔥 ID={d['therapist_id']}: {prev_occ:.0f}% → {d['occupancy_pct']:.0f}% (+{delta:.0f}%)")
            elif delta <= -10:
                alerts.append(
                    f"  🔄 ID={d['therapist_id']}: {prev_occ:.0f}% → {d['occupancy_pct']:.0f}% ({delta:.0f}% cancel?)")
    return alerts


def run(loop_minutes=0):
    init_db()
    conn = get_connection()

    while True:
        t = now_jst()
        print(f"\n⏰ Slot check: {t:%H:%M:%S} JST")
        jst = now_str()

        try:
            # 当日スロット
            today_data = scrape_slots_today()
            print(f"  📋 Today: {len(today_data)} therapists")

            # 充足速度アラート
            alerts = detect_velocity(conn, today_data)
            if alerts:
                print("  ── Velocity alerts ──")
                for a in alerts:
                    print(a)

            recorded = record_slot_summary(conn, today_data)

            # 翌日スロット（あれば）
            try:
                tomorrow_data = scrape_slots_tomorrow()
                if tomorrow_data:
                    recorded += record_slot_summary(conn, tomorrow_data)
                    print(f"  📋 Tomorrow: {len(tomorrow_data)} therapists")
            except Exception:
                pass  # 翌日データがない場合はスキップ

            conn.execute("""
                INSERT INTO scrape_logs (run_at, task_type, target_date, records_found, success)
                VALUES (?, 'slot_monitor', ?, ?, 1)
            """, (jst, today_str(), recorded))
            conn.commit()

            # サマリ表示
            for d in today_data:
                if d["occupancy_pct"] > 0:
                    bar = "█" * int(d["occupancy_pct"] / 5) + "░" * (20 - int(d["occupancy_pct"] / 5))
                    print(f"  {d['therapist_id']:5d} {bar} {d['occupancy_pct']:.0f}%")

        except Exception as e:
            print(f"  ❌ {e}")
            conn.execute("""
                INSERT INTO scrape_logs (run_at, task_type, target_date, success, error_message)
                VALUES (?, 'slot_monitor', ?, 0, ?)
            """, (jst, today_str(), str(e)))
            conn.commit()

        if loop_minutes <= 0:
            break
        print(f"  💤 Next: {loop_minutes}min")
        time.sleep(loop_minutes * 60)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", type=int, default=0)
    args = parser.parse_args()
    run(args.loop)
