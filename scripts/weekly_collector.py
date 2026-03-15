"""
週次出勤データ収集タスク（同期版）
cron: 毎日 6:00 実行推奨
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from db_setup import get_connection, init_db
from scraper import scrape_week
from datetime import datetime


def upsert_therapist(conn, data: dict):
    conn.execute("""
        INSERT INTO therapists (therapist_id, name, name_raw, age, height_cm, cup_size, profile_text)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(therapist_id) DO UPDATE SET
            name       = excluded.name,
            name_raw   = excluded.name_raw,
            age        = COALESCE(excluded.age, therapists.age),
            height_cm  = COALESCE(excluded.height_cm, therapists.height_cm),
            cup_size   = COALESCE(excluded.cup_size, therapists.cup_size),
            profile_text = COALESCE(excluded.profile_text, therapists.profile_text),
            last_seen  = datetime('now','localtime'),
            is_active  = 1
    """, (data["therapist_id"], data["name"], data["name_raw"],
          data["age"], data["height_cm"], data["cup_size"], data["profile_text"]))


def upsert_schedule(conn, data: dict):
    conn.execute("""
        INSERT INTO daily_schedules
            (therapist_id, schedule_date, location, start_time, end_time, is_fully_booked)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(therapist_id, schedule_date) DO UPDATE SET
            location        = excluded.location,
            start_time      = excluded.start_time,
            end_time        = excluded.end_time,
            is_fully_booked = excluded.is_fully_booked,
            scraped_at      = datetime('now','localtime')
    """, (data["therapist_id"], data["schedule_date"], data["location"],
          data["start_time"], data["end_time"], int(data["is_fully_booked"])))


def run():
    init_db()
    conn = get_connection()

    print("=" * 60)
    print(f"🔄 週次スケジュール収集: {datetime.now():%Y-%m-%d %H:%M}")
    print("=" * 60)

    try:
        week_data = scrape_week()
        total = 0
        for date, records in week_data.items():
            for rec in records:
                upsert_therapist(conn, rec)
                upsert_schedule(conn, rec)
            total += len(records)
            conn.execute("""
                INSERT INTO scrape_logs (task_type, target_date, records_found, success)
                VALUES ('weekly', ?, ?, 1)
            """, (date, len(records)))
            print(f"  ✅ {date}: {len(records)} records")

        conn.commit()
        print(f"\n📊 合計: {total} records / {len(week_data)} days")
    except Exception as e:
        conn.execute("""
            INSERT INTO scrape_logs (task_type, success, error_message)
            VALUES ('weekly', 0, ?)
        """, (str(e),))
        conn.commit()
        print(f"❌ Error: {e}")
        raise
    finally:
        conn.close()

    print("✅ 完了")


if __name__ == "__main__":
    run()
