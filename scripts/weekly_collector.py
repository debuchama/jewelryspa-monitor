"""
週次出勤データ収集タスク（同期版）
cron: 毎日 JST 6:00 実行推奨
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from db_setup import get_connection, init_db
from scraper import scrape_week
from tz import now_jst, now_str


def upsert_therapist(conn, data: dict):
    jst = now_str()
    conn.execute("""
        INSERT INTO therapists (therapist_id, name, name_raw, age, height_cm, cup_size, profile_text, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(therapist_id) DO UPDATE SET
            name       = excluded.name,
            name_raw   = excluded.name_raw,
            age        = COALESCE(excluded.age, therapists.age),
            height_cm  = COALESCE(excluded.height_cm, therapists.height_cm),
            cup_size   = COALESCE(excluded.cup_size, therapists.cup_size),
            profile_text = COALESCE(excluded.profile_text, therapists.profile_text),
            last_seen  = ?,
            is_active  = 1
    """, (data["therapist_id"], data["name"], data["name_raw"],
          data["age"], data["height_cm"], data["cup_size"], data["profile_text"],
          jst, jst, jst))


def upsert_schedule(conn, data: dict):
    jst = now_str()
    conn.execute("""
        INSERT INTO daily_schedules
            (therapist_id, schedule_date, location, start_time, end_time, is_fully_booked, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(therapist_id, schedule_date) DO UPDATE SET
            location        = excluded.location,
            start_time      = excluded.start_time,
            end_time        = excluded.end_time,
            is_fully_booked = excluded.is_fully_booked,
            scraped_at      = ?
    """, (data["therapist_id"], data["schedule_date"], data["location"],
          data["start_time"], data["end_time"], int(data["is_fully_booked"]),
          jst, jst))


def run():
    init_db()
    conn = get_connection()
    t = now_jst()

    print("=" * 60)
    print(f"🔄 週次スケジュール収集: {t:%Y-%m-%d %H:%M} JST")
    print("=" * 60)

    try:
        week_data = scrape_week()
        total = 0
        jst = now_str()
        for date, records in week_data.items():
            for rec in records:
                upsert_therapist(conn, rec)
                upsert_schedule(conn, rec)
            total += len(records)
            conn.execute("""
                INSERT INTO scrape_logs (run_at, task_type, target_date, records_found, success)
                VALUES (?, 'weekly', ?, ?, 1)
            """, (jst, date, len(records)))
            print(f"  ✅ {date}: {len(records)} records")

        conn.commit()
        print(f"\n📊 合計: {total} records / {len(week_data)} days")
    except Exception as e:
        jst = now_str()
        conn.execute("""
            INSERT INTO scrape_logs (run_at, task_type, success, error_message)
            VALUES (?, 'weekly', 0, ?)
        """, (jst, str(e)))
        conn.commit()
        print(f"❌ Error: {e}")
        raise
    finally:
        conn.close()

    print("✅ 完了")


if __name__ == "__main__":
    run()
