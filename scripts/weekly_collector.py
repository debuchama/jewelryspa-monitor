"""
週次出勤データ収集タスク v2

1. Caskan /cast ページから全ロスター取得
2. 各セラピストの14日スケジュール取得
3. セラピストマスタ更新（写真・SNS含む）
4. 引退検出（前回ロスターにいたが今回消えた）
5. 14日分のスケジュールをDBに保存

cron: 毎日 JST 6:00 + 18:00（2回/日）推奨
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from db_setup import get_connection, init_db
from scraper import scrape_week
from roster_scraper import scrape_roster, scrape_14day_schedule
from tz import now_jst, now_str, today_str


def upsert_therapist_from_roster(conn, member: dict, jst: str):
    """ロスターデータからセラピスト情報を更新"""
    sns_json = json.dumps(member.get("sns_links", []), ensure_ascii=False)
    conn.execute("""
        INSERT INTO therapists
            (therapist_id, name, age, height_cm, cup_size, photo_url, sns_links, first_seen, last_seen, is_active, retired_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL)
        ON CONFLICT(therapist_id) DO UPDATE SET
            name       = excluded.name,
            age        = COALESCE(excluded.age, therapists.age),
            height_cm  = COALESCE(excluded.height_cm, therapists.height_cm),
            cup_size   = COALESCE(excluded.cup_size, therapists.cup_size),
            photo_url  = COALESCE(excluded.photo_url, therapists.photo_url),
            sns_links  = COALESCE(excluded.sns_links, therapists.sns_links),
            last_seen  = ?,
            is_active  = 1,
            retired_at = NULL
    """, (
        member["therapist_id"], member["name"],
        member.get("age"), member.get("height_cm"), member.get("cup_size"),
        member.get("photo_url"), sns_json,
        jst, jst, jst
    ))


def upsert_schedule(conn, sched: dict, jst: str):
    """スケジュールをUPSERT"""
    conn.execute("""
        INSERT INTO daily_schedules
            (therapist_id, schedule_date, location, start_time, end_time, is_fully_booked, scraped_at)
        VALUES (?, ?, ?, ?, ?, 0, ?)
        ON CONFLICT(therapist_id, schedule_date) DO UPDATE SET
            location   = excluded.location,
            start_time = excluded.start_time,
            end_time   = excluded.end_time,
            scraped_at = ?
    """, (
        sched["therapist_id"], sched["schedule_date"],
        sched["location"], sched["start_time"], sched["end_time"],
        jst, jst
    ))


def detect_retirements(conn, current_roster_ids: set, jst: str) -> list[dict]:
    """
    前回在籍していたが今回のロスターに不在 → 引退候補。
    ただし初回実行時（DBにまだデータが少ない）は誤検出を避ける。
    """
    rows = conn.execute("""
        SELECT therapist_id, name FROM therapists
        WHERE is_active = 1 AND retired_at IS NULL
    """).fetchall()

    retired = []
    for row in rows:
        if row["therapist_id"] not in current_roster_ids:
            conn.execute("""
                UPDATE therapists SET is_active = 0, retired_at = ?
                WHERE therapist_id = ?
            """, (jst, row["therapist_id"]))
            retired.append({"therapist_id": row["therapist_id"], "name": row["name"]})

    return retired


def run():
    init_db()
    conn = get_connection()
    jst = now_str()
    t = now_jst()

    print("=" * 60)
    print(f"🔄 Weekly Collection v2: {t:%Y-%m-%d %H:%M} JST")
    print("=" * 60)

    try:
        # ── Phase 1: ロスター取得 ──
        print("\n📋 Phase 1: Full roster from Caskan...")
        roster = scrape_roster()
        roster_ids = {m["therapist_id"] for m in roster}
        print(f"  ✅ {len(roster)} therapists on roster")

        for member in roster:
            upsert_therapist_from_roster(conn, member, jst)

        # ── Phase 2: 引退検出 ──
        print("\n🔍 Phase 2: Retirement detection...")
        retired = detect_retirements(conn, roster_ids, jst)
        if retired:
            for r in retired:
                print(f"  🚪 RETIRED: {r['name']} (ID={r['therapist_id']})")
        else:
            print("  (no retirements detected)")

        # ── Phase 3: 14日スケジュール取得 ──
        print("\n📅 Phase 3: 14-day schedules...")
        total_scheds = 0
        active_count = 0
        for i, member in enumerate(roster):
            try:
                scheds = scrape_14day_schedule(member["therapist_id"])
                for s in scheds:
                    upsert_schedule(conn, s, jst)
                total_scheds += len(scheds)
                if scheds:
                    active_count += 1
            except Exception as e:
                pass  # Individual page failures are non-fatal

            if (i + 1) % 20 == 0:
                print(f"  ... {i + 1}/{len(roster)}")

        print(f"  ✅ {total_scheds} shifts from {active_count} active therapists")

        # ── Phase 4: スケジュールページからの満了情報更新 ──
        # 公式サイトの7日分で is_fully_booked を更新
        print("\n🔄 Phase 4: Booking status from schedule page...")
        week_data = scrape_week()
        booked_count = 0
        for date, records in week_data.items():
            for rec in records:
                # セラピスト情報補完（公式サイト由来）
                conn.execute("""
                    INSERT INTO therapists
                        (therapist_id, name, name_raw, age, height_cm, cup_size, profile_text, first_seen, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(therapist_id) DO UPDATE SET
                        name_raw     = COALESCE(excluded.name_raw, therapists.name_raw),
                        profile_text = COALESCE(excluded.profile_text, therapists.profile_text),
                        last_seen    = ?
                """, (
                    rec["therapist_id"], rec["name"], rec["name_raw"],
                    rec["age"], rec["height_cm"], rec["cup_size"], rec["profile_text"],
                    jst, jst, jst
                ))

                # is_fully_booked を更新
                if rec["is_fully_booked"]:
                    conn.execute("""
                        UPDATE daily_schedules SET is_fully_booked = 1, scraped_at = ?
                        WHERE therapist_id = ? AND schedule_date = ?
                    """, (jst, rec["therapist_id"], rec["schedule_date"]))
                    booked_count += 1

        print(f"  ✅ {booked_count} pre-booked slots marked")

        # ── ログ記録 ──
        conn.execute("""
            INSERT INTO scrape_logs (run_at, task_type, target_date, records_found, success)
            VALUES (?, 'weekly_v2', ?, ?, 1)
        """, (jst, today_str(), total_scheds))

        conn.commit()
        print(f"\n📊 Summary: {len(roster)} roster / {total_scheds} schedules / {len(retired)} retired / {booked_count} pre-booked")

    except Exception as e:
        conn.execute("""
            INSERT INTO scrape_logs (run_at, task_type, success, error_message)
            VALUES (?, 'weekly_v2', 0, ?)
        """, (jst, str(e)))
        conn.commit()
        print(f"❌ Error: {e}")
        raise
    finally:
        conn.close()

    print("✅ Complete")


if __name__ == "__main__":
    run()
