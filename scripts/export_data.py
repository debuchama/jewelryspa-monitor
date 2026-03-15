"""
ダッシュボード用データエクスポート

SQLiteからJSONを生成してダッシュボードに渡す。
"""

import json
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from db_setup import get_connection

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def export_dashboard_data():
    conn = get_connection()
    data = {}

    # ── 1. セラピスト一覧 ──
    rows = conn.execute("""
        SELECT therapist_id, name, age, height_cm, cup_size, is_active, first_seen, last_seen
        FROM therapists ORDER BY name
    """).fetchall()
    data["therapists"] = [dict(r) for r in rows]

    # ── 2. 今週のスケジュール ──
    today = datetime.now().strftime("%Y-%m-%d")
    week_end = (datetime.now() + timedelta(days=6)).strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT ds.*, t.name as therapist_name
        FROM daily_schedules ds
        JOIN therapists t ON ds.therapist_id = t.therapist_id
        WHERE ds.schedule_date BETWEEN ? AND ?
        ORDER BY ds.schedule_date, ds.start_time
    """, (today, week_end)).fetchall()
    data["weekly_schedules"] = [dict(r) for r in rows]

    # ── 3. 店舗別・日別集計 ──
    rows = conn.execute("""
        SELECT
            schedule_date,
            location,
            COUNT(*) as total_staff,
            SUM(CASE WHEN is_fully_booked = 0 THEN 1 ELSE 0 END) as available_staff,
            SUM(CASE WHEN is_fully_booked = 1 THEN 1 ELSE 0 END) as booked_staff
        FROM daily_schedules
        WHERE schedule_date BETWEEN ? AND ?
        GROUP BY schedule_date, location
        ORDER BY schedule_date, location
    """, (today, week_end)).fetchall()
    data["daily_location_summary"] = [dict(r) for r in rows]

    # ── 4. セラピスト別出勤回数（直近30日） ──
    month_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT
            t.therapist_id, t.name,
            COUNT(ds.id) as shift_count,
            GROUP_CONCAT(DISTINCT ds.location) as locations,
            ROUND(AVG(
                CASE WHEN ds.end_time IS NOT NULL AND ds.start_time IS NOT NULL THEN
                    CAST(SUBSTR(ds.end_time,1,INSTR(ds.end_time,':')-1) AS REAL)
                    + CAST(SUBSTR(ds.end_time,INSTR(ds.end_time,':')+1) AS REAL)/60.0
                    - CAST(SUBSTR(ds.start_time,1,INSTR(ds.start_time,':')-1) AS REAL)
                    - CAST(SUBSTR(ds.start_time,INSTR(ds.start_time,':')+1) AS REAL)/60.0
                ELSE NULL END
            ), 1) as avg_hours
        FROM therapists t
        LEFT JOIN daily_schedules ds ON t.therapist_id = ds.therapist_id
            AND ds.schedule_date >= ?
        GROUP BY t.therapist_id
        ORDER BY shift_count DESC
    """, (month_ago,)).fetchall()
    data["therapist_stats"] = [dict(r) for r in rows]

    # ── 5. 時間帯別カバー率（今週） ──
    rows = conn.execute("""
        SELECT
            schedule_date,
            SUM(CASE WHEN CAST(SUBSTR(start_time,1,INSTR(start_time,':')-1) AS INT) < 18 THEN 1 ELSE 0 END) as day_shift,
            SUM(CASE WHEN CAST(SUBSTR(start_time,1,INSTR(start_time,':')-1) AS INT) >= 18 THEN 1 ELSE 0 END) as night_shift,
            COUNT(*) as total
        FROM daily_schedules
        WHERE schedule_date BETWEEN ? AND ?
        GROUP BY schedule_date
        ORDER BY schedule_date
    """, (today, week_end)).fetchall()
    data["shift_coverage"] = [dict(r) for r in rows]

    # ── 6. 当日の空き状況スナップショット履歴 ──
    rows = conn.execute("""
        SELECT
            av.checked_at,
            t.name as therapist_name,
            av.location,
            av.status,
            av.start_time,
            av.end_time
        FROM availability_snapshots av
        JOIN therapists t ON av.therapist_id = t.therapist_id
        WHERE av.schedule_date = ?
        ORDER BY av.checked_at DESC, t.name
    """, (today,)).fetchall()
    data["today_snapshots"] = [dict(r) for r in rows]

    # ── 7. スクレイプ実行ログ（直近20件）──
    rows = conn.execute("""
        SELECT * FROM scrape_logs ORDER BY run_at DESC LIMIT 20
    """).fetchall()
    data["scrape_logs"] = [dict(r) for r in rows]

    data["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn.close()

    out_path = os.path.join(OUTPUT_DIR, "dashboard_data.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ Exported to {out_path}")
    return data


if __name__ == "__main__":
    d = export_dashboard_data()
    print(f"  therapists: {len(d['therapists'])}")
    print(f"  weekly_schedules: {len(d['weekly_schedules'])}")
    print(f"  therapist_stats: {len(d['therapist_stats'])}")
