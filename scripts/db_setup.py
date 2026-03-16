"""
ジュエリースパ 出勤管理データベース

テーブル設計:
  1. therapists        … セラピストマスタ（ID・名前・属性）
  2. daily_schedules   … 日別シフト（誰が/いつ/どの店舗で/何時〜何時）
  3. availability_snapshots … 定期監視のスナップショット（予約可否の時系列）
  4. scrape_logs        … スクレイプ実行ログ（成否・取得件数）
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "jewelryspa.db")


def get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    # ── セラピストマスタ ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS therapists (
        therapist_id  INTEGER PRIMARY KEY,   -- サイト上のcast_id
        name          TEXT NOT NULL,          -- 表示名（💎除去済）
        name_raw      TEXT,                   -- 元の表示名
        age           INTEGER,
        height_cm     INTEGER,
        cup_size      TEXT,
        profile_text  TEXT,
        first_seen    TEXT NOT NULL DEFAULT '',
        last_seen     TEXT NOT NULL DEFAULT '',
        is_active     INTEGER NOT NULL DEFAULT 1
    )
    """)

    # ── 日別出勤スケジュール ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_schedules (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        therapist_id  INTEGER NOT NULL REFERENCES therapists(therapist_id),
        schedule_date TEXT NOT NULL,          -- YYYY-MM-DD
        location      TEXT NOT NULL,          -- 赤羽 / 王子 / 西新井
        start_time    TEXT NOT NULL,          -- HH:MM (24h, 翌日の場合は25:00等で正規化)
        end_time      TEXT NOT NULL,          -- HH:MM
        is_fully_booked INTEGER NOT NULL DEFAULT 0,
        scraped_at    TEXT NOT NULL DEFAULT '',
        UNIQUE(therapist_id, schedule_date)   -- 同一日に重複登録しない
    )
    """)

    # ── 予約枠スナップショット（定期監視用） ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS availability_snapshots (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        checked_at    TEXT NOT NULL DEFAULT '',
        therapist_id  INTEGER NOT NULL REFERENCES therapists(therapist_id),
        schedule_date TEXT NOT NULL,
        location      TEXT NOT NULL,
        status        TEXT NOT NULL,          -- 'available' / 'fully_booked' / 'not_scheduled'
        start_time    TEXT,
        end_time      TEXT
    )
    """)

    # ── スクレイプログ ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS scrape_logs (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        run_at        TEXT NOT NULL DEFAULT '',
        task_type     TEXT NOT NULL,          -- 'weekly' / 'daily_monitor'
        target_date   TEXT,
        records_found INTEGER DEFAULT 0,
        success       INTEGER NOT NULL DEFAULT 1,
        error_message TEXT
    )
    """)

    # ── インデックス ──
    cur.execute("CREATE INDEX IF NOT EXISTS idx_schedules_date ON daily_schedules(schedule_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_schedules_therapist ON daily_schedules(therapist_id, schedule_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_checked ON availability_snapshots(checked_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_date ON availability_snapshots(schedule_date, therapist_id)")

    conn.commit()
    conn.close()
    print(f"✅ DB initialized: {DB_PATH}")


if __name__ == "__main__":
    init_db()
