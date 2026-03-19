"""
ダッシュボード用データエクスポート

SQLiteからJSONを生成してダッシュボードに渡す。
"""

import json
import sys
import os
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))
from db_setup import get_connection

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
FAVORITES_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "favorites.json")

JST = timezone(timedelta(hours=9))
def _now_jst(): return datetime.now(JST)


def _load_favorites() -> list[dict]:
    """
    お気に入りスタッフ設定を読み込む。
    data/favorites.json が存在しなければ空リスト。
    """
    if os.path.exists(FAVORITES_PATH):
        with open(FAVORITES_PATH, encoding="utf-8") as f:
            return json.load(f)
    return []


def export_dashboard_data():
    conn = get_connection()
    data = {}

    # ── 1. セラピスト一覧 ──
    rows = conn.execute("""
        SELECT therapist_id, name, age, height_cm, cup_size, is_active,
               first_seen, last_seen, photo_url, sns_links, retired_at
        FROM therapists ORDER BY name
    """).fetchall()
    data["therapists"] = [dict(r) for r in rows]

    # ── 2. 14日分のスケジュール ──
    today = _now_jst().strftime("%Y-%m-%d")
    fortnight_end = (_now_jst() + timedelta(days=13)).strftime("%Y-%m-%d")
    week_end = (_now_jst() + timedelta(days=6)).strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT ds.*, t.name as therapist_name
        FROM daily_schedules ds
        JOIN therapists t ON ds.therapist_id = t.therapist_id
        WHERE ds.schedule_date BETWEEN ? AND ?
        ORDER BY ds.schedule_date, ds.start_time
    """, (today, fortnight_end)).fetchall()
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
    """, (today, fortnight_end)).fetchall()
    data["daily_location_summary"] = [dict(r) for r in rows]

    # ── 4. セラピスト別出勤回数（直近30日） ──
    month_ago = (_now_jst() - timedelta(days=30)).strftime("%Y-%m-%d")
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
    """, (today, fortnight_end)).fetchall()
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

    # ── 7b. リアルタイムスロット詳細（最大14日先まで）──
    try:
        from slot_scraper import scrape_slots_range
        all_slots = scrape_slots_range(days=14)
        tid_name = {t["therapist_id"]: t["name"] for t in data["therapists"]}
        for date_key, slots_list in all_slots.items():
            for s in slots_list:
                s["name"] = tid_name.get(s["therapist_id"], f"ID:{s['therapist_id']}")
                for slot in s.get("slot_detail", []):
                    slot["booked"] = int(slot["booked"])
        # 日付順に整理した辞書としてエクスポート
        data["slot_dates"] = sorted(all_slots.keys())
        data["slots_by_date"] = {d: all_slots[d] for d in sorted(all_slots.keys())}
        # 後方互換: today / tomorrow
        data["realtime_slots_today"] = all_slots.get(today, [])
        tomorrow = (_now_jst() + timedelta(days=1)).strftime("%Y-%m-%d")
        data["realtime_slots_tomorrow"] = all_slots.get(tomorrow, [])
        total_therapists = sum(len(v) for v in all_slots.values())
        print(f"  realtime_slots: {len(all_slots)} days, {total_therapists} total entries")
    except Exception as e:
        data["slot_dates"] = []
        data["slots_by_date"] = {}
        data["realtime_slots_today"] = []
        data["realtime_slots_tomorrow"] = []
        print(f"  ⚠️ slot fetch skipped: {e}")

    # ── 8. 人気度分析（予約が埋まるスピード）──
    # 各セラピストの「available → fully_booked」遷移を検出し、
    # シフト開始からの経過時間で人気度スコアを算出
    rows = conn.execute("""
        SELECT
            av.therapist_id,
            t.name,
            av.schedule_date,
            av.start_time,
            MIN(CASE WHEN av.status = 'fully_booked' THEN av.checked_at END) as booked_at,
            MIN(CASE WHEN av.status = 'available' THEN av.checked_at END) as first_seen_at
        FROM availability_snapshots av
        JOIN therapists t ON av.therapist_id = t.therapist_id
        GROUP BY av.therapist_id, av.schedule_date
        HAVING booked_at IS NOT NULL
        ORDER BY av.schedule_date DESC, booked_at
    """).fetchall()
    data["booking_events"] = [dict(r) for r in rows]

    # セラピスト別の平均埋まり速度（hours from shift start to fully_booked）
    rows = conn.execute("""
        WITH booked_times AS (
            SELECT
                av.therapist_id,
                av.schedule_date,
                av.start_time,
                MIN(CASE WHEN av.status = 'fully_booked' THEN av.checked_at END) as booked_at,
                MIN(CASE WHEN av.status = 'available' THEN av.checked_at END) as first_avail
            FROM availability_snapshots av
            GROUP BY av.therapist_id, av.schedule_date
            HAVING booked_at IS NOT NULL AND first_avail IS NOT NULL
        )
        SELECT
            bt.therapist_id,
            t.name,
            COUNT(*) as times_booked,
            ROUND(AVG(
                (julianday(bt.booked_at) - julianday(bt.first_avail)) * 24.0
            ), 1) as avg_hours_to_book,
            GROUP_CONCAT(DISTINCT ds.location) as locations
        FROM booked_times bt
        JOIN therapists t ON bt.therapist_id = t.therapist_id
        LEFT JOIN daily_schedules ds ON bt.therapist_id = ds.therapist_id
            AND bt.schedule_date = ds.schedule_date
        GROUP BY bt.therapist_id
        ORDER BY avg_hours_to_book ASC
    """).fetchall()
    data["popularity_ranking"] = [dict(r) for r in rows]

    # ── 9. お気に入りスタッフの今週出勤 ──
    favorites = _load_favorites()
    data["favorites_config"] = favorites
    if favorites:
        fav_ids = [f["therapist_id"] for f in favorites]
        placeholders = ",".join("?" * len(fav_ids))
        rows = conn.execute(f"""
            SELECT ds.*, t.name as therapist_name
            FROM daily_schedules ds
            JOIN therapists t ON ds.therapist_id = t.therapist_id
            WHERE ds.therapist_id IN ({placeholders})
              AND ds.schedule_date BETWEEN ? AND ?
            ORDER BY ds.schedule_date, ds.start_time
        """, fav_ids + [today, fortnight_end]).fetchall()
        data["favorites_schedule"] = [dict(r) for r in rows]
    else:
        data["favorites_schedule"] = []

    # ── 10. 事前満了率による人気度分析 ──
    # 週次スクレイプ時点で既に予約満了 = スケジュール公開前〜直後に埋まった
    rows = conn.execute("""
        SELECT
            t.therapist_id, t.name,
            COUNT(*) as total_shifts,
            SUM(ds.is_fully_booked) as prebooked_count,
            ROUND(100.0 * SUM(ds.is_fully_booked) / COUNT(*), 1) as prebooked_rate,
            GROUP_CONCAT(DISTINCT ds.location) as locations
        FROM daily_schedules ds
        JOIN therapists t ON ds.therapist_id = t.therapist_id
        GROUP BY t.therapist_id
        HAVING total_shifts >= 1
        ORDER BY prebooked_rate DESC, prebooked_count DESC
    """).fetchall()
    data["prebooked_ranking"] = [dict(r) for r in rows]

    # ── 11. 週間満了マトリクス（全日程×全セラピスト）──
    rows = conn.execute("""
        SELECT
            ds.schedule_date,
            t.therapist_id, t.name as therapist_name,
            ds.location,
            ds.start_time, ds.end_time,
            ds.is_fully_booked
        FROM daily_schedules ds
        JOIN therapists t ON ds.therapist_id = t.therapist_id
        WHERE ds.schedule_date BETWEEN ? AND ?
        ORDER BY t.name, ds.schedule_date
    """, (today, fortnight_end)).fetchall()
    data["weekly_booked_matrix"] = [dict(r) for r in rows]

    # ── 12. 過去全期間の事前満了履歴（トレンド用）──
    rows = conn.execute("""
        SELECT
            ds.schedule_date,
            COUNT(*) as total_staff,
            SUM(ds.is_fully_booked) as prebooked_staff,
            ROUND(100.0 * SUM(ds.is_fully_booked) / COUNT(*), 1) as prebooked_rate
        FROM daily_schedules ds
        GROUP BY ds.schedule_date
        ORDER BY ds.schedule_date
    """).fetchall()
    data["daily_prebooked_trend"] = [dict(r) for r in rows]

    # ══════════════════════════════════════════════
    # Phase 1-4: スロットベースの高精度分析
    # ══════════════════════════════════════════════

    # ── 13. 現在の占有率（最新スナップショット）──
    rows = conn.execute("""
        SELECT ss.therapist_id, t.name, ss.schedule_date,
               ss.total_slots, ss.booked_slots, ss.occupancy_pct,
               ss.first_slot, ss.last_slot, ss.booked_ranges, ss.checked_at
        FROM slot_summaries ss
        JOIN therapists t ON ss.therapist_id = t.therapist_id
        WHERE ss.checked_at = (
            SELECT MAX(checked_at) FROM slot_summaries
            WHERE schedule_date = ss.schedule_date
        )
        AND ss.schedule_date BETWEEN ? AND date(?, '+1 day')
        ORDER BY ss.occupancy_pct DESC
    """, (today, today)).fetchall()
    data["current_occupancy"] = [dict(r) for r in rows]

    # ── 14. 占有率タイムライン（当日の全チェック履歴）──
    rows = conn.execute("""
        SELECT ss.checked_at, ss.therapist_id, t.name,
               ss.occupancy_pct, ss.booked_slots, ss.total_slots,
               ss.booked_ranges
        FROM slot_summaries ss
        JOIN therapists t ON ss.therapist_id = t.therapist_id
        WHERE ss.schedule_date = ?
        ORDER BY ss.checked_at, t.name
    """, (today,)).fetchall()
    data["occupancy_timeline"] = [dict(r) for r in rows]

    # ── 15. 充足速度（直近2チェックの占有率差分）──
    rows = conn.execute("""
        WITH ranked AS (
            SELECT ss.*, t.name,
                   ROW_NUMBER() OVER (PARTITION BY ss.therapist_id, ss.schedule_date
                                      ORDER BY ss.checked_at DESC) as rn
            FROM slot_summaries ss
            JOIN therapists t ON ss.therapist_id = t.therapist_id
            WHERE ss.schedule_date = ?
        )
        SELECT
            cur.therapist_id, cur.name, cur.occupancy_pct as current_pct,
            prev.occupancy_pct as prev_pct,
            ROUND(cur.occupancy_pct - prev.occupancy_pct, 1) as velocity,
            cur.checked_at as cur_time, prev.checked_at as prev_time
        FROM ranked cur
        LEFT JOIN ranked prev ON cur.therapist_id = prev.therapist_id AND prev.rn = 2
        WHERE cur.rn = 1
        ORDER BY velocity DESC
    """, (today,)).fetchall()
    data["fill_velocity"] = [dict(r) for r in rows]

    # ── 16. ゴールデンタイム分析（セラピスト別）──
    # 最新スナップショットの booked_ranges からプライムタイム占有率を推定
    # → JS側で booked_ranges を解析して計算するためデータは current_occupancy で十分

    # ── 17. 曜日×時間帯 需要マトリクス（過去データから集計）──
    rows = conn.execute("""
        SELECT
            CAST(strftime('%w', ss.schedule_date) AS INT) as dow,
            ROUND(AVG(ss.occupancy_pct), 1) as avg_occupancy,
            COUNT(DISTINCT ss.therapist_id) as sample_therapists,
            COUNT(*) as sample_count
        FROM slot_summaries ss
        GROUP BY dow
        ORDER BY dow
    """).fetchall()
    data["dow_demand"] = [dict(r) for r in rows]

    # ── 18. 総合人気スコア（信頼度補正済み）──
    #
    # 問題点の修正:
    #   - 1シフトで100%満了 → サンプル不足。信頼度ペナルティ適用
    #   - location="不明" → スケルトン公開の可能性。除外
    #   - スロットデータが少ない → max_occupancyも加味
    #   - 出勤回数が多い安定的な人気 → ボーナス
    #
    # 最終スコア (0-100):
    #   adjusted_prebooked * 0.30  ← 信頼度補正済み事前満了率
    #   + slot_signal * 0.40       ← avg_occ と max_occ のブレンド
    #   + latest_occ * 0.15        ← リアルタイム状況
    #   + regularity * 0.15        ← 出勤頻度ボーナス (max 100)

    rows = conn.execute("""
        WITH
        pb_raw AS (
            SELECT therapist_id,
                   COUNT(*) as total_shifts,
                   SUM(CASE WHEN is_fully_booked = 1 THEN 1 ELSE 0 END) as booked_shifts,
                   COUNT(CASE WHEN location != '不明' THEN 1 END) as valid_shifts,
                   SUM(CASE WHEN is_fully_booked = 1 AND location != '不明' THEN 1 ELSE 0 END) as valid_booked
            FROM daily_schedules
            GROUP BY therapist_id
        ),
        slot_agg AS (
            SELECT therapist_id,
                   ROUND(AVG(occupancy_pct), 1) as avg_occ,
                   ROUND(MAX(occupancy_pct), 1) as max_occ,
                   COUNT(*) as slot_checks,
                   COUNT(DISTINCT schedule_date) as slot_dates
            FROM slot_summaries
            GROUP BY therapist_id
        ),
        latest_occ AS (
            SELECT therapist_id, occupancy_pct as latest_occ
            FROM slot_summaries
            WHERE checked_at = (SELECT MAX(checked_at) FROM slot_summaries)
        )
        SELECT
            t.therapist_id, t.name,
            -- raw metrics
            COALESCE(pb.valid_shifts, 0) as valid_shifts,
            COALESCE(pb.valid_booked, 0) as valid_booked,
            COALESCE(pb.total_shifts, 0) as total_shifts,
            CASE WHEN COALESCE(pb.valid_shifts, 0) > 0
                 THEN ROUND(100.0 * pb.valid_booked / pb.valid_shifts, 1)
                 ELSE 0 END as raw_pb_rate,
            -- confidence: min(valid_shifts, 5) / 5
            ROUND(MIN(COALESCE(pb.valid_shifts, 0), 5) / 5.0, 2) as confidence,
            -- slot metrics
            COALESCE(sa.avg_occ, 0) as avg_occupancy,
            COALESCE(sa.max_occ, 0) as max_occupancy,
            COALESCE(lo.latest_occ, 0) as latest_occupancy,
            COALESCE(sa.slot_checks, 0) as slot_checks,
            -- regularity: shift_count normalized (max 10 shifts = 100)
            MIN(COALESCE(pb.total_shifts, 0) * 10, 100) as regularity,
            -- locations
            GROUP_CONCAT(DISTINCT ds.location) as locations
        FROM therapists t
        LEFT JOIN pb_raw pb ON t.therapist_id = pb.therapist_id
        LEFT JOIN slot_agg sa ON t.therapist_id = sa.therapist_id
        LEFT JOIN latest_occ lo ON t.therapist_id = lo.therapist_id
        LEFT JOIN daily_schedules ds ON t.therapist_id = ds.therapist_id
        GROUP BY t.therapist_id
        HAVING COALESCE(pb.total_shifts, 0) > 0
    """).fetchall()

    composite = []
    for r in rows:
        d = dict(r)
        raw_pb = d["raw_pb_rate"]
        conf = d["confidence"]
        avg_occ = d["avg_occupancy"]
        max_occ = d["max_occupancy"]
        latest = d["latest_occupancy"]
        reg = d["regularity"]

        # Adjusted pre-booked: rate * confidence factor
        adj_pb = raw_pb * conf

        # Slot signal: blend avg and max (max matters when data is sparse)
        slot_signal = max(avg_occ, max_occ * 0.5) if (avg_occ > 0 or max_occ > 0) else 0

        # Final composite score
        score = round(adj_pb * 0.30 + slot_signal * 0.40 + latest * 0.15 + reg * 0.15, 1)

        d["adjusted_pb"] = round(adj_pb, 1)
        d["slot_signal"] = round(slot_signal, 1)
        d["composite_score"] = score
        # Keep backward compat field names
        d["prebooked_rate"] = d["raw_pb_rate"]
        d["shift_count"] = d["total_shifts"]
        composite.append(d)

    composite.sort(key=lambda x: x["composite_score"], reverse=True)
    data["composite_popularity"] = composite

    # ── 19. キャンセル検出（占有率が下がったイベント）──
    rows = conn.execute("""
        WITH ranked AS (
            SELECT ss.*, t.name,
                   LAG(ss.occupancy_pct) OVER (
                       PARTITION BY ss.therapist_id, ss.schedule_date
                       ORDER BY ss.checked_at
                   ) as prev_occ,
                   LAG(ss.checked_at) OVER (
                       PARTITION BY ss.therapist_id, ss.schedule_date
                       ORDER BY ss.checked_at
                   ) as prev_time
            FROM slot_summaries ss
            JOIN therapists t ON ss.therapist_id = t.therapist_id
            WHERE ss.schedule_date >= ?
        )
        SELECT therapist_id, name, schedule_date, checked_at,
               prev_occ, occupancy_pct as new_occ,
               ROUND(occupancy_pct - prev_occ, 1) as delta
        FROM ranked
        WHERE prev_occ IS NOT NULL AND occupancy_pct < prev_occ
        ORDER BY checked_at DESC
        LIMIT 50
    """, (today,)).fetchall()
    data["cancellation_events"] = [dict(r) for r in rows]

    # ── 20. お気に入り推奨タイミング ──
    if favorites:
        fav_ids = [f["therapist_id"] for f in favorites]
        placeholders = ",".join("?" * len(fav_ids))
        rows = conn.execute(f"""
            SELECT
                ss.therapist_id, t.name,
                CAST(strftime('%w', ss.schedule_date) AS INT) as dow,
                ROUND(AVG(ss.occupancy_pct), 1) as avg_occ,
                COUNT(*) as samples
            FROM slot_summaries ss
            JOIN therapists t ON ss.therapist_id = t.therapist_id
            WHERE ss.therapist_id IN ({placeholders})
            GROUP BY ss.therapist_id, dow
            ORDER BY ss.therapist_id, avg_occ ASC
        """, fav_ids).fetchall()
        data["favorite_timing"] = [dict(r) for r in rows]
    else:
        data["favorite_timing"] = []

    # ── 21. 新人トラッキング（入店30日以内）──
    rows = conn.execute("""
        SELECT t.therapist_id, t.name, t.first_seen,
               CAST(julianday(?) - julianday(t.first_seen) AS INT) as days_since_first,
               COALESCE(ao.avg_occ, 0) as avg_occupancy,
               COUNT(DISTINCT ds.schedule_date) as shift_count
        FROM therapists t
        LEFT JOIN (
            SELECT therapist_id, ROUND(AVG(occupancy_pct), 1) as avg_occ
            FROM slot_summaries GROUP BY therapist_id
        ) ao ON t.therapist_id = ao.therapist_id
        LEFT JOIN daily_schedules ds ON t.therapist_id = ds.therapist_id
        WHERE julianday(?) - julianday(t.first_seen) <= 30
          AND t.first_seen != ''
        GROUP BY t.therapist_id
        ORDER BY avg_occupancy DESC
    """, (today, today)).fetchall()
    data["newcomers"] = [dict(r) for r in rows]

    # ── 22. 引退・離脱セラピスト ──
    rows = conn.execute("""
        SELECT therapist_id, name, first_seen, retired_at,
               CAST(julianday(retired_at) - julianday(first_seen) AS INT) as tenure_days
        FROM therapists
        WHERE retired_at IS NOT NULL
        ORDER BY retired_at DESC
    """).fetchall()
    data["retired_therapists"] = [dict(r) for r in rows]

    # ── 23. ロスター統計 ──
    row = conn.execute("""
        SELECT
            COUNT(*) as total_ever,
            SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_now,
            SUM(CASE WHEN retired_at IS NOT NULL THEN 1 ELSE 0 END) as retired
        FROM therapists
    """).fetchone()
    data["roster_stats"] = dict(row) if row else {"total_ever": 0, "active_now": 0, "retired": 0}

    data["generated_at"] = _now_jst().strftime("%Y-%m-%d %H:%M:%S")

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
