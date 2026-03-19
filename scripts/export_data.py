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

    # ── 7c. スロット履歴（日ごとのスナップショット時系列）──
    # 同じ日の中で時間経過とともにどう埋まったかを復元可能にする
    # full_range: セラピストごとの最初のスナップショットのfirst_slot〜last_slot（フルシフト範囲）
    tid_name = {t["therapist_id"]: t["name"] for t in data["therapists"]}
    slot_history = {}
    hist_rows = conn.execute("""
        SELECT ss.schedule_date, ss.checked_at, ss.therapist_id,
               ss.occupancy_pct, ss.booked_slots, ss.total_slots,
               ss.booked_ranges, ss.first_slot, ss.last_slot
        FROM slot_summaries ss
        ORDER BY ss.schedule_date, ss.checked_at, ss.therapist_id
    """).fetchall()

    # Pass 1: collect full_range per therapist per date (earliest snapshot has widest range)
    full_ranges = {}  # (date, tid) → {"fs": ..., "ls": ...}
    for r in hist_rows:
        key = (r["schedule_date"], r["therapist_id"])
        if key not in full_ranges and r["first_slot"] and r["last_slot"]:
            full_ranges[key] = {"fs": r["first_slot"], "ls": r["last_slot"]}

    # Pass 2: build history structure
    for r in hist_rows:
        date = r["schedule_date"]
        check_time = r["checked_at"].split(" ")[1][:5]  # HH:MM
        if date not in slot_history:
            slot_history[date] = {"check_times": [], "snapshots": {}, "full_ranges": {}}
        sh = slot_history[date]
        if check_time not in sh["snapshots"]:
            sh["check_times"].append(check_time)
            sh["snapshots"][check_time] = []

        tid = r["therapist_id"]
        sh["snapshots"][check_time].append({
            "tid": tid,
            "n": tid_name.get(tid, f"ID:{tid}"),
            "occ": r["occupancy_pct"],
            "bk": r["booked_slots"],
            "tot": r["total_slots"],
            "ranges": json.loads(r["booked_ranges"]) if r["booked_ranges"] else [],
            "fs": r["first_slot"],
            "ls": r["last_slot"],
        })

        # Store full_range per therapist (stringify tid for JSON key)
        fr = full_ranges.get((date, tid))
        if fr and str(tid) not in sh["full_ranges"]:
            sh["full_ranges"][str(tid)] = fr

    # deduplicate check_times
    for date in slot_history:
        sh = slot_history[date]
        sh["check_times"] = sorted(set(sh["check_times"]))

    data["slot_history"] = slot_history
    hist_dates = list(slot_history.keys())
    hist_total = sum(len(v["check_times"]) for v in slot_history.values())
    print(f"  slot_history: {len(hist_dates)} dates, {hist_total} total snapshots")

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

    # ── 18. 総合人気スコア（v4: スロット占有率ベース統合需要）──
    #
    # 設計原則:
    #   1. 日ごとに「ピーク需要」を算出:
    #      - スロットデータがある日 → その日の最大占有率（実予約/全枠）
    #      - スロットデータなし + is_fully_booked=1 → 100%とみなす
    #      - スロットデータなし + is_fully_booked=0 → 不明（平均から除外、0%扱いしない）
    #   2. セラピストの人気 = 全出勤日のピーク需要の平均
    #   3. 信頼度 = 「需要判定可能だった日数」/ max(全日数, 3)
    #   4. 最終スコア = avg_demand × confidence × volume_bonus

    import math as _math

    # Step A: 全セラピスト×日のスケジュール + is_fully_booked
    sched_rows = conn.execute("""
        SELECT ds.therapist_id, t.name, ds.schedule_date, ds.location, ds.is_fully_booked
        FROM daily_schedules ds
        JOIN therapists t ON ds.therapist_id = t.therapist_id
    """).fetchall()

    # Step B: スロットデータ（日×セラピストのピーク占有率）
    slot_peaks = {}
    slot_rows = conn.execute("""
        SELECT therapist_id, schedule_date,
               MAX(occupancy_pct) as peak_occ,
               MAX(booked_slots) as peak_booked,
               MAX(total_slots) as total_slots
        FROM slot_summaries
        GROUP BY therapist_id, schedule_date
    """).fetchall()
    for sr in slot_rows:
        slot_peaks[(sr["therapist_id"], sr["schedule_date"])] = {
            "peak_occ": sr["peak_occ"],
            "peak_booked": sr["peak_booked"],
            "total_slots": sr["total_slots"],
        }

    # Step C: セラピストごとにピーク需要を集計
    from collections import defaultdict
    therapist_data = defaultdict(lambda: {
        "name": "", "dates": [], "demand_scores": [],
        "total_shifts": 0, "locations": set(),
        "slot_data_days": 0, "prebooked_days": 0,
    })

    for row in sched_rows:
        tid = row["therapist_id"]
        date = row["schedule_date"]
        td = therapist_data[tid]
        td["name"] = row["name"]
        td["total_shifts"] += 1
        td["dates"].append(date)
        if row["location"] and row["location"] != "不明":
            td["locations"].add(row["location"])

        key = (tid, date)
        if key in slot_peaks:
            # スロットデータあり → 実占有率を使う
            peak = slot_peaks[key]["peak_occ"]
            td["demand_scores"].append(peak)
            td["slot_data_days"] += 1
        elif row["is_fully_booked"]:
            # スロットなし + 満了フラグ → 100%とみなす
            td["demand_scores"].append(100.0)
            td["prebooked_days"] += 1
        else:
            # スロットなし + 未満了 → 不明（計算から除外）
            pass

    # Step D: スコア計算
    max_shifts = max((td["total_shifts"] for td in therapist_data.values()), default=1)
    composite = []

    for tid, td in therapist_data.items():
        scores = td["demand_scores"]
        total_shifts = td["total_shifts"]

        # 平均需要（判定可能な日のみ）
        avg_demand = sum(scores) / len(scores) if scores else 0.0
        max_demand = max(scores) if scores else 0.0

        # 信頼度: 判定可能日数 / max(全出勤日数, 3)
        evidence_days = len(scores)
        confidence = min(evidence_days / max(total_shifts, 3), 1.0)

        # 出勤量ボーナス (0-1): 正規化 + log dampening
        volume_raw = total_shifts / max(max_shifts, 1)
        volume = min(_math.log(total_shifts + 1) / _math.log(max_shifts + 1), 1.0)

        # 最終スコア (0-100)
        # demand_signal(70%) × confidence + volume_bonus(30%)
        demand_signal = avg_demand * 0.6 + max_demand * 0.4
        score = round(demand_signal * confidence * 0.70 + volume * 100 * 0.30, 1)

        composite.append({
            "therapist_id": tid,
            "name": td["name"],
            "composite_score": score,
            "avg_demand": round(avg_demand, 1),
            "max_demand": round(max_demand, 1),
            "confidence": round(confidence, 2),
            "evidence_days": evidence_days,
            "slot_data_days": td["slot_data_days"],
            "prebooked_days": td["prebooked_days"],
            "total_shifts": total_shifts,
            "shift_count": total_shifts,
            "prebooked_rate": round(100.0 * td["prebooked_days"] / total_shifts, 1) if total_shifts else 0,
            "avg_occupancy": round(avg_demand, 1),
            "max_occupancy": round(max_demand, 1),
            "locations": ",".join(sorted(td["locations"])) if td["locations"] else "",
        })

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
