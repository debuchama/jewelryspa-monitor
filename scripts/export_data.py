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
    # 偽満了を除外: scraped_at がシフト終了2h20m以上前のis_fully_bookedのみカウント
    rows = conn.execute("""
        WITH adjusted AS (
            SELECT
                ds.therapist_id,
                ds.is_fully_booked,
                ds.start_time, ds.end_time, ds.scraped_at,
                CASE
                    WHEN ds.is_fully_booked = 1 AND ds.scraped_at IS NOT NULL
                         AND ds.end_time IS NOT NULL THEN
                        CASE
                            -- 深夜帯補正してend - scraped >= 140分 (80min course + 60min margin)
                            WHEN (CASE WHEN CAST(substr(ds.end_time,1,2) AS INT) < 6
                                       THEN CAST(substr(ds.end_time,1,2) AS INT) + 24
                                       ELSE CAST(substr(ds.end_time,1,2) AS INT) END) * 60
                                + CAST(substr(ds.end_time,4,2) AS INT)
                                - (CASE WHEN CAST(substr(substr(ds.scraped_at,-8,5),1,2) AS INT) < 6
                                        THEN CAST(substr(substr(ds.scraped_at,-8,5),1,2) AS INT) + 24
                                        ELSE CAST(substr(substr(ds.scraped_at,-8,5),1,2) AS INT) END) * 60
                                - CAST(substr(substr(ds.scraped_at,-8,5),4,2) AS INT)
                                >= 140 THEN 1
                            ELSE 0
                        END
                    ELSE 0
                END as is_truly_prebooked
            FROM daily_schedules ds
        )
        SELECT
            t.therapist_id, t.name,
            COUNT(*) as total_shifts,
            SUM(a.is_truly_prebooked) as prebooked_count,
            ROUND(100.0 * SUM(a.is_truly_prebooked) / COUNT(*), 1) as prebooked_rate,
            SUM(a.is_fully_booked) - SUM(a.is_truly_prebooked) as fake_booked_count,
            GROUP_CONCAT(DISTINCT ds2.location) as locations
        FROM adjusted a
        JOIN therapists t ON a.therapist_id = t.therapist_id
        JOIN daily_schedules ds2 ON a.therapist_id = ds2.therapist_id
            AND a.start_time = ds2.start_time AND a.end_time = ds2.end_time
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

    # ── 18. 総合人気スコア（v6: クリティカルシンキング全面改訂）──
    #
    # 発見された問題と対策:
    #   Bug1: is_fully_booked=1 だが同日のスロットデータが0% → スロットを正とする
    #   Bug2: 占有率の膨張（total_slots縮小）→ 初回スナップの total_slots で補正
    #   Bug3: 残時間<80分で全枠× → 偽100%として除外
    #
    # 新シグナル:
    #   ⭐ first_check_booked: 初回チェック時点で予約あり = 監視開始前に予約された = 最強証拠
    #   📈 booking_velocity: 占有率の上昇速度 (%/h) = リアルタイム需要の強さ
    #
    # スコア構成 (0-100):
    #   demand_signal × confidence × 0.55  ← 補正済みピーク占有率
    #   + pre_signal × 0.20                ← 初回チェック予約率 or 事前満了フラグ
    #   + velocity_signal × 0.10           ← 予約速度
    #   + volume_bonus × 0.15              ← 出勤頻度

    import math as _math
    from collections import defaultdict

    MIN_COURSE_MIN = 80

    def _tm(t):
        if not t: return 0
        h, m = map(int, t.split(':'))
        if h < 6: h += 24
        return h * 60 + m

    # ── A: スケジュールデータ ──
    sched_rows = conn.execute("""
        SELECT ds.therapist_id, t.name, ds.schedule_date, ds.location,
               ds.is_fully_booked, ds.start_time, ds.end_time, ds.scraped_at
        FROM daily_schedules ds
        JOIN therapists t ON ds.therapist_id = t.therapist_id
    """).fetchall()

    # ── B: スロットデータ全量取得 ──
    slot_rows = conn.execute("""
        SELECT therapist_id, schedule_date, checked_at,
               total_slots, booked_slots, occupancy_pct,
               first_slot, last_slot,
               ROW_NUMBER() OVER (PARTITION BY therapist_id, schedule_date
                                  ORDER BY checked_at ASC) as rn
        FROM slot_summaries
        ORDER BY therapist_id, schedule_date, checked_at
    """).fetchall()

    # B1: 初回スナップショットのフル枠数 + 初回予約状況
    slot_first = {}     # (tid, date) → {full_total, first_booked, first_occ}
    # B2: ピーク予約数
    slot_peak_bk = {}   # (tid, date) → max booked_slots
    # B3: 全スナップショット（velocity計算用）
    slot_all = defaultdict(list)  # (tid, date) → [{checked_at, booked, total, occ}, ...]

    for sr in slot_rows:
        key = (sr["therapist_id"], sr["schedule_date"])
        slot_all[key].append({
            "at": sr["checked_at"], "bk": sr["booked_slots"],
            "tot": sr["total_slots"], "occ": sr["occupancy_pct"],
            "fs": sr["first_slot"], "ls": sr["last_slot"],
        })
        if sr["rn"] == 1:
            slot_first[key] = {
                "full_total": sr["total_slots"],
                "first_booked": sr["booked_slots"],
                "first_occ": round(100.0 * sr["booked_slots"] / sr["total_slots"], 1) if sr["total_slots"] > 0 else 0,
            }
        # Track peak booked_slots (absolute, not pct)
        prev = slot_peak_bk.get(key, 0)
        if sr["booked_slots"] > prev:
            slot_peak_bk[key] = sr["booked_slots"]

    # B4: 偽100%チェック + 補正占有率
    slot_corrected = {}  # (tid, date) → corrected peak occ
    for key, snaps in slot_all.items():
        ft = slot_first.get(key, {}).get("full_total", 1)
        peak_bk = slot_peak_bk.get(key, 0)
        corrected = round(100.0 * peak_bk / ft, 1) if ft > 0 else 0.0

        # 偽100%チェック: ピーク時のスナップで残り < 80分?
        is_fake = False
        for sn in reversed(snaps):
            if sn["bk"] == peak_bk:
                remaining = (_tm(sn["ls"]) - _tm(sn["fs"])) + 5 if sn["fs"] and sn["ls"] else 999
                if sn["occ"] >= 99.9 and remaining < MIN_COURSE_MIN:
                    is_fake = True
                break

        slot_corrected[key] = 0.0 if is_fake else corrected

    # B5: Velocity (%/h) — 占有率の最大上昇速度
    slot_velocity = {}  # (tid, date) → max velocity %/h
    for key, snaps in slot_all.items():
        ft = slot_first.get(key, {}).get("full_total", 1)
        max_vel = 0.0
        for i in range(1, len(snaps)):
            bk_delta = snaps[i]["bk"] - snaps[i-1]["bk"]
            if bk_delta <= 0:
                continue
            t1 = snaps[i-1]["at"]
            t2 = snaps[i]["at"]
            # Parse time difference in hours
            try:
                from datetime import datetime as dt2
                d1 = dt2.strptime(t1, "%Y-%m-%d %H:%M:%S")
                d2 = dt2.strptime(t2, "%Y-%m-%d %H:%M:%S")
                hours = (d2 - d1).total_seconds() / 3600
                if hours > 0:
                    pct_per_h = (bk_delta / ft * 100) / hours
                    if pct_per_h > max_vel:
                        max_vel = pct_per_h
            except:
                pass
        slot_velocity[key] = round(max_vel, 1)

    # ── C: セラピストごとに集計 ──
    td_map = defaultdict(lambda: {
        "name": "", "total_shifts": 0, "locations": set(),
        "corrected_peaks": [],       # 補正済みピーク占有率リスト
        "first_check_booked": [],    # 初回チェック予約率リスト
        "velocities": [],            # booking velocity リスト
        "prebooked_days": 0,
        "fake_booked_days": 0,
        "slot_data_days": 0,
    })

    for row in sched_rows:
        tid = row["therapist_id"]
        date = row["schedule_date"]
        td = td_map[tid]
        td["name"] = row["name"]
        td["total_shifts"] += 1
        if row["location"] and row["location"] != "不明":
            td["locations"].add(row["location"])

        key = (tid, date)
        has_slot = key in slot_corrected

        if has_slot:
            cpeak = slot_corrected[key]
            if cpeak > 0 or slot_peak_bk.get(key, 0) == 0:
                # 有効なスロットデータ（偽100%除外済み）
                td["corrected_peaks"].append(cpeak)
                td["slot_data_days"] += 1

                # 初回チェック予約
                sf = slot_first.get(key, {})
                if sf.get("first_booked", 0) > 0:
                    td["first_check_booked"].append(sf["first_occ"])

                # Velocity
                vel = slot_velocity.get(key, 0)
                if vel > 0:
                    td["velocities"].append(vel)
            else:
                td["fake_booked_days"] += 1
        elif row["is_fully_booked"]:
            # スロットなし → is_fully_booked を条件付きで信頼
            scraped_min = _tm(
                row["scraped_at"].split(" ")[1][:5] if row["scraped_at"] and " " in row["scraped_at"] else "12:00"
            )
            end_min = _tm(row["end_time"]) if row["end_time"] else 0
            remaining = end_min - scraped_min
            if remaining >= MIN_COURSE_MIN + 60:
                td["prebooked_days"] += 1
            else:
                td["fake_booked_days"] += 1

    # ── D: スコア計算 ──
    max_shifts = max((td["total_shifts"] for td in td_map.values()), default=1)
    composite = []

    for tid, td in td_map.items():
        peaks = td["corrected_peaks"]
        total_shifts = td["total_shifts"]
        pb_days = td["prebooked_days"]
        fcb = td["first_check_booked"]
        vels = td["velocities"]

        # 1. Demand signal: 補正済みピーク占有率の平均
        demand_days = peaks + [100.0] * pb_days  # スロット日 + 事前満了日
        avg_demand = sum(demand_days) / len(demand_days) if demand_days else 0.0
        max_demand = max(demand_days) if demand_days else 0.0

        # 2. Pre-booking signal: 初回チェック予約率
        avg_fcb = sum(fcb) / len(fcb) if fcb else 0.0

        # 3. Velocity signal: 最大上昇速度 (cap at 100)
        max_vel = min(max(vels) if vels else 0.0, 100.0)

        # 4. Volume: log normalized
        volume = min(_math.log(total_shifts + 1) / _math.log(max_shifts + 1), 1.0)

        # 5. Confidence
        evidence_days = len(demand_days)
        confidence = min(evidence_days / max(total_shifts, 3), 1.0)

        # Final score
        demand_s = (avg_demand * 0.6 + max_demand * 0.4) * confidence
        pre_s = avg_fcb * confidence  # FCBは強いシグナルだが信頼度で減衰
        vel_s = max_vel
        vol_s = volume * 100

        score = round(demand_s * 0.55 + pre_s * 0.20 + vel_s * 0.10 + vol_s * 0.15, 1)

        composite.append({
            "therapist_id": tid,
            "name": td["name"],
            "composite_score": score,
            "avg_demand": round(avg_demand, 1),
            "max_demand": round(max_demand, 1),
            "avg_fcb": round(avg_fcb, 1),
            "max_velocity": round(max_vel, 1),
            "confidence": round(confidence, 2),
            "evidence_days": evidence_days,
            "slot_data_days": td["slot_data_days"],
            "prebooked_days": pb_days,
            "fake_booked_days": td["fake_booked_days"],
            "total_shifts": total_shifts,
            "shift_count": total_shifts,
            "prebooked_rate": round(100.0 * pb_days / total_shifts, 1) if total_shifts else 0,
            "avg_occupancy": round(avg_demand, 1),
            "max_occupancy": round(max_demand, 1),
            "locations": ",".join(sorted(td["locations"])) if td["locations"] else "",
        })

    composite.sort(key=lambda x: x["composite_score"], reverse=True)
    data["composite_popularity"] = composite

    # ── 18b. 売上推計 ──
    # コース: 80分=16,000円, 100分=20,000円, 120分=24,000円
    # 指名料: 1,000円 (推定60%指名率)
    # ブロック時間からコースを推定し、日次/週次/月次の売上を算出

    COURSE_MAP = [
        (140, 24000, '120min'),
        (110, 20000, '100min'),
        (70, 16000, '80min'),
        (40, 16000, '80min'),  # short capture → assume 80min
    ]
    NOM_FEE = 600  # 1000 * 0.6 avg

    rev_rows = conn.execute("""
        SELECT ss.therapist_id, ss.schedule_date, ss.booked_ranges, ss.booked_slots,
               ROW_NUMBER() OVER (PARTITION BY ss.therapist_id, ss.schedule_date
                                  ORDER BY ss.booked_slots DESC) as rn
        FROM slot_summaries ss
        WHERE ss.booked_slots > 0
    """).fetchall()

    sched_counts = {}
    for sr in conn.execute("SELECT schedule_date, COUNT(*) as cnt FROM daily_schedules GROUP BY schedule_date").fetchall():
        sched_counts[sr["schedule_date"]] = sr["cnt"]

    from collections import defaultdict as _dd
    daily_rev = _dd(lambda: {"revenue": 0, "sessions": 0, "courses": _dd(int), "staff": 0, "booked_staff": 0})
    _seen_rev = set()

    for r in rev_rows:
        if r["rn"] != 1:
            continue
        key = (r["therapist_id"], r["schedule_date"])
        if key in _seen_rev:
            continue
        _seen_rev.add(key)
        date = r["schedule_date"]
        daily_rev[date]["booked_staff"] += 1
        ranges = json.loads(r["booked_ranges"]) if r["booked_ranges"] else []
        for rng in ranges:
            dur = (_tm(rng[1]) - _tm(rng[0])) + 5
            if dur < 40:
                continue
            price = 16000
            course = '80min'
            for min_dur, p, c in COURSE_MAP:
                if dur >= min_dur:
                    price = p
                    course = c
                    break
            daily_rev[date]["sessions"] += 1
            daily_rev[date]["revenue"] += price + NOM_FEE
            daily_rev[date]["courses"][course] += 1

    for date in daily_rev:
        daily_rev[date]["staff"] = sched_counts.get(date, 0)

    # Build weekly aggregation
    from datetime import datetime as _dt
    weekly_rev = _dd(lambda: {"revenue": 0, "sessions": 0, "days_observed": 0,
                               "staff_days": 0, "week_start": "", "courses": _dd(int)})
    for date_str, dr in daily_rev.items():
        d = _dt.strptime(date_str, "%Y-%m-%d")
        yr, wk, _ = d.isocalendar()
        wk_key = f"{yr}-W{wk:02d}"
        ws = d - timedelta(days=d.weekday())
        weekly_rev[wk_key]["revenue"] += dr["revenue"]
        weekly_rev[wk_key]["sessions"] += dr["sessions"]
        weekly_rev[wk_key]["days_observed"] += 1
        weekly_rev[wk_key]["staff_days"] += dr["staff"]
        weekly_rev[wk_key]["week_start"] = ws.strftime("%m/%d")
        for c, n in dr["courses"].items():
            weekly_rev[wk_key]["courses"][c] += n

    # Project weekly
    weekly_list = []
    for wk in sorted(weekly_rev.keys()):
        wr = weekly_rev[wk]
        avg_daily = wr["revenue"] / max(wr["days_observed"], 1)
        projected = round(avg_daily * 7)
        weekly_list.append({
            "week": wk,
            "week_start": wr["week_start"],
            "days_observed": wr["days_observed"],
            "actual_revenue": wr["revenue"],
            "projected_weekly": projected,
            "sessions": wr["sessions"],
            "avg_daily": round(avg_daily),
            "staff_days": wr["staff_days"],
            "courses": dict(wr["courses"]),
        })

    # Monthly: average weekly projected × 4.3
    total_weeks = len(weekly_list)
    avg_weekly = sum(w["projected_weekly"] for w in weekly_list) / max(total_weeks, 1)
    monthly_est = round(avg_weekly * 4.3)

    # Daily list for chart
    daily_list = []
    for date in sorted(daily_rev.keys()):
        dr = daily_rev[date]
        daily_list.append({
            "date": date,
            "revenue": dr["revenue"],
            "sessions": dr["sessions"],
            "staff": dr["staff"],
            "booked_staff": dr["booked_staff"],
            "courses": dict(dr["courses"]),
        })

    data["revenue_daily"] = daily_list
    data["revenue_weekly"] = weekly_list
    data["revenue_monthly_est"] = monthly_est
    data["revenue_avg_weekly"] = round(avg_weekly)
    print(f"  revenue: {len(daily_list)} days, {len(weekly_list)} weeks, monthly ~¥{monthly_est:,}")

    # ── 19. キャンセル検出（本物のキャンセルのみ）──
    # 偽陽性の排除: Caskanは過ぎた時間のスロットを返さないため、
    # total_slotsが減りbooked_slotsも同量減る → これは時間経過であってキャンセルではない。
    # 本物のキャンセル = booked_slotsがtotal_slots以上に減った場合のみ。
    rows = conn.execute("""
        WITH ranked AS (
            SELECT ss.*, t.name,
                   LAG(ss.booked_slots) OVER (
                       PARTITION BY ss.therapist_id, ss.schedule_date
                       ORDER BY ss.checked_at
                   ) as prev_booked,
                   LAG(ss.total_slots) OVER (
                       PARTITION BY ss.therapist_id, ss.schedule_date
                       ORDER BY ss.checked_at
                   ) as prev_total,
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
               prev_booked, booked_slots as new_booked,
               prev_total, total_slots as new_total,
               (booked_slots - prev_booked) as delta_booked,
               (total_slots - prev_total) as delta_total
        FROM ranked
        WHERE prev_booked IS NOT NULL
          AND (booked_slots - prev_booked) < 0
          AND (booked_slots - prev_booked) < (total_slots - prev_total)
        ORDER BY checked_at DESC
        LIMIT 50
    """, (today,)).fetchall()
    data["cancellation_events"] = [{
        **dict(r),
        "delta": round(r["new_occ"] - r["prev_occ"], 1),
        "freed_slots": -(r["delta_booked"] - min(r["delta_total"], 0)),
    } for r in rows]

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
