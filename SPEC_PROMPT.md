# JewelrySpa Schedule Monitor — システム仕様書 v3

このドキュメントは、ジュエリースパ出勤スケジュール監視・分析システムの完全な仕様です。新しいチャットセッションでこのプロンプトを使えば、システムの保守・改修・再構築が行えます。

---

## 1. 概要

### 目的
- メンズエステ「ジュエリースパ」（赤羽・王子・西新井）のスタッフ出勤・予約データを自動収集しSQLiteにDB化
- 5分刻みの予約スロット空き状況を定期監視し、占有率・人気度・充足速度を分析
- インタラクティブなダッシュボードで全データを可視化（GitHub Pages公開）
- お気に入りスタッフの出勤通知と予約タイミング推奨

### 運用環境
- **リポジトリ**: https://github.com/debuchama/jewelryspa-monitor
- **ダッシュボード**: https://debuchama.github.io/jewelryspa-monitor/
- **実行基盤**: GitHub Actions（publicリポ → 分数無制限）
- **DB**: SQLite（WALモード、リポジトリ内 `data/jewelryspa.db`）
- **言語**: Python 3.12 + HTML/JS（Chart.js 4.4.1 + chartjs-plugin-annotation）
- **スクレイピング**: httpx + BeautifulSoup（Playwright不要 — Caskan CMSはSSR）

---

## 2. データソース

### 2.1 スケジュールページ（メインサイト）
- **URL**: `https://jewelryspa-nishiarai.com/schedule?day={YYYY-MM-DD}&from={YYYY-MM-DD}`
- **CMS**: Caskan（SSR）
- **カード要素**: `div.therapist-datas-each`
- **取得項目**: therapist_id（`/therapist/{id}`から）、名前（`💎`除去）、年齢、身長、カップ、店舗（赤羽/王子/西新井）、勤務時間（HH:MM〜HH:MM）、予約満了フラグ、プロフィールテキスト

### 2.2 予約ページ（Caskan）— ★最重要データソース
- **URL**: `https://r.caskan.jp/jsnishiarai?date={YYYY-MM-DD}`
- **特性**: 1リクエストで当日の全セラピストの5分刻みスロットデータがSSRで返る
- **構造**:
  - 各セラピストごとに `div#cast-hour-{cast_id}` 内に `<table>`
  - `<th>`: 5分刻みタイムスロット（12:00, 12:05, ...）
  - `<td>` に `<input type="radio">` → **空き枠**
  - `<td>` にテキスト `x` のみ → **予約済み枠**
- **データ取得範囲**: シフトが公開されている日のみ（通常6〜7日先まで、最大14日先）
- **スロット粒度**: 5分ごと（1セラピストあたり60〜160スロット/日）

### 2.3 セラピストプロフィール
- **URL**: `https://jewelryspa-nishiarai.com/therapist/{cast_id}`
- ダッシュボード上の全スタッフ名はこのURLへのリンク

### 時刻正規化ルール
- 深夜帯（0:00〜5:59）は +24h して `25:00`〜`29:59` として格納
- 表示時は24H表記にゼロパディング（`01:00`, `20:30`）

### タイムゾーン
- GitHub ActionsはUTC環境。全スクリプトは `scripts/tz.py` 経由で明示的にJST（UTC+9）を使用
- SQLiteの `datetime('now','localtime')` は使用禁止（UTCになるため）
- Python側から `tz.now_str()` でJST文字列を生成してINSERT

---

## 3. データベース設計

### therapists（セラピストマスタ）
```sql
CREATE TABLE therapists (
    therapist_id  INTEGER PRIMARY KEY,   -- cast_id
    name          TEXT NOT NULL,          -- 💎除去済
    name_raw      TEXT,
    age           INTEGER,
    height_cm     INTEGER,
    cup_size      TEXT,
    profile_text  TEXT,
    first_seen    TEXT NOT NULL DEFAULT '',
    last_seen     TEXT NOT NULL DEFAULT '',
    is_active     INTEGER NOT NULL DEFAULT 1
);
```

### daily_schedules（日別出勤スケジュール）
```sql
CREATE TABLE daily_schedules (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    therapist_id  INTEGER NOT NULL REFERENCES therapists(therapist_id),
    schedule_date TEXT NOT NULL,          -- YYYY-MM-DD
    location      TEXT NOT NULL,          -- 赤羽 / 王子 / 西新井
    start_time    TEXT NOT NULL,          -- HH:MM（深夜25:00等）
    end_time      TEXT NOT NULL,
    is_fully_booked INTEGER NOT NULL DEFAULT 0,
    scraped_at    TEXT NOT NULL DEFAULT '',
    UNIQUE(therapist_id, schedule_date)
);
```

### availability_snapshots（空き監視スナップショット）
```sql
CREATE TABLE availability_snapshots (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    checked_at    TEXT NOT NULL DEFAULT '',
    therapist_id  INTEGER NOT NULL REFERENCES therapists(therapist_id),
    schedule_date TEXT NOT NULL,
    location      TEXT NOT NULL,
    status        TEXT NOT NULL,          -- 'available' / 'fully_booked'
    start_time    TEXT,
    end_time      TEXT
);
```

### slot_summaries（5分スロット占有率の時系列）
```sql
CREATE TABLE slot_summaries (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    checked_at    TEXT NOT NULL,
    therapist_id  INTEGER NOT NULL,
    schedule_date TEXT NOT NULL,
    total_slots   INTEGER NOT NULL,
    booked_slots  INTEGER NOT NULL,
    occupancy_pct REAL NOT NULL,
    first_slot    TEXT,
    last_slot     TEXT,
    booked_ranges TEXT,                   -- JSON: [["22:30","01:50"], ...]
    UNIQUE(checked_at, therapist_id, schedule_date)
);
```

### scrape_logs（実行ログ）
```sql
CREATE TABLE scrape_logs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at        TEXT NOT NULL DEFAULT '',
    task_type     TEXT NOT NULL,          -- 'weekly' / 'daily_monitor' / 'slot_monitor'
    target_date   TEXT,
    records_found INTEGER DEFAULT 0,
    success       INTEGER NOT NULL DEFAULT 1,
    error_message TEXT
);
```

---

## 4. スクリプト構成

### scripts/tz.py — JST タイムゾーンヘルパー
- `now_jst()`: JST datetime
- `now_str()`: `YYYY-MM-DD HH:MM:SS` JST文字列
- `today_str()`: `YYYY-MM-DD` JST今日

### scripts/scraper.py — メインサイトスクレイパー
- `scrape_day(target_date)`: 指定日のスケジュールページからセラピスト情報を抽出
- `scrape_week()`: 7日分を順次取得
- `scrape_today()`: 当日分のみ
- httpx + BeautifulSoup、セレクタ `div.therapist-datas-each`

### scripts/slot_scraper.py — Caskan予約スロットスクレイパー ★
- `scrape_slots(target_date)`: 指定日の全セラピスト5分スロットを1リクエストで取得
- `scrape_slots_range(days=14)`: 最大14日先まで、データがある日すべてを取得（データなしで打ち切り）
- 各セラピストごとに `slot_detail`（5分刻みの空き/予約済み配列）と `booked_ranges`（連続予約ブロック）を返す

### scripts/weekly_collector.py — 週次収集
- 7日分のシフトデータをスクレイプ → therapists + daily_schedules にUPSERT
- cron: 毎日 JST 6:00

### scripts/daily_monitor.py — 空き監視
- 当日の出勤状況をスクレイプ → availability_snapshots に追記
- 前回との差分（available↔fully_booked）を検出

### scripts/slot_monitor.py — スロット占有率監視 ★
- 当日+翌日の5分スロットを取得 → slot_summaries にUPSERT
- 充足速度アラート（占有率が10%以上変化したら検出）

### scripts/export_data.py — ダッシュボード用JSONエクスポート
21種のデータセットを出力:
1. `therapists` — セラピスト一覧
2. `weekly_schedules` — 今週スケジュール
3. `daily_location_summary` — 店舗別・日別集計
4. `therapist_stats` — 出勤回数・平均時間
5. `shift_coverage` — 昼夜シフト人数
6. `today_snapshots` — 当日スナップショット履歴
7. `scrape_logs` — 実行ログ
8. `realtime_slots_today` / `realtime_slots_tomorrow` — リアルタイムスロット（後方互換）
9. `slot_dates` + `slots_by_date` — 全日程スロットデータ（日付タブ用）
10. `booking_events` — 予約埋まりイベント
11. `popularity_ranking` — スナップショットベースの埋まり速度ランキング
12. `prebooked_ranking` — 事前満了率ランキング
13. `weekly_booked_matrix` — 週間満了マトリクス
14. `daily_prebooked_trend` — 日別事前満了率トレンド
15. `current_occupancy` — 最新占有率
16. `occupancy_timeline` — 当日占有率タイムライン
17. `fill_velocity` — 充足速度（直近2チェック差分）
18. `composite_popularity` — 総合人気スコア
19. `cancellation_events` — キャンセル検出
20. `favorite_timing` — お気に入り推奨タイミング
21. `newcomers` — 新人トラッキング

---

## 5. ダッシュボード仕様

### 技術スタック
- 単一HTML（`dashboard_template.html`）に `DASHBOARD_DATA_PLACEHOLDER` をJSON置換してビルド
- Chart.js 4.4.1 + chartjs-plugin-annotation 3.0.1
- ダークテーマ（`#0c0c0f` bg, `#c9a96e` accent）

### 店舗カラー
| 店舗 | 色 | CSS変数 |
|------|-----|---------|
| 赤羽 | `#e05555` | `--aka` |
| 王子 | `#5b8fd9` | `--oji` |
| 西新井 | `#4caf7d` | `--nsh` |

### 5タブ構成

#### Favorites バナー（常時表示）
- お気に入りスタッフの出勤予定、当日は「TODAY」バッジ
- 曜日別占有率から予約おすすめタイミングをTip表示

#### Today タブ
- メトリクスカード（出勤数/空き/満了/平均占有率）
- 占有率メーター（バー + velocity矢印🔥↗→↘）
- 店舗別スタッフカード（占有率%、満了時刻表示付き）
- **5分スロットヒートマップ** ★ — 日付タブ切り替え式、最大14日先まで
  - 統一時間軸（全セラピスト共通）
  - 赤=予約済み、緑=空き、グレー=シフト外
  - 当日はnowマーカー（赤い縦線）
  - 占有率降順ソート、サマリー行
- Ganttタイムライン（現在時刻ライン、満了バー分割表示）

#### Weekly タブ
- 満了マトリクス（●=事前満了、○=空き、·=出勤なし）
- 事前満了率トレンド折れ線グラフ
- 店舗別スタッフ数積み上げ棒グラフ
- 昼夜シフトバランス

#### Stats タブ
- 総合人気スコアカード（事前満了30% + 平均占有率40% + 最新占有率30%）
- 出勤頻度横棒グラフ
- 詳細テーブル（Score / Occ% / Pre-booked / Shifts / Locations）

#### Insights タブ
- 🔥 Filling fast — 占有率急上昇スタッフ
- 🔄 Cancellation — 占有率が下がった＝キャンセル検出
- 🌱 Newcomers — 入店30日以内のスタッフ追跡
- 💡 Favorites timing — 曜日別最適予約タイミング

#### Log タブ
- 当日スナップショット履歴
- スクレイプ実行ログ

### 共通仕様
- 全スタッフ名は `https://jewelryspa-nishiarai.com/therapist/{id}` へのリンク
- ヘルパー関数 `nl(name, therapist_id)` で統一生成
- 時刻は24H表記、ゼロパディング（`fH()`, `fT()` 関数）

---

## 6. お気に入り管理

### 設定ファイル: `data/favorites.json`
```json
[
  { "therapist_id": 54672, "name": "ほのか", "note": "" }
]
```
- `therapist_id` はサイトURL `/therapist/{id}` から取得
- 編集してgit pushすれば次回ワークフロー実行で反映
- ファイル不在時は空リスト（エラーにならない）

---

## 7. GitHub Actions ワークフロー

### weekly_collect.yml（メインパイプライン）
- **cron**: `0 21 * * *`（UTC 21:00 = JST 6:00）+ workflow_dispatch
- **処理**: DB初期化 → 週次収集 → 空き監視 → スロット監視 → JSONエクスポート → HTMLビルド → docs/ にコピー → git commit & push

### availability_monitor.yml（統合モニター）
- **cron**: `*/15 2-20 * * *`（JST 11:00〜翌5:30、15分ごと）+ workflow_dispatch
- **処理**: DB初期化 → 空き監視（daily_monitor.py）→ スロット監視（slot_monitor.py）→ git commit & push
- ※ スロット監視はこのワークフローに統合済み（安定動作のため）

---

## 8. ファイル構成

```
jewelry-spa-monitor/
├── .github/workflows/
│   ├── weekly_collect.yml        # 毎日JST6:00: 収集+ダッシュボードデプロイ
│   └── availability_monitor.yml  # 15分毎: 空き+スロット監視
├── scripts/
│   ├── tz.py                     # JSTヘルパー
│   ├── scraper.py                # メインサイトスクレイパー
│   ├── slot_scraper.py           # Caskan予約スロットスクレイパー ★
│   ├── weekly_collector.py       # 週次収集
│   ├── daily_monitor.py          # 空き監視
│   ├── slot_monitor.py           # スロット占有率監視
│   ├── export_data.py            # 21種データJSONエクスポート
│   └── db_setup.py               # DBスキーマ初期化
├── dashboards/
│   ├── dashboard_template.html   # テンプレート
│   └── dashboard.html            # ビルド済み
├── data/
│   ├── jewelryspa.db             # SQLite
│   ├── dashboard_data.json       # エクスポートJSON
│   └── favorites.json            # お気に入り設定
├── docs/index.html               # GitHub Pages
├── build_dashboard.py            # ローカルビルド
├── requirements.txt              # httpx, beautifulsoup4
├── SPEC_PROMPT.md                # この仕様書
└── README.md
```

---

## 9. 注意事項

### スクレイパー
- Caskan CMSのHTML構造変更時: `div.therapist-datas-each` セレクタ、`cast-hour-{id}` パターン、radio/x判定ロジックを修正
- 予約ページのスロット粒度が変わった場合: slot_scraper.py の `<th>` パース部分を修正

### タイムゾーン
- `datetime('now','localtime')` はGitHub Actionsで使用禁止（UTCになる）
- 必ず `tz.py` の `now_str()` / `today_str()` を使う

### git rebase コンフリクト
- github-actions[bot]の自動コミットとローカルpushが競合する場合:
  - `git checkout --ours data/jewelryspa.db data/dashboard_data.json docs/index.html`
  - `git add -A && GIT_EDITOR=true git rebase --continue`

### DB肥大化
- slot_summariesは15分ごとに蓄積される。年間で数十MB程度の見込み
- 古いデータのパージ: `DELETE FROM slot_summaries WHERE checked_at < date('now', '-90 days')`
