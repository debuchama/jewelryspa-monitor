# JewelrySpa Schedule Monitor — プロジェクト仕様プロンプト

このプロンプトは、ジュエリースパ（赤羽・王子・西新井）の出勤スケジュールを自動収集・DB化・可視化するシステムの全仕様を記述したものです。このプロンプトに基づいてシステムの保守・改修・再構築を行ってください。

---

## 1. プロジェクト概要

### 目的
- メンズエステ「ジュエリースパ」（https://jewelryspa-nishiarai.com/schedule）のスタッフ出勤情報を定期的にスクレイプし、SQLiteにDB化する
- 出勤統計をインタラクティブなダッシュボードで可視化する
- 予約枠の空き状況を定期監視し、時系列で記録する
- お気に入りスタッフの出勤を通知する

### 運用環境
- **実行基盤**: GitHub Actions（完全無料・publicリポジトリ）
- **ダッシュボード公開**: GitHub Pages（`docs/index.html`）
- **データベース**: SQLite（リポジトリ内 `data/jewelryspa.db` にバージョン管理）
- **言語**: Python 3.12 + HTML/JS（Chart.js）
- **リポジトリ**: https://github.com/debuchama/jewelryspa-monitor
- **ダッシュボードURL**: https://debuchama.github.io/jewelryspa-monitor/

---

## 2. 対象サイトの構造

### URL パターン
- スケジュールページ: `https://jewelryspa-nishiarai.com/schedule?day={YYYY-MM-DD}&from={YYYY-MM-DD}`
- セラピストプロフィール: `https://jewelryspa-nishiarai.com/therapist/{cast_id}`
- 予約ページ: `https://r.caskan.jp/jsnishiarai/reserve?cast_id={id}&date={YYYY-MM-DD}&room={店舗名}`

### CMS: Caskan
- サーバーサイドレンダリング（SSR）のため、Playwright不要。`httpx` + `BeautifulSoup` で十分
- カード要素のCSSセレクタ: `div.therapist-datas-each`
- セラピストIDは `a[href*="/therapist/"]` のhrefから `/therapist/(\d+)` で抽出
- 名前は `💎名前💎` 形式。`💎` と空白を除去して格納

### 抽出対象フィールド
各セラピストカードから以下を抽出:
- `therapist_id` (integer): URL内のcast_id
- `name` (text): 💎除去済の表示名
- `name_raw` (text): 元の表示名
- `age` (integer): `(\d{2})歳` パターンから
- `height_cm` (integer): `(\d{3})㎝` パターンから
- `cup_size` (text): `\(([A-K])\)` パターンから
- `location` (text): 赤羽 / 王子 / 西新井（カード内テキストまたはreserveリンクのroomパラメータ）
- `start_time`, `end_time` (text): `HH:MM〜HH:MM` パターン
- `is_fully_booked` (boolean): 「予約満了」テキストの有無
- `profile_text` (text): 名前・数値・店舗名を除いた紹介文

### 店舗一覧
| 店舗 | エリア |
|------|--------|
| 赤羽 | 赤羽エリア |
| 王子 | 王子エリア |
| 西新井 | 西新井エリア |

### 営業時間
12:00〜翌5:00（受付9:00〜3:30）

### 時刻の正規化ルール
- 深夜帯（0:00〜5:59）は +24h して `25:00`〜`29:59` として格納
- これにより日付を跨がずにソート・集計が可能

---

## 3. データベース設計 (SQLite)

### テーブル: therapists（セラピストマスタ）
```sql
CREATE TABLE therapists (
    therapist_id  INTEGER PRIMARY KEY,   -- サイト上のcast_id
    name          TEXT NOT NULL,          -- 表示名（💎除去済）
    name_raw      TEXT,                   -- 元の表示名
    age           INTEGER,
    height_cm     INTEGER,
    cup_size      TEXT,
    profile_text  TEXT,
    first_seen    TEXT DEFAULT (datetime('now','localtime')),
    last_seen     TEXT DEFAULT (datetime('now','localtime')),
    is_active     INTEGER DEFAULT 1
);
```

### テーブル: daily_schedules（日別出勤スケジュール）
```sql
CREATE TABLE daily_schedules (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    therapist_id  INTEGER NOT NULL REFERENCES therapists(therapist_id),
    schedule_date TEXT NOT NULL,          -- YYYY-MM-DD
    location      TEXT NOT NULL,          -- 赤羽 / 王子 / 西新井
    start_time    TEXT NOT NULL,          -- HH:MM（深夜は25:00等）
    end_time      TEXT NOT NULL,
    is_fully_booked INTEGER DEFAULT 0,
    scraped_at    TEXT DEFAULT (datetime('now','localtime')),
    UNIQUE(therapist_id, schedule_date)   -- 冪等な UPSERT 用
);
```

### テーブル: availability_snapshots（定期監視スナップショット）
```sql
CREATE TABLE availability_snapshots (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    checked_at    TEXT DEFAULT (datetime('now','localtime')),
    therapist_id  INTEGER NOT NULL REFERENCES therapists(therapist_id),
    schedule_date TEXT NOT NULL,
    location      TEXT NOT NULL,
    status        TEXT NOT NULL,          -- 'available' / 'fully_booked'
    start_time    TEXT,
    end_time      TEXT
);
```

### テーブル: scrape_logs（スクレイプ実行ログ）
```sql
CREATE TABLE scrape_logs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at        TEXT DEFAULT (datetime('now','localtime')),
    task_type     TEXT NOT NULL,          -- 'weekly' / 'daily_monitor'
    target_date   TEXT,
    records_found INTEGER DEFAULT 0,
    success       INTEGER DEFAULT 1,
    error_message TEXT
);
```

### 設計ポイント
- `PRAGMA journal_mode=WAL` でGitHub Actions並行実行に対応
- `PRAGMA foreign_keys=ON` で参照整合性を保証
- `(therapist_id, schedule_date)` UNIQUE制約で冪等UPSERT
- `availability_snapshots` は追記型 → 「何時に予約が埋まったか」を遡及分析可能

---

## 4. スクレイパー仕様

### 技術スタック
- `httpx` (HTTP client、follow_redirects=True)
- `BeautifulSoup` (HTML parser、html.parser)
- User-Agent: Chrome相当のUA文字列

### タスク分離（3つの独立タスク）

#### タスク1: 週次収集 (`weekly_collector.py`)
- 当日から7日分のスケジュールを順次スクレイプ
- `therapists` テーブルに UPSERT（COALESCE で既存値を保持）
- `daily_schedules` テーブルに UPSERT
- `scrape_logs` に記録
- **実行タイミング**: 毎日 JST 6:00

#### タスク2: 当日モニター (`daily_monitor.py`)
- 当日分のみスクレイプ
- 前回スナップショットとの差分を検出（available ↔ fully_booked の変化）
- `availability_snapshots` に追記
- `--loop N` オプションでN分間隔の継続監視にも対応
- **実行タイミング**: 営業時間中（JST 12:00〜翌5:00）30分ごと

#### タスク3: データエクスポート (`export_data.py`)
- DBから以下のデータセットをJSONに出力:
  1. `therapists`: セラピスト一覧
  2. `weekly_schedules`: 今週のスケジュール（therapist_name JOIN済）
  3. `daily_location_summary`: 店舗別・日別の出勤数集計
  4. `therapist_stats`: セラピスト別出勤回数・平均勤務時間（直近30日）
  5. `shift_coverage`: 日別の昼夜シフト人数
  6. `today_snapshots`: 当日のスナップショット履歴
  7. `scrape_logs`: 直近20件のスクレイプログ
  8. `booking_events`: 予約が埋まったイベント一覧
  9. `popularity_ranking`: 平均埋まり速度による人気度ランキング
  10. `favorites_schedule`: お気に入りスタッフの今週出勤
  11. `favorites_config`: お気に入り設定内容
  12. `generated_at`: 生成日時

---

## 5. ダッシュボード仕様

### 技術スタック
- 単一HTMLファイル（`dashboard_template.html`）に `DASHBOARD_DATA_PLACEHOLDER` を JSON で置換してビルド
- Chart.js 4.4.1 + chartjs-plugin-annotation 3.0.1
- Google Fonts: DM Sans + Noto Sans JP
- ダークテーマ（背景 `#0c0c0f`、ゴールドアクセント `#c9a96e`）

### 店舗カラー
| 店舗 | カラー | CSS変数 |
|------|--------|---------|
| 赤羽 | `#e05555` | `--akabane` |
| 王子 | `#5b8fd9` | `--oji` |
| 西新井 | `#4caf7d` | `--nishiarai` |

### 画面構成

#### ページ上部: お気に入りアラートバナー
- `data/favorites.json` に登録されたスタッフの今週出勤を表示
- 当日出勤は「TODAY」バッジ付き
- 出勤がなければ「No favorite staff scheduled this week」

#### Today タブ
- **メトリクスカード**: 当日の出勤数・空き数・満了数・店舗数
- **店舗別カード**: 赤羽/王子/西新井ごとにスタッフリスト。各スタッフに空き/満了ドット、満了時刻（スナップショットから算出）を表示
- **Ganttタイムライン**: 横棒で全スタッフのシフトを視覚化
  - 店舗別に色分け
  - **現在時刻の赤い縦破線**（`chartjs-plugin-annotation`）
  - 満了スタッフは破線ボーダー + 満了時刻に赤マーカー
  - X軸: 10:00〜翌6:00（深夜帯は25:00〜30:00表記を通常時刻に変換表示）
  - ツールチップで時間帯と満了情報を表示

#### Weekly タブ
- **積み上げ棒グラフ**: 日別・店舗別のスタッフ数
- **ヒートマップ**: 店舗×日のスタッフ数を色の濃さで表現
- **昼夜シフトバランス**: Day（18:00未満開始）vs Night（18:00以降開始）の積み上げ棒

#### Staff Stats タブ
- **人気度ランキングカード**: 予約が埋まるまでの平均時間でソート
  - Lightning（2h未満・赤）/ Moderate（2〜6h・黄）/ Gradual（6h超・緑）
  - スナップショットデータの蓄積で精度が向上する旨を表示
- **出勤頻度横棒グラフ**: セラピスト別シフト回数（店舗で色分け）
- **詳細テーブル**: 名前・シフト数・平均時間・配属店舗・人気度

#### Monitor Log タブ
- **スナップショット履歴**: 当日の各チェック時刻ごとのスタッフ状態
- **スクレイプ実行ログ**: 直近20件の成否・取得件数

### 共通UI仕様
- **スタッフ名は全箇所でプロフィールリンク**
  - `https://jewelryspa-nishiarai.com/therapist/{therapist_id}` への外部リンク（`target="_blank"`）
  - スタイル: ドット下線、ホバーでゴールドに変化
  - ヘルパー関数 `nl(name, therapist_id)` で統一生成
  - `tidByName` ルックアップでtherapist_idが直接ない箇所でも名前からID解決

---

## 6. お気に入りスタッフ管理

### 設定ファイル: `data/favorites.json`
```json
[
  {
    "therapist_id": 54672,
    "name": "ほのか",
    "note": ""
  }
]
```

### 仕様
- `therapist_id` はサイトURLの `/therapist/{id}` から取得
- JSONファイルを編集してgit pushすれば次回ワークフロー実行時にダッシュボードに反映
- `export_data.py` が `_load_favorites()` で読み込み、該当IDの今週スケジュールを `favorites_schedule` として出力
- ファイルが存在しなければ空リスト扱い（エラーにならない）

---

## 7. GitHub Actions ワークフロー

### weekly_collect.yml（メインパイプライン）
- **トリガー**: `cron: '0 21 * * *'`（UTC 21:00 = JST 6:00）+ `workflow_dispatch`
- **処理フロー**:
  1. Python 3.12 セットアップ + `httpx beautifulsoup4` インストール
  2. DB初期化（`db_setup.py`）
  3. 週次収集（`weekly_collector.py`）
  4. 当日モニター（`daily_monitor.py`）
  5. JSONエクスポート（`export_data.py`）
  6. HTMLビルド（テンプレートにJSON埋め込み）
  7. `docs/index.html` にコピー
  8. `data/jewelryspa.db`, `data/dashboard_data.json`, `docs/` を自動コミット+push
- **Permissions**: contents:write, pages:write, id-token:write, actions:read

### availability_monitor.yml（空き監視）
- **トリガー**: `cron: '0,30 3-20 * * *'`（UTC 3:00〜20:00 = JST 12:00〜翌5:00、30分ごと）+ `workflow_dispatch`
- **処理フロー**:
  1. DB初期化 + 当日モニター実行
  2. `data/jewelryspa.db` を自動コミット+push
- **Permissions**: contents:write, actions:read

### 無料枠見積もり
- publicリポジトリ: Actions分数無制限
- privateの場合: 約1,140分/月（Free枠 2,000分に収まる）

---

## 8. ファイル構成

```
jewelry-spa-monitor/
├── .github/workflows/
│   ├── weekly_collect.yml          # 週次収集 + ダッシュボードデプロイ
│   └── availability_monitor.yml    # 営業時間中の空き監視
├── scripts/
│   ├── db_setup.py                 # SQLiteスキーマ初期化
│   ├── scraper.py                  # httpx+BS4 スクレイパー（コア）
│   ├── weekly_collector.py         # 週次出勤データ収集
│   ├── daily_monitor.py            # 当日空き状況モニター
│   └── export_data.py              # DB→JSON エクスポート（人気度分析・お気に入り含む）
├── dashboards/
│   ├── dashboard_template.html     # テンプレート（DASHBOARD_DATA_PLACEHOLDER）
│   └── dashboard.html              # ビルド済み
├── data/
│   ├── jewelryspa.db               # SQLiteデータベース
│   ├── dashboard_data.json         # エクスポートJSON
│   └── favorites.json              # お気に入りスタッフ設定
├── docs/
│   └── index.html                  # GitHub Pages公開用
├── build_dashboard.py              # ローカル一括ビルドスクリプト
├── requirements.txt                # httpx, beautifulsoup4
├── .gitignore
└── README.md
```

---

## 9. 改修・拡張時の注意事項

### スクレイパー
- Caskan CMSのHTML構造が変わった場合、`div.therapist-datas-each` セレクタと各フィールドの正規表現を修正
- サイトがSPAに移行した場合は `httpx` → `Playwright` に切り替え（現状SSRなので不要）
- rate limitに注意（現状1リクエスト/日/ページなので問題なし）

### ダッシュボード
- テンプレート内の `DASHBOARD_DATA_PLACEHOLDER` 文字列がJSONで置換される仕組み。テンプレート直接編集時はこの文字列を壊さないこと
- Chart.jsのCDNバージョンを上げる場合、annotation pluginとの互換性を確認
- スタッフ名リンクは `nl()` ヘルパーで統一。新しく名前を表示する箇所を追加する際も必ずこれを使う

### データ
- SQLiteファイルはGitで管理されるため、肥大化に注意（年間 ~50MB 程度の見積もり）
- 古いスナップショットの定期パージが必要になった場合は `DELETE FROM availability_snapshots WHERE checked_at < date('now', '-90 days')` 等を追加
