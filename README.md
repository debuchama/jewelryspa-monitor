# JewelrySpa Schedule Monitor

ジュエリースパ（赤羽・王子・西新井）の出勤スケジュールを自動収集・DB化・可視化するシステム。

**GitHub Actions で完全無料稼働** — サーバー不要、GitHub Pages でダッシュボード公開。

## アーキテクチャ

```
┌─────────────────────────────────────────────────┐
│  GitHub Actions (無料)                           │
│                                                  │
│  ① weekly_collect.yml   毎日 JST 6:00            │
│     → 7日分スクレイプ → DB保存 → ダッシュボード生成  │
│     → docs/ にコミット → GitHub Pages 自動更新    │
│                                                  │
│  ② availability_monitor.yml  営業時間中 30分ごと   │
│     → 当日の予約可否をスナップショット → DB保存     │
└─────────────────────┬───────────────────────────┘
                      │ git commit & push
                      ▼
┌─────────────────────────────────────────────────┐
│  GitHub Repository                               │
│  data/jewelryspa.db    ← SQLite (バージョン管理)  │
│  docs/index.html       ← ダッシュボード HTML      │
└─────────────────────┬───────────────────────────┘
                      │ GitHub Pages
                      ▼
            https://<user>.github.io/<repo>/
```

## セットアップ手順

### 1. リポジトリ作成

```bash
cd jewelry-spa-monitor
git init
git add -A
git commit -m "initial commit"
git branch -M main
git remote add origin https://github.com/<YOUR_USER>/<YOUR_REPO>.git
git push -u origin main
```

### 2. GitHub Pages を有効化

1. リポジトリの **Settings** → **Pages**
2. **Source** を `Deploy from a branch` に設定
3. **Branch** を `main`、**Folder** を `/docs` に設定
4. Save

これで `https://<user>.github.io/<repo>/` でダッシュボードが公開されます。

### 3. GitHub Actions のパーミッション確認

1. **Settings** → **Actions** → **General**
2. **Workflow permissions** を `Read and write permissions` に設定
3. Save

### 4. 初回実行

1. リポジトリの **Actions** タブを開く
2. `Weekly Schedule Collection` ワークフローを選択
3. `Run workflow` ボタンで手動実行

初回実行後、ダッシュボードが自動生成されます。

## ファイル構成

```
jewelry-spa-monitor/
├── .github/workflows/
│   ├── weekly_collect.yml        # 週次収集 + ダッシュボードビルド
│   └── availability_monitor.yml  # 営業時間中の空き監視
├── scripts/
│   ├── db_setup.py               # SQLiteスキーマ初期化
│   ├── scraper.py                # httpx+BS4 スクレイパー
│   ├── weekly_collector.py       # 週次出勤データ収集
│   ├── daily_monitor.py          # 当日空き状況モニター
│   └── export_data.py            # DB→JSON エクスポート
├── dashboards/
│   ├── dashboard_template.html   # テンプレート
│   └── dashboard.html            # ビルド済み
├── data/
│   ├── jewelryspa.db             # SQLiteデータベース
│   └── dashboard_data.json       # エクスポートJSON
├── docs/
│   └── index.html                # GitHub Pages公開用
├── build_dashboard.py            # ローカル一括ビルド
├── requirements.txt
└── README.md
```

## GitHub Actions スケジュール

| ワークフロー | 実行タイミング | 処理内容 |
|---|---|---|
| **Weekly Collection** | 毎日 JST 6:00 | 7日分スクレイプ → DB → ダッシュボード → Pages デプロイ |
| **Availability Monitor** | 営業時間中 30分ごと | 当日の予約可否スナップショット → DB |

### 無料枠の使用量目安

- Weekly: 約2分/回 × 30回/月 = **約60分/月**
- Monitor: 約1分/回 × 36回/日 × 30日 = **約1,080分/月**
- GitHub Free の上限: **2,000分/月**（privateリポジトリ）
- **publicリポジトリなら無制限**

→ publicリポジトリ推奨。privateでも余裕あり。

## DBスキーマ

| テーブル | 用途 |
|---|---|
| `therapists` | セラピストマスタ（ID, 名前, 年齢, 身長, 初出勤, 最終出勤） |
| `daily_schedules` | 日別シフト（誰が/いつ/どこで/何時〜何時/予約満了か） |
| `availability_snapshots` | 定期監視スナップショット（予約可否の時系列変化） |
| `scrape_logs` | スクレイプ実行ログ（成否・取得件数・エラー） |

### 設計ポイント

- 深夜帯は `25:00`〜`29:00` に正規化 → 日付跨ぎなし
- `(therapist_id, schedule_date)` UNIQUE → 冪等な UPSERT
- `availability_snapshots` は追記型 → 「何時に予約が埋まったか」を遡れる
- WALモード → GitHub Actions の並行実行に対応

## ダッシュボード機能

| タブ | 内容 |
|---|---|
| **Today** | 当日の出勤サマリ、店舗別スタッフリスト、Ganttタイムライン |
| **Weekly** | 7日分の積み上げ棒グラフ、ヒートマップ、昼夜シフトバランス |
| **Staff Stats** | 出勤頻度ランキング、平均勤務時間、配属店舗一覧 |
| **Monitor Log** | 空き状況スナップショット履歴、スクレイプ実行ログ |

## ローカル開発

```bash
pip install -r requirements.txt
python scripts/db_setup.py
python build_dashboard.py          # 週次収集 + ダッシュボード
python build_dashboard.py --today  # 当日モニターのみ
open dashboards/dashboard.html
```
