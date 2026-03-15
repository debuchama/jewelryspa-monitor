#!/usr/bin/env python3
"""
ダッシュボード一括ビルドスクリプト

Usage:
  python build_dashboard.py          # 週次収集 + ダッシュボード生成
  python build_dashboard.py --today   # 当日モニター + ダッシュボード再生成
"""

import json, os, sys, argparse

SCRIPTS = os.path.join(os.path.dirname(__file__), "scripts")
DASHBOARDS = os.path.join(os.path.dirname(__file__), "dashboards")
DATA = os.path.join(os.path.dirname(__file__), "data")

sys.path.insert(0, SCRIPTS)

def build():
    parser = argparse.ArgumentParser()
    parser.add_argument("--today", action="store_true", help="Run daily monitor instead of weekly")
    args = parser.parse_args()

    # 1) Scrape
    if args.today:
        from daily_monitor import run
        run(loop_minutes=0)
    else:
        from weekly_collector import run
        run()

    # 2) Export JSON
    from export_data import export_dashboard_data
    data = export_dashboard_data()

    # 3) Build HTML
    template_path = os.path.join(DASHBOARDS, "dashboard_template.html")
    output_path = os.path.join(DASHBOARDS, "dashboard.html")

    with open(template_path, encoding="utf-8") as f:
        html = f.read()

    json_str = json.dumps(data, ensure_ascii=False)
    html = html.replace("DASHBOARD_DATA_PLACEHOLDER", json_str)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n🎨 Dashboard built: {output_path}")
    print(f"   Open in browser to view.")

if __name__ == "__main__":
    build()
