"""
JST タイムゾーンヘルパー

GitHub Actions（UTC環境）でも常にJSTを返す。
全スクリプトはこのモジュール経由で現在時刻を取得すること。
"""

from datetime import datetime, timedelta, timezone

JST = timezone(timedelta(hours=9))


def now_jst() -> datetime:
    """現在のJST日時を返す"""
    return datetime.now(JST)


def now_str() -> str:
    """'YYYY-MM-DD HH:MM:SS' 形式のJST日時文字列"""
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")


def today_str() -> str:
    """'YYYY-MM-DD' 形式のJST今日の日付"""
    return now_jst().strftime("%Y-%m-%d")


def jst_sql() -> str:
    """SQLite用のJST日時文字列（INSERT文のVALUES内で使う）"""
    return now_str()
