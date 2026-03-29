import os
import sys
import time
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.shared_app import (
    get_top_plays_today_df,
    get_gsheet_client,
    SHEET_KEY,
    CURRENT_SEASON
)

from src.write_ops import append_manual_play_to_sheet1


def run_pregame_pipeline():
    print("[PREGAME] Starting pipeline...", flush=True)

    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        raise ValueError("ODDS_API_KEY not found")

    # =========================
    # STEP 1 — BUILD TOP PLAYS
    # =========================
    print("[PREGAME] Building top plays...", flush=True)
    top_df = get_top_plays_today_df(api_key, debug=False)

    if top_df is None or top_df.empty:
        print("[PREGAME] No top plays returned.", flush=True)
        return

    # =========================
    # STEP 2 — LOAD TO SHEET1
    # =========================
    print(f"[PREGAME] Loading {len(top_df)} players into Sheet1...", flush=True)

    for i, (_, row) in enumerate(top_df.iterrows(), start=1):
        player_name = row.get("PLAYER_NAME")
        sportsbook = row.get("sportsbook", "draftkings")
        line = row.get("sportsbook_line")

        print(f"[PREGAME] Sheet1 append {i}/{len(top_df)} -> {player_name} | {sportsbook} | {line}", flush=True)

        try:
            append_manual_play_to_sheet1(
                player_name=player_name,
                sportsbook_key=sportsbook,
                sportsbook_line=line
            )
        except Exception as e:
            print(f"[PREGAME] Sheet1 append failed -> {player_name} | {line} | {e}", flush=True)

        # THROTTLE 
        time.sleep(0.6)

    # =========================
    # STEP 3 — WRITE TOP PLAYS LIVE
    # =========================
    print("[PREGAME] Writing Top Plays Live...", flush=True)

    client = get_gsheet_client()
    sheet = client.open_by_key(SHEET_KEY)

    try:
        top_sheet = sheet.worksheet("Top Plays Live")
    except Exception:
        top_sheet = sheet.add_worksheet(title="Top Plays Live", rows=1000, cols=20)

    top_sheet.clear()

    if top_df.empty:
        top_sheet.update("A1", [["No data available"]])
    else:
        top_sheet.update([top_df.columns.values.tolist()] + top_df.values.tolist())

    print("[PREGAME] Top Plays Live updated.", flush=True)

    print("[PREGAME] Pipeline complete.", flush=True)


if __name__ == "__main__":
    run_pregame_pipeline()
