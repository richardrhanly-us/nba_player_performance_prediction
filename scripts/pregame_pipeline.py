import sys
import os
import time
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.shared_app import (
    BOOKMAKER_KEY,
    SHEET_KEY,
    normalize_name,
    fetch_all_today_player_props,
    get_gsheet_client,
    get_results_sheet,
    get_strong_plays_df,
    get_top_plays_today_df,
    format_event_game_date,
)
from src.write_ops import append_manual_play_to_sheet1, append_play_to_strong_plays


HISTORICAL_LINES_SHEET_NAME = "Historical Lines"
TOP_PLAYS_LIVE_SHEET_NAME = "Top Plays Live"


def log(msg):
    print(msg, flush=True)


def get_worksheet(sheet_name):
    client = get_gsheet_client()
    return client.open_by_key(SHEET_KEY).worksheet(sheet_name)


def get_top_plays_live_sheet():
    return get_worksheet(TOP_PLAYS_LIVE_SHEET_NAME)


def get_historical_lines_sheet():
    return get_worksheet(HISTORICAL_LINES_SHEET_NAME)


def get_sheet1_df():
    try:
        sheet = get_results_sheet()
        values = sheet.get_all_values()

        if not values or len(values) < 2:
            return pd.DataFrame()

        headers = [str(h).strip() for h in values[0]]
        rows = values[1:]
        return pd.DataFrame(rows, columns=headers)
    except Exception as e:
        log(f"[PREGAME] get_sheet1_df failed: {e}")
        return pd.DataFrame()


def update_top_plays_live_sheet(df):
    sheet = get_top_plays_live_sheet()

    if df is None or df.empty:
        log("[PREGAME] No data available -> writing placeholder to Top Plays Live")
        sheet.clear()
        sheet.update(range_name="A1", values=[["No data available"]])
        return 0

    log(f"[PREGAME] Writing {len(df)} rows to Top Plays Live")
    sheet.clear()
    sheet.update(
        range_name="A1",
        values=[df.columns.values.tolist()] + df.values.tolist()
    )
    return len(df)


def already_logged_strong_play(records_df, player_name, game_date, sportsbook, line):
    if records_df is None or records_df.empty:
        return False

    target_player = normalize_name(player_name)
    target_date = str(game_date).strip()
    target_book = str(sportsbook).strip().lower()

    try:
        target_line = float(line)
    except Exception:
        target_line = None

    for _, row in records_df.iterrows():
        row_player = normalize_name(row.get("PLAYER_NAME", ""))
        row_date = str(row.get("GAME_DATE", "")).strip()
        row_book = str(row.get("sportsbook", "")).strip().lower()

        try:
            row_line = float(row.get("sportsbook_line", ""))
        except Exception:
            row_line = None

        if (
            row_player == target_player
            and row_date == target_date
            and row_book == target_book
            and row_line == target_line
        ):
            return True

    return False


def append_historical_lines(scan_df, sportsbook_key):
    historical_ws = get_historical_lines_sheet()
    existing_values = historical_ws.get_all_values()

    existing_keys = set()
    if len(existing_values) > 1:
        for row in existing_values[1:]:
            if len(row) >= 4:
                existing_player = normalize_name(str(row[0]).strip())
                existing_date = str(row[1]).strip()
                existing_line = str(row[2]).strip()
                existing_book = str(row[3]).strip().lower()

                if existing_player and existing_date and existing_line and existing_book:
                    existing_keys.add((
                        existing_player,
                        existing_date,
                        existing_line,
                        existing_book
                    ))

    today_date = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d")
    captured_at = pd.Timestamp.now(tz="America/Chicago").strftime("%Y-%m-%d %H:%M:%S")

    rows_to_append = []

    for _, row in scan_df.iterrows():
        player_name = normalize_name(str(row["player_name_raw"]).strip())
        sportsbook_line = str(row["line"]).strip()
        sportsbook_name = str(sportsbook_key).strip().lower()

        history_key = (
            player_name,
            today_date,
            sportsbook_line,
            sportsbook_name
        )

        if history_key not in existing_keys:
            rows_to_append.append([
                player_name,
                today_date,
                sportsbook_line,
                sportsbook_name,
                captured_at
            ])
            existing_keys.add(history_key)

    if rows_to_append:
        historical_ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        log(f"[PREGAME] Historical Lines appended: {len(rows_to_append)}")
    else:
        log("[PREGAME] Historical Lines already up to date")

    return len(rows_to_append)


def build_scan_df(api_key, sportsbook_key):
    props_df = fetch_all_today_player_props(api_key, sportsbook_key)

    if props_df is None or props_df.empty:
        return pd.DataFrame()

    scan_df = props_df.copy()
    scan_df = scan_df.dropna(subset=["player_name_raw", "line"]).copy()
    scan_df["player_name_raw"] = scan_df["player_name_raw"].astype(str).str.strip()

    sort_cols = ["player_name_raw"]
    ascending = [True]

    if "last_update" in scan_df.columns:
        sort_cols.append("last_update")
        ascending.append(False)

    scan_df = scan_df.sort_values(by=sort_cols, ascending=ascending)
    scan_df = scan_df.drop_duplicates(subset=["player_name_raw"], keep="first").reset_index(drop=True)

    return scan_df


def append_new_sheet1_rows(scan_df, sportsbook_key):
    if scan_df is None or scan_df.empty:
        log("[PREGAME] No scan rows available for Sheet1 append")
        return {"loaded": 0, "failed": 0, "skipped_existing": 0}

    sheet1_df = get_sheet1_df()

    existing_sheet_keys = set()
    if not sheet1_df.empty:
        working_sheet1 = sheet1_df.copy()

        for col in ["PLAYER_NAME", "sportsbook", "sportsbook_line"]:
            if col not in working_sheet1.columns:
                working_sheet1[col] = ""

        for _, existing_row in working_sheet1.iterrows():
            existing_player = normalize_name(str(existing_row.get("PLAYER_NAME", "")).strip())
            existing_book = str(existing_row.get("sportsbook", "")).strip().lower()
            existing_line = str(existing_row.get("sportsbook_line", "")).strip()

            if existing_player and existing_book and existing_line:
                try:
                    existing_sheet_keys.add((
                        existing_player,
                        existing_book,
                        float(existing_line)
                    ))
                except Exception:
                    pass

    loaded_count = 0
    failed_count = 0
    skipped_existing = 0

    total_rows = len(scan_df)

    for i, (_, row) in enumerate(scan_df.iterrows(), start=1):
        player_name = str(row["player_name_raw"]).strip()
        sportsbook_line = float(row["line"])
        last_update = str(row.get("last_update", "") or "")
        dedupe_key = (normalize_name(player_name), str(sportsbook_key).lower(), sportsbook_line)

        if dedupe_key in existing_sheet_keys:
            skipped_existing += 1
            continue

        log(
            f"[PREGAME] Sheet1 append {i}/{total_rows} -> "
            f"{player_name} | {sportsbook_key} | {sportsbook_line}"
        )

        success = False
        last_error = None

        for attempt in range(2):
            try:
                append_manual_play_to_sheet1(
                    player_name=player_name,
                    sportsbook_key=sportsbook_key,
                    sportsbook_line=sportsbook_line,
                    last_update=last_update,
                )
                success = True
                loaded_count += 1
                existing_sheet_keys.add(dedupe_key)
                break
            except Exception as e:
                last_error = e
                if attempt == 0:
                    time.sleep(1.0)

        if not success:
            failed_count += 1
            log(f"[PREGAME] Sheet1 append failed -> {player_name} | {sportsbook_line} | {last_error}")

        time.sleep(0.5)

    return {
        "loaded": loaded_count,
        "failed": failed_count,
        "skipped_existing": skipped_existing,
    }


def rebuild_top_plays_and_strong_plays(api_key):
    log("[PREGAME] Rebuilding Top Plays Live...")
    top_df = get_top_plays_today_df(api_key=api_key, debug=False)

    if top_df is None or top_df.empty:
        rows_written = update_top_plays_live_sheet(top_df)
        return {
            "top_rows_written": rows_written,
            "strong_appended": 0,
        }

    rows_written = update_top_plays_live_sheet(top_df)

    records_df = get_strong_plays_df()
    appended_count = 0
    total_rows = len(top_df)

    for i, (_, row) in enumerate(top_df.iterrows(), start=1):
        player_name = row.get("PLAYER_NAME", "")
        sportsbook = str(row.get("sportsbook", "")).lower()
        line = row.get("sportsbook_line", "")
        predicted_points = row.get("predicted_points", "")
        model_pick = row.get("model_pick", "")
        last_update = row.get("last_update", "")
        edge = row.get("edge", 0)

        game_date = row.get("GAME_DATE", "")
        if not game_date:
            game_date = format_event_game_date(row.get("commence_time", ""))

        log(f"[PREGAME] Strong Plays {i}/{total_rows} -> {player_name} | {sportsbook} | {line}")

        if already_logged_strong_play(records_df, player_name, game_date, sportsbook, line):
            log(f"[PREGAME] Strong Plays skip existing -> {player_name} | {sportsbook} | {line}")
            continue

        append_play_to_strong_plays(
            player_name=player_name,
            game_date=game_date,
            sportsbook_line=line,
            sportsbook=sportsbook,
            predicted_points=predicted_points,
            model_pick=model_pick,
            last_update=last_update,
            edge=edge,
        )

        appended_count += 1
        time.sleep(0.5)

    return {
        "top_rows_written": rows_written,
        "strong_appended": appended_count,
    }


def main():
    start_time = time.time()
    api_key = os.environ["ODDS_API_KEY"]
    sportsbook_key = os.environ.get("PREGAME_BOOKMAKER_KEY", BOOKMAKER_KEY).lower()

    log("[PREGAME] ===== START WORKFLOW =====")
    log(f"[PREGAME] Bookmaker: {sportsbook_key}")

    scan_df = build_scan_df(api_key, sportsbook_key)
    log(f"[PREGAME] Scan rows found: {len(scan_df)}")

    historical_added = append_historical_lines(scan_df, sportsbook_key)
    sheet1_summary = append_new_sheet1_rows(scan_df, sportsbook_key)
    top_summary = rebuild_top_plays_and_strong_plays(api_key)

    log(
        "[PREGAME] DONE | "
        f"historical_added={historical_added} | "
        f"sheet1_loaded={sheet1_summary['loaded']} | "
        f"sheet1_failed={sheet1_summary['failed']} | "
        f"sheet1_skipped_existing={sheet1_summary['skipped_existing']} | "
        f"top_rows_written={top_summary['top_rows_written']} | "
        f"strong_appended={top_summary['strong_appended']}"
    )
    log(f"[PREGAME] Runtime: {round(time.time() - start_time, 2)} seconds")
    log("[PREGAME] ===== END WORKFLOW =====")


if __name__ == "__main__":
    main()
