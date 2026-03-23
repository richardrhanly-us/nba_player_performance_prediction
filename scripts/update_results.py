import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.shared_app import update_all_pending_sheet_results


def log(msg):
    print(msg, flush=True)


if __name__ == "__main__":
    start_time = time.time()
    log("[UPDATE RESULTS] ===== START WORKFLOW =====")

    result = update_all_pending_sheet_results(debug=False)

    if isinstance(result, tuple):
        if len(result) == 3:
            updated_count, checked_count, debug_info = result
            log(f"[UPDATE RESULTS] Checked {checked_count} rows, updated {updated_count} rows.")
            log(f"[UPDATE RESULTS] Debug: {debug_info}")
        elif len(result) == 2:
            updated_count, checked_count = result
            log(f"[UPDATE RESULTS] Checked {checked_count} rows, updated {updated_count} rows.")
        else:
            log(f"[UPDATE RESULTS] Unexpected result: {result}")
    else:
        log(f"[UPDATE RESULTS] Result: {result}")

    log(f"[UPDATE RESULTS] Runtime: {round(time.time() - start_time, 2)} seconds")
    log("[UPDATE RESULTS] ===== END WORKFLOW =====")
