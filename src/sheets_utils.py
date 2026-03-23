import os
import json
import pandas as pd
import gspread
import streamlit as st

from google.oauth2.service_account import Credentials


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_KEY = "1uhjV_Si-qcILfNJbKZrD52y4JnT_GvqQ0hzN7POekQM"

RESULTS_SHEET_NAME = "Sheet1"
STRONG_PLAYS_SHEET_NAME = "Strong Plays"
HISTORICAL_LINES_SHEET_NAME = "Historical Lines"


def clear_app_caches():
    try:
        st.cache_data.clear()
    except Exception:
        pass

    try:
        st.cache_resource.clear()
    except Exception:
        pass


@st.cache_resource
def get_gsheet_client():
    service_account_info = None

    try:
        if "GCP_SERVICE_ACCOUNT" in st.secrets:
            service_account_info = dict(st.secrets["GCP_SERVICE_ACCOUNT"])
        elif "gcp_service_account" in st.secrets:
            service_account_info = dict(st.secrets["gcp_service_account"])
    except Exception:
        pass

    if not service_account_info:
        env_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        if env_json:
            service_account_info = json.loads(env_json)

    if not service_account_info:
        raise ValueError(
            "Google Sheets credentials not found. "
            "Set GCP_SERVICE_ACCOUNT or gcp_service_account in Streamlit secrets, "
            "or GCP_SERVICE_ACCOUNT_JSON in environment variables."
        )

    creds = Credentials.from_service_account_info(
        service_account_info,
        scopes=SCOPES
    )
    return gspread.authorize(creds)


@st.cache_resource
def get_worksheet(sheet_name):
    client = get_gsheet_client()
    return client.open_by_key(SHEET_KEY).worksheet(sheet_name)


@st.cache_resource
def get_historical_lines_sheet():
    return get_worksheet(HISTORICAL_LINES_SHEET_NAME)


@st.cache_resource
def get_strong_plays_sheet():
    return get_worksheet(STRONG_PLAYS_SHEET_NAME)


@st.cache_resource
def get_results_sheet():
    return get_worksheet(RESULTS_SHEET_NAME)


def get_worksheet_with_df(sheet_name):
    ws = get_worksheet(sheet_name)
    values = ws.get_all_values()

    if not values:
        return ws, pd.DataFrame(), []

    headers = [str(h).strip() for h in values[0]]
    rows = values[1:]

    if not rows:
        return ws, pd.DataFrame(columns=headers), headers

    df = pd.DataFrame(rows, columns=headers)
    return ws, df, headers


def column_letter_from_index(index_1_based):
    result = ""
    while index_1_based > 0:
        index_1_based, remainder = divmod(index_1_based - 1, 26)
        result = chr(65 + remainder) + result
    return result


def build_header_index_map(headers):
    return {str(col).strip(): i + 1 for i, col in enumerate(headers)}
