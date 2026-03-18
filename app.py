import streamlit as st
import pandas as pd
import joblib
import json
from scipy.stats import norm
from datetime import datetime
import pytz

from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog, commonplayerinfo, scoreboardv2


# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="NBA Points Prop Predictor",
    page_icon="🏀",
    layout="centered"
)

# -----------------------------
# Custom CSS
# -----------------------------
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(180deg, #0b1220 0%, #111827 100%);
        color: #f3f4f6;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
        max-width: 900px;
    }

    h1, h2, h3 {
        color: #f9fafb;
        letter-spacing: 0.2px;
    }

    .hero {
        background: linear-gradient(135deg, #111827 0%, #1f2937 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 20px;
        padding: 24px 24px 20px 24px;
        margin-bottom: 20px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.25);
    }

    .hero-title {
        font-size: 2rem;
        font-weight: 800;
        margin-bottom: 4px;
    }

    .hero-subtitle {
        color: #9ca3af;
        font-size: 0.95rem;
        margin-bottom: 0;
    }

    .section-card {
        background: rgba(17, 24, 39, 0.92);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 20px;
        margin-top: 18px;
        box-shadow: 0 8px 24px rgba(0,0,0,0.20);
    }

    .section-title {
        font-size: 1.2rem;
        font-weight: 700;
        margin-bottom: 14px;
        color: #f9fafb;
    }

    .stat-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 12px;
        margin-top: 10px;
    }

    .stat-box {
        background: #0f172a;
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 14px;
        padding: 14px;
    }

    .stat-label {
        color: #9ca3af;
        font-size: 0.82rem;
        margin-bottom: 6px;
    }

    .stat-value {
        color: #f9fafb;
        font-size: 1.15rem;
        font-weight: 700;
    }

    .pick-banner {
        margin-top: 18px;
        border-radius: 16px;
        padding: 16px 18px;
        font-size: 1.05rem;
        font-weight: 700;
        text-align: center;
    }

    .pick-over {
        background: rgba(16, 185, 129, 0.15);
        color: #34d399;
        border: 1px solid rgba(52, 211, 153, 0.25);
    }

    .pick-under {
        background: rgba(245, 158, 11, 0.14);
        color: #fbbf24;
        border: 1px solid rgba(251, 191, 36, 0.22);
    }

    .small-note {
        color: #9ca3af;
        font-size: 0.85rem;
        margin-top: 8px;
    }

    .divider {
        height: 1px;
        background: rgba(255,255,255,0.08);
        margin: 14px 0 8px 0;
    }

    /* Inputs */
    .stSelectbox label, .stNumberInput label {
        color: #d1d5db !important;
        font-weight: 600;
    }

    div[data-baseweb="select"] > div {
        background-color: #111827 !important;
        border: 1px solid rgba(255,255,255,0.10) !important;
        border-radius: 14px !important;
        color: white !important;
    }

    .stNumberInput > div > div > input {
        background-color: #111827 !important;
        color: white !important;
        border-radius: 14px !important;
    }

    .stNumberInput button {
        background-color: #1f2937 !important;
        color: white !important;
        border: none !important;
    }

    .metric-pill {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 999px;
        font-size: 0.82rem;
        font-weight: 700;
        margin-left: 6px;
    }

    .green-pill {
        background: rgba(16, 185, 129, 0.15);
        color: #34d399;
    }

    .red-pill {
        background: rgba(239, 68, 68, 0.16);
        color: #f87171;
    }

    .yellow-pill {
        background: rgba(245, 158, 11, 0.16);
        color: #fbbf24;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------
# Helpers
# -----------------------------
def get_pick_label(edge_value):
    if edge_value > 0:
        return "Lean Over", "pick-over"
    else:
        return "Lean Under", "pick-under"


def format_edge(edge_value):
    sign = "+" if edge_value > 0 else ""
    return f"{sign}{edge_value:.2f}"


def safe_float(value, default=0.0):
    try:
        return float(value)
    except:
        return default


# -----------------------------
# Load model + stats
# -----------------------------
model = joblib.load("models/points_regression.pkl")

with open("models/points_model_stats.json", "r") as f:
    model_stats = json.load(f)

points_std = model_stats["std_dev"]

# -----------------------------
# Player list
# -----------------------------
all_players = players.get_players()
player_name_map = {p["full_name"]: p["id"] for p in all_players}
player_names = sorted(player_name_map.keys())

# -----------------------------
# Header
# -----------------------------
st.markdown("""
<div class="hero">
    <div class="hero-title">NBA Points Prop Predictor</div>
    <p class="hero-subtitle">Search a player, set the line, and get a quick model-based lean.</p>
</div>
""", unsafe_allow_html=True)

# -----------------------------
# Inputs
# -----------------------------
st.caption("Search for a player by name")

selected_player = st.selectbox(
    "Player",
    options=player_names,
    index=None,
    placeholder="Start typing a player name..."
)

line = st.number_input(
    "Enter points line",
    min_value=0.0,
    max_value=100.0,
    value=20.5,
    step=0.5
)

# -----------------------------
# Main logic
# -----------------------------
if selected_player:
    player_id = player_name_map[selected_player]

    try:
        # ---------------------------------
        # Pull recent game log
        # ---------------------------------
        gamelog = playergamelog.PlayerGameLog(player_id=player_id, season="2025-26")
        df = gamelog.get_data_frames()[0]

        if df.empty:
            st.warning("No recent game log found for this player.")
            st.stop()

        # Example feature setup
        recent_games = df.head(10).copy()
        recent_games["PTS"] = pd.to_numeric(recent_games["PTS"], errors="coerce")
        recent_games["MIN"] = pd.to_numeric(recent_games["MIN"], errors="coerce")

        avg_pts_last_10 = recent_games["PTS"].mean()
        avg_min_last_10 = recent_games["MIN"].mean()

        latest_game = df.iloc[0]
        matchup = latest_game.get("MATCHUP", "N/A")

        # ---------------------------------
        # Very basic example feature frame
        # Replace/add your real model inputs here
        # ---------------------------------
        feature_data = pd.DataFrame([{
            "avg_pts_last_10": safe_float(avg_pts_last_10),
            "avg_min_last_10": safe_float(avg_min_last_10),
        }])

        predicted_points = float(model.predict(feature_data)[0])

        edge = predicted_points - line
        prob_over = 1 - norm.cdf(line, loc=predicted_points, scale=points_std)
        prob_under = 1 - prob_over

        pick_text, pick_class = get_pick_label(edge)

        # ---------------------------------
        # Next game info
        # ---------------------------------
        next_game_status = "No game today"
        next_matchup = "N/A"
        next_date = "N/A"
        next_time = "N/A"

        try:
            eastern = pytz.timezone("US/Eastern")
            today = datetime.now(eastern).strftime("%Y-%m-%d")

            scoreboard = scoreboardv2.ScoreboardV2(game_date=today)
            games = scoreboard.game_header.get_data_frame()
            line_scores = scoreboard.line_score.get_data_frame()

            team_abbr = None
            if " vs. " in matchup or " @ " in matchup:
                team_abbr = matchup[:3]

            if team_abbr is not None and not line_scores.empty and not games.empty:
                matching_rows = line_scores[line_scores["TEAM_ABBREVIATION"] == team_abbr]
                if not matching_rows.empty:
                    game_id = matching_rows.iloc[0]["GAME_ID"]
                    game_row = games[games["GAME_ID"] == game_id]

                    if not game_row.empty:
                        game_row = game_row.iloc[0]
                        game_status_text = game_row.get("GAME_STATUS_TEXT", "Scheduled")
                        next_game_status = game_status_text

                        home_team = line_scores[
                            (line_scores["GAME_ID"] == game_id) &
                            (line_scores["TEAM_CITY_NAME"].notna())
                        ]

                        game_date_est = game_row.get("GAME_DATE_EST", None)

                        if game_date_est:
                            try:
                                dt = pd.to_datetime(game_date_est)
                                next_date = dt.strftime("%B %d, %Y")
                                next_time = dt.strftime("%I:%M %p ET").lstrip("0")
                            except:
                                pass

                        teams_in_game = line_scores[line_scores["GAME_ID"] == game_id]["TEAM_ABBREVIATION"].tolist()
                        if len(teams_in_game) >= 2:
                            next_matchup = f"{teams_in_game[0]} vs {teams_in_game[1]}"

        except:
            pass

        # -----------------------------
        # Game Info Card
        # -----------------------------
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Game Info</div>', unsafe_allow_html=True)

        st.markdown(f"""
        <div class="stat-grid">
            <div class="stat-box">
                <div class="stat-label">Status</div>
                <div class="stat-value">{next_game_status}</div>
            </div>
            <div class="stat-box">
                <div class="stat-label">Matchup</div>
                <div class="stat-value">{next_matchup}</div>
            </div>
            <div class="stat-box">
                <div class="stat-label">Date</div>
                <div class="stat-value">{next_date}</div>
            </div>
            <div class="stat-box">
                <div class="stat-label">Time</div>
                <div class="stat-value">{next_time}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

        # -----------------------------
        # Prediction Card
        # -----------------------------
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Prediction</div>', unsafe_allow_html=True)

        edge_class = "green-pill" if edge > 0 else "red-pill"

        st.markdown(f"""
        <div class="stat-grid">
            <div class="stat-box">
                <div class="stat-label">Predicted Points</div>
                <div class="stat-value">{predicted_points:.2f}</div>
            </div>
            <div class="stat-box">
                <div class="stat-label">Line</div>
                <div class="stat-value">{line:.1f}</div>
            </div>
            <div class="stat-box">
                <div class="stat-label">Edge</div>
                <div class="stat-value">{format_edge(edge)}</div>
            </div>
            <div class="stat-box">
                <div class="stat-label">Confidence Split</div>
                <div class="stat-value">O {prob_over*100:.1f}% / U {prob_under*100:.1f}%</div>
            </div>
        </div>

        <div class="pick-banner {pick_class}">
            {pick_text}
        </div>
        """, unsafe_allow_html=True)

        st.markdown(
            '<div class="small-note">This is a model lean, not guaranteed betting advice.</div>',
            unsafe_allow_html=True
        )

        st.markdown('</div>', unsafe_allow_html=True)

        # -----------------------------
        # Optional recent form card
        # -----------------------------
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Recent Form</div>', unsafe_allow_html=True)

        st.markdown(f"""
        <div class="stat-grid">
            <div class="stat-box">
                <div class="stat-label">Avg Points (Last 10)</div>
                <div class="stat-value">{avg_pts_last_10:.2f}</div>
            </div>
            <div class="stat-box">
                <div class="stat-label">Avg Minutes (Last 10)</div>
                <div class="stat-value">{avg_min_last_10:.2f}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.dataframe(
            recent_games[["GAME_DATE", "MATCHUP", "PTS", "MIN"]].head(5),
            use_container_width=True,
            hide_index=True
        )

        st.markdown('</div>', unsafe_allow_html=True)

    except Exception as e:
        st.error(f"Something went wrong: {e}")
else:
    st.markdown("""
    <div class="section-card">
        <div class="section-title">Get Started</div>
        <div class="small-note">
            Select a player above to load game info and generate a prediction.
        </div>
    </div>
    """, unsafe_allow_html=True)
