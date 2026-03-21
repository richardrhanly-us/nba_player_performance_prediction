import sys
import os
import pandas as pd
import streamlit as st

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.shared_app import (
    APP_VERSION,
    CURRENT_SEASON,
    normalize_name,
    load_model,
    load_model_stats,
    load_active_players,
    get_strong_plays_summary,
    get_strong_plays_health,
    get_player_gamelog_df,
    get_player_info_df,
    build_player_feature_row,
    get_live_player_stats,
    get_team_game_info,
    get_top_plays_today_df,
)



st.set_page_config(
    page_title="NBA Points Prop Predictor",
    page_icon="🏀",
    layout="centered"
)


st.markdown("""
<style>


    .model-card {
        border-radius: 18px;
        padding: 18px 18px 14px 18px;
        margin-top: 4px;
        margin-bottom: 16px;
    }

    .model-title {
        font-size: 1.15rem;
        font-weight: 900;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        margin-bottom: 14px;
        color: white;
        text-shadow: 0 0 6px rgba(255,255,255,0.25);
    }

    .model-subtitle {
        font-size: 0.75rem;
        letter-spacing: 1px;
        text-transform: uppercase;
        opacity: 0.7;
        margin-bottom: 10px;
        color: #e2e8f0;
    }

    .model-main {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
        gap: 14px;
    }

    .model-stat {
        border-radius: 14px;
        padding: 14px 16px;
    }

    .model-stat-label {
        font-size: 0.75rem;
        margin-bottom: 6px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }

    .model-stat-value {
        color: #ffffff;
        font-size: 1.15rem;
        font-weight: 900;
    }

    .pick-banner {
        margin-top: 16px;
        border-radius: 14px;
        padding: 14px 16px;
        font-size: 1.05rem;
        font-weight: 900;
        text-align: center;
        letter-spacing: 0.05em;
        width: 100%;
        display: block;
        box-sizing: border-box;
    }

    .small-note {
        color: #94a3b8;
        font-size: 0.84rem;
        margin-top: 10px;
    }
    
    
    .stApp {
        background: linear-gradient(180deg, #081120 0%, #0f172a 100%);
        color: #f8fafc;
    }

    .block-container {
        padding-top: 1.1rem;
        padding-bottom: 3rem;
        max-width: 980px;
    }

    hr, div[data-testid="stDivider"] {
        display: none !important;
    }

    .hero {
        background: linear-gradient(135deg, #111827 0%, #1e293b 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 1.15rem 1.15rem 1rem 1.15rem;
        margin-bottom: 1rem;
        box-shadow: 0 8px 30px rgba(0,0,0,0.25);
    }

    .hero-title {
        font-size: 1.7rem;
        font-weight: 800;
        color: #f8fafc;
        margin-bottom: 0.2rem;
    }

    .hero-sub {
        color: #94a3b8;
        font-size: 0.95rem;
        margin-bottom: 0.15rem;
    }

    .section-card {
        background: rgba(17,24,39,0.78);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 16px;
        padding: 1rem 1rem 0.85rem 1rem;
        margin-bottom: 1rem;
        box-shadow: 0 8px 24px rgba(0,0,0,0.20);
    }

    .section-title {
        color: #f8fafc;
        font-size: 1.08rem;
        font-weight: 800;
        margin-bottom: 0.8rem;
    }

    .metric-box {
        background: rgba(15,23,42,0.78);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 12px;
        padding: 12px 14px;
        margin-bottom: 12px;
    }

    .metric-label {
        color: #94a3b8;
        font-size: 0.72rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        margin-bottom: 4px;
    }

    .metric-value {
        color: #f8fafc;
        font-size: 1.05rem;
        font-weight: 800;
    }

    .mini-card {
        background: rgba(15,23,42,0.72);
        border: 1px solid rgba(255,255,255,0.05);
        border-radius: 14px;
        padding: 14px;
        margin-bottom: 10px;
    }

    .mini-title {
        color: #cbd5e1;
        font-size: 0.84rem;
        margin-bottom: 6px;
    }

    .mini-value {
        color: #f8fafc;
        font-size: 1.2rem;
        font-weight: 800;
    }

    .muted {
        color: #94a3b8;
    }
</style>
""", unsafe_allow_html=True)


def get_player_name_map():
    active_players = load_active_players()
    if isinstance(active_players, tuple):
        actual_name_to_id, _ = active_players
    else:
        actual_name_to_id = active_players
    return actual_name_to_id


def get_player_lookup():
    actual_name_to_id = get_player_name_map()
    player_names = sorted(actual_name_to_id.keys())
    return actual_name_to_id, player_names


def format_health_last_update(value):
    if value is None:
        return "N/A"
    try:
        return pd.to_datetime(value).strftime("%b %d, %I:%M %p")
    except Exception:
        return str(value)


def safe_live_display(value, fallback="N/A"):
    if value is None:
        return fallback
    if isinstance(value, str) and not value.strip():
        return fallback
    return str(value)


def hex_to_rgba(hex_color: str, alpha: float) -> str:
    hex_color = hex_color.lstrip("#")
    if len(hex_color) != 6:
        return f"rgba(56,189,248,{alpha})"
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    return f"rgba({r}, {g}, {b}, {alpha})"


TEAM_THEMES = {
    "ATL": {"primary": "#E03A3E", "secondary": "#C1D32F"},
    "BOS": {"primary": "#007A33", "secondary": "#BA9653"},
    "BKN": {"primary": "#000000", "secondary": "#FFFFFF"},
    "CHA": {"primary": "#1D1160", "secondary": "#00788C"},
    "CHI": {"primary": "#CE1141", "secondary": "#000000"},
    "CLE": {"primary": "#860038", "secondary": "#FDBB30"},
    "DAL": {"primary": "#00538C", "secondary": "#B8C4CA"},
    "DEN": {"primary": "#0E2240", "secondary": "#FEC524"},
    "DET": {"primary": "#C8102E", "secondary": "#1D42BA"},
    "GSW": {"primary": "#1D428A", "secondary": "#FFC72C"},
    "HOU": {"primary": "#CE1141", "secondary": "#C4CED4"},
    "IND": {"primary": "#002D62", "secondary": "#FDBB30"},
    "LAC": {"primary": "#C8102E", "secondary": "#1D428A"},
    "LAL": {"primary": "#552583", "secondary": "#FDB927"},
    "MEM": {"primary": "#5D76A9", "secondary": "#12173F"},
    "MIA": {"primary": "#98002E", "secondary": "#F9A01B"},
    "MIL": {"primary": "#00471B", "secondary": "#EEE1C6"},
    "MIN": {"primary": "#0C2340", "secondary": "#236192"},
    "NOP": {"primary": "#0C2340", "secondary": "#C8102E"},
    "NYK": {"primary": "#006BB6", "secondary": "#F58426"},
    "OKC": {"primary": "#007AC1", "secondary": "#EF3B24"},
    "ORL": {"primary": "#0077C0", "secondary": "#C4CED4"},
    "PHI": {"primary": "#006BB6", "secondary": "#ED174C"},
    "PHX": {"primary": "#1D1160", "secondary": "#E56020"},
    "POR": {"primary": "#E03A3E", "secondary": "#000000"},
    "SAC": {"primary": "#5A2D81", "secondary": "#63727A"},
    "SAS": {"primary": "#000000", "secondary": "#C4CED4"},
    "TOR": {"primary": "#CE1141", "secondary": "#000000"},
    "UTA": {"primary": "#002B5C", "secondary": "#F9A01B"},
    "WAS": {"primary": "#002B5C", "secondary": "#E31837"}
}


def get_team_theme(team_abbr: str):
    return TEAM_THEMES.get(team_abbr, {"primary": "#38bdf8", "secondary": "#60a5fa"})


def get_pick_label(edge):
    abs_edge = abs(edge)
    if abs_edge < 1.5:
        return "No Bet", "neutral"
    if abs_edge < 3.0:
        return ("Lean Over", "over") if edge > 0 else ("Lean Under", "under")
    return ("Strong Over", "over") if edge > 0 else ("Strong Under", "under")

@st.cache_data(ttl=120)
def build_prediction(player_name, sportsbook_line):
    model = load_model()
    model_stats = load_model_stats()
    active_players = load_active_players()

    if isinstance(active_players, tuple):
        actual_name_to_id, normalized_to_actual = active_players
    else:
        actual_name_to_id = active_players
        normalized_to_actual = {normalize_name(k): k for k in actual_name_to_id.keys()}

    normalized = normalize_name(player_name)
    actual_name = normalized_to_actual.get(normalized, player_name)
    player_id = actual_name_to_id.get(actual_name)

    if not player_id:
        return {"error": "Player ID not found."}

    gamelog_df = get_player_gamelog_df(player_id, CURRENT_SEASON)
    if gamelog_df is None or gamelog_df.empty:
        return {"error": "Player gamelog unavailable."}

    X = build_player_feature_row(gamelog_df, actual_name)
    if X is None or X.empty:
        return {"error": "Not enough games to build features."}

    model_feature_names = list(getattr(model, "feature_names_in_", []))
    if model_feature_names:
        X = X.reindex(columns=model_feature_names, fill_value=0)

    predicted_points = float(model.predict(X)[0])

    points_std = None
    if isinstance(model_stats, dict):
        points_std = model_stats.get("std_dev")

    over_prob = None
    under_prob = None

    if sportsbook_line is not None and points_std:
        try:
            from scipy.stats import norm
            over_prob = 1 - norm.cdf(sportsbook_line, loc=predicted_points, scale=points_std)
            under_prob = norm.cdf(sportsbook_line, loc=predicted_points, scale=points_std)
        except Exception:
            over_prob = None
            under_prob = None

    season_avg = None
    last5_avg = None
    games_used = len(gamelog_df)

    try:
        gamelog_df["PTS"] = pd.to_numeric(gamelog_df["PTS"], errors="coerce")
        season_avg = float(gamelog_df["PTS"].mean())
        last5_avg = float(gamelog_df["PTS"].tail(5).mean())
    except Exception:
        pass

    live_stats = None
    team_info = None
    try:
        live_stats = get_live_player_stats(actual_name)
    except Exception:
        live_stats = None

    try:
        team_info = get_team_game_info(actual_name)
    except Exception:
        team_info = None

    player_info_df = None
    try:
        player_info_df = get_player_info_df(player_id)
    except Exception:
        player_info_df = None

    team_name = None
    if player_info_df is not None and not player_info_df.empty and "TEAM_NAME" in player_info_df.columns:
        try:
            team_name = player_info_df.iloc[0]["TEAM_NAME"]
        except Exception:
            team_name = None

    return {
        "actual_name": actual_name,
        "predicted_points": predicted_points,
        "sportsbook_line": sportsbook_line,
        "edge": predicted_points - sportsbook_line if sportsbook_line is not None else None,
        "over_prob": over_prob,
        "under_prob": under_prob,
        "season_avg": season_avg,
        "last5_avg": last5_avg,
        "games_used": games_used,
        "live_stats": live_stats,
        "team_info": team_info,
        "team_name": team_name,
    }


st.markdown(
    f"""
    <div class="hero">
        <div class="hero-title">NBA Points Prop Predictor</div>
        <div class="hero-sub">Model-based player points projections and top plays board</div>
        <div class="hero-sub">Version: {APP_VERSION}</div>
    </div>
    """,
    unsafe_allow_html=True
)


top_games_win_rate, top_games_total = get_strong_plays_summary()
health = get_strong_plays_health()

st.markdown('<div class="section-card"><div class="section-title">Top Plays Today</div>', unsafe_allow_html=True)

if top_games_win_rate is not None:
    st.markdown(
        f"""
        <div class="metric-box">
            <div class="metric-label">Win Rate for Top Games</div>
            <div class="metric-value">
                {top_games_win_rate:.1f}% <span style="color: #94a3b8; font-size: 0.9rem; font-weight: 600;">({top_games_total} graded games)</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )
else:
    st.markdown(
        """
        <div class="metric-box">
            <div class="metric-label">Win Rate for Top Games</div>
            <div class="metric-value">
                N/A <span style="color: #94a3b8; font-size: 0.9rem; font-weight: 600;">(no graded games yet)</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

if health:
    st.caption(
        f"Health Check: Last update {format_health_last_update(health.get('last_update'))} | "
        f"Graded {health.get('graded', 0)} | Pending {health.get('pending', 0)}"
    )

odds_api_key = os.getenv("ODDS_API_KEY")

if not odds_api_key:
    st.warning("ODDS_API_KEY not found in environment.")
else:
    try:
        loading_box = st.empty()

        with loading_box.container():
            st.markdown(
                """
                <div class="metric-box">
                    <div class="metric-label">Loading Top Plays</div>
                    <div class="metric-value">Finding today’s strongest edges...</div>
                    <div class="muted">Comparing sportsbook lines against recent player performance</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        top_plays_df = get_top_plays_today_df(odds_api_key)

        loading_box.empty()

        if top_plays_df is None or top_plays_df.empty:
            st.info("No top plays available right now.")
        else:
            st.dataframe(top_plays_df, use_container_width=True, height=360)

    except Exception as e:
        st.error(f"Could not build top plays board: {e}")

st.markdown("</div>", unsafe_allow_html=True)


st.markdown('<div class="section-card"><div class="section-title">Player Prediction</div>', unsafe_allow_html=True)

actual_name_to_id, player_names = get_player_lookup()

selected_player = st.selectbox(
    "Search for a player",
    options=player_names,
    index=None,
    placeholder="Start typing a player name..."
)

sportsbook_line = st.number_input(
    "Sportsbook points line",
    min_value=0.0,
    max_value=80.0,
    value=25.5,
    step=0.5
)

if selected_player:
    with st.spinner("Building prediction..."):
        result = build_prediction(selected_player, sportsbook_line)

    if result.get("error"):
        st.error(result["error"])
    else:
        col1, col2, col3 = st.columns(3)

        player_info_df = get_player_info_df(actual_name_to_id[result["actual_name"]])

        team_abbr = None
        if player_info_df is not None and not player_info_df.empty and "TEAM_ABBREVIATION" in player_info_df.columns:
            try:
                team_abbr = player_info_df.iloc[0]["TEAM_ABBREVIATION"]
            except Exception:
                team_abbr = None

        team_theme = get_team_theme(team_abbr or "")
        primary = team_theme["primary"]
        secondary = team_theme["secondary"]

        model_bg = (
            f"linear-gradient(135deg, "
            f"{hex_to_rgba(primary, 0.35)} 0%, "
            f"{hex_to_rgba(secondary, 0.25)} 50%, "
            f"rgba(15, 23, 42, 0.95) 100%)"
        )
        model_border = primary
        model_glow = hex_to_rgba(primary, 0.28)
        model_stat_bg = "rgba(255, 255, 255, 0.06)"
        model_stat_border = hex_to_rgba(secondary, 0.32)
        model_label_color = "#cbd5e1"

        predicted_points = result["predicted_points"]
        edge = result["edge"]
        over_prob = result.get("over_prob")
        under_prob = result.get("under_prob")

        if edge is not None:
            pick_text, pick_kind = get_pick_label(edge)
        else:
            pick_text, pick_kind = "No Posted Line", "neutral"

        if pick_kind == "over":
            pick_bg = "rgba(34,197,94,0.25)"
            pick_border = "#22c55e"
            pick_text_color = "#22c55e"
        elif pick_kind == "under":
            pick_bg = "rgba(239,68,68,0.25)"
            pick_border = "#ef4444"
            pick_text_color = "#ef4444"
        else:
            pick_bg = "rgba(148,163,184,0.12)"
            pick_border = "#94a3b8"
            pick_text_color = "#e5e7eb"

        probability_text = "N/A"
        if over_prob is not None and under_prob is not None:
            probability_text = f"O {over_prob * 100:.1f}% / U {under_prob * 100:.1f}%"

        interpretation_text = ""
        if edge is None:
            interpretation_text = ""
        elif abs(edge) < 1.5:
            interpretation_text = (
                f"The model projects {predicted_points:.2f} points against a line of {sportsbook_line:.1f}, "
                f"which is too close to call confidently."
            )
        else:
            interpretation_text = (
                f"The model projects a {over_prob:.0%} chance of the over hitting compared to "
                f"{under_prob:.0%} for the under."
            )

        st.markdown(
            f"""
            <div class="model-card" style="
                background: {model_bg};
                border: 2px solid {hex_to_rgba(model_border, 0.95)};
                box-shadow: 0 0 0 1px rgba(255,255,255,0.04), 0 0 22px {model_glow};
            ">
                <div class="model-title">{result["actual_name"]}</div>
                <div class="model-subtitle">Model Output</div>

                <div class="model-main">
                    <div class="model-stat" style="
                        background: {model_stat_bg};
                        border: 1px solid {model_stat_border};
                    ">
                        <div class="model-stat-label" style="color: {model_label_color};">Predicted Points</div>
                        <div class="model-stat-value">{predicted_points:.2f}</div>
                    </div>

                    <div class="model-stat" style="
                        background: {model_stat_bg};
                        border: 1px solid {model_stat_border};
                    ">
                        <div class="model-stat-label" style="color: {model_label_color};">Sportsbook Line</div>
                        <div class="model-stat-value">{sportsbook_line:.1f}</div>
                    </div>

                    <div class="model-stat" style="
                        background: {model_stat_bg};
                        border: 1px solid {model_stat_border};
                    ">
                        <div class="model-stat-label" style="color: {model_label_color};">Model Edge</div>
                        <div class="model-stat-value">{edge:+.2f}</div>
                    </div>

                    <div class="model-stat" style="
                        background: {model_stat_bg};
                        border: 1px solid {model_stat_border};
                    ">
                        <div class="model-stat-label" style="color: {model_label_color};">Probability Split</div>
                        <div class="model-stat-value">{probability_text}</div>
                    </div>
                </div>

                <div class="small-note">{interpretation_text}</div>

                <div class="pick-banner" style="
                    background: {pick_bg};
                    border: 2px solid {pick_border};
                    color: {pick_text_color};
                ">
                    {pick_text}
                </div>

                <div class="small-note">Trained regression model output compared against the current sportsbook line.</div>
            </div>
            """,
            unsafe_allow_html=True
        )

        with col1:
            st.markdown(
                f"""
                <div class="mini-card">
                    <div class="mini-title">Predicted Points</div>
                    <div class="mini-value">{predicted_points:.2f}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col2:
            edge_display = "N/A" if edge is None else f"{edge:+.2f}"
            st.markdown(
                f"""
                <div class="mini-card">
                    <div class="mini-title">Edge vs Line</div>
                    <div class="mini-value">{edge_display}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col3:
            pick_label = "N/A"
            if edge is not None:
                pick_label = "OVER" if edge > 0 else "UNDER"
            st.markdown(
                f"""
                <div class="mini-card">
                    <div class="mini-title">Model Pick</div>
                    <div class="mini-value">{pick_label}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        subcol1, subcol2, subcol3 = st.columns(3)

        with subcol1:
            st.markdown(
                f"""
                <div class="mini-card">
                    <div class="mini-title">Season Avg</div>
                    <div class="mini-value">{'N/A' if season_avg is None else f'{season_avg:.2f}'}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with subcol2:
            st.markdown(
                f"""
                <div class="mini-card">
                    <div class="mini-title">Last 5 Avg</div>
                    <div class="mini-value">{'N/A' if last5_avg is None else f'{last5_avg:.2f}'}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with subcol3:
            st.markdown(
                f"""
                <div class="mini-card">
                    <div class="mini-title">Games Used</div>
                    <div class="mini-value">{result['games_used']}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        over_prob = result.get("over_prob")
        under_prob = result.get("under_prob")

        if over_prob is not None and under_prob is not None:
            prob_col1, prob_col2 = st.columns(2)

            with prob_col1:
                st.markdown(
                    f"""
                    <div class="mini-card">
                        <div class="mini-title">Over Probability</div>
                        <div class="mini-value">{over_prob * 100:.1f}%</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            with prob_col2:
                st.markdown(
                    f"""
                    <div class="mini-card">
                        <div class="mini-title">Under Probability</div>
                        <div class="mini-value">{under_prob * 100:.1f}%</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        live_stats = result.get("live_stats")
        if live_stats:
            st.markdown("#### Live Game Status")
            live_col1, live_col2, live_col3 = st.columns(3)

            with live_col1:
                st.markdown(
                    f"""
                    <div class="mini-card">
                        <div class="mini-title">Current Points</div>
                        <div class="mini-value">{safe_live_display(live_stats.get('points', 'N/A'))}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            with live_col2:
                st.markdown(
                    f"""
                    <div class="mini-card">
                        <div class="mini-title">Minutes</div>
                        <div class="mini-value">{safe_live_display(live_stats.get('minutes', 'N/A'))}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            with live_col3:
                st.markdown(
                    f"""
                    <div class="mini-card">
                        <div class="mini-title">Game Status</div>
                        <div class="mini-value">{safe_live_display(live_stats.get('game_status', 'Live'))}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        team_info = result.get("team_info")
        if team_info:
            st.caption(
                f"Team Context: {team_info}"
            )

st.markdown("</div>", unsafe_allow_html=True)
