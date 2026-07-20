import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from datetime import datetime

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

from config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

# ----------------------------------------------------------------------------
# Page config
# ----------------------------------------------------------------------------
st.set_page_config(page_title="FinVent", page_icon="🏦", layout="wide")

REFRESH_SECONDS = 5

# ----------------------------------------------------------------------------
# Theme / CSS — dark navy fintech look
# ----------------------------------------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

.stApp {
    background: radial-gradient(circle at top left, #111a2e 0%, #0a0f1e 55%, #070b15 100%);
}
section[data-testid="stSidebar"] {
    background: #0d1526;
    border-right: 1px solid #1e2a44;
}

/* Hide default streamlit chrome */
#MainMenu, footer, header {visibility: hidden;}

/* Top bar */
.topbar {
    display:flex; align-items:center; justify-content:space-between;
    padding: 4px 2px 18px 2px;
    border-bottom: 1px solid #1e2a44;
    margin-bottom: 22px;
}
.brand {
    font-size: 26px; font-weight: 800; color: #f8fafc;
    letter-spacing: -0.5px;
}
.brand span { color: #ef4444; }
.subtitle { color:#64748b; font-size:13px; margin-top:2px; }
.status-pill {
    display:flex; align-items:center; gap:8px;
    background:#0f1e17; border:1px solid #14532d;
    padding:6px 14px; border-radius:999px;
    color:#4ade80; font-size:13px; font-weight:600;
}
.dot {
    width:8px; height:8px; border-radius:50%;
    background:#22c55e; box-shadow: 0 0 8px #22c55e;
    animation: pulse 1.6s infinite;
}
@keyframes pulse { 0%{opacity:1;} 50%{opacity:0.35;} 100%{opacity:1;} }
.clock { color:#475569; font-size:12px; margin-top:4px; text-align:right; }

/* KPI cards */
.kpi-card {
    background: linear-gradient(155deg, #131f38 0%, #0e1729 100%);
    border: 1px solid #22304d;
    border-radius: 16px;
    padding: 20px 22px;
    position: relative;
    overflow: hidden;
}
.kpi-card::before {
    content:"";
    position:absolute; top:0; left:0; right:0; height:3px;
}
.kpi-red::before { background:#ef4444; }
.kpi-green::before { background:#22c55e; }
.kpi-blue::before { background:#3b82f6; }
.kpi-amber::before { background:#f59e0b; }

.kpi-label { color:#8aa0c7; font-size:12.5px; font-weight:600; text-transform:uppercase; letter-spacing:0.6px; }
.kpi-value { color:#f8fafc; font-size:28px; font-weight:800; margin-top:10px; letter-spacing:-0.5px; }
.kpi-delta { font-size:12px; margin-top:6px; color:#64748b; }

/* Section headers */
.section-title {
    color:#e2e8f0; font-size:15px; font-weight:700;
    margin: 6px 0 10px 2px; letter-spacing:0.2px;
}

/* Chart / table containers */
.panel {
    background:#0e1729;
    border:1px solid #1e2a44;
    border-radius:16px;
    padding:16px 18px 6px 18px;
}

/* Dataframe tweaks */
[data-testid="stDataFrame"] { border-radius:12px; overflow:hidden; }

/* Badges inside fraud table won't render natively, keep risk coloring via px chart instead */

hr { border-color:#1e2a44; }
</style>
""", unsafe_allow_html=True)


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def format_inr(value: float) -> str:
    """Format a number in Indian currency style: ₹12.45 Cr / ₹56.8 Lakh / ₹8,200"""
    if value is None or pd.isna(value):
        return "₹ 0"
    value = float(value)
    sign = "-" if value < 0 else ""
    value = abs(value)
    if value >= 1_00_00_000:
        return f"{sign}₹ {value / 1_00_00_000:,.2f} Cr"
    if value >= 1_00_000:
        return f"{sign}₹ {value / 1_00_000:,.2f} Lakh"
    if value >= 1_000:
        return f"{sign}₹ {value:,.0f}"
    return f"{sign}₹ {value:,.2f}"


@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def load_data():
    conn = get_connection()
    transactions = pd.read_sql(
        "select * from transactions order by transaction_id desc limit 5000", conn
    )
    frauds = pd.read_sql(
        "select * from fraud_transactions order by alert_id desc limit 2000", conn
    )
    return transactions, frauds


PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#cbd5e1", family="Inter"),
    title_font=dict(size=15, color="#e2e8f0"),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
    margin=dict(t=50, b=20, l=10, r=10),
)

RISK_COLORS = {"low": "#22c55e", "medium": "#f59e0b", "high": "#ef4444",
               "Low": "#22c55e", "Medium": "#f59e0b", "High": "#ef4444"}


# ----------------------------------------------------------------------------
# Static shell (renders once — sidebar filters live outside the fragment)
# ----------------------------------------------------------------------------
st.sidebar.markdown("### ⚙️ Filters")

try:
    _tx_preview, _ = load_data()
    all_types = sorted(_tx_preview["transaction_type"].unique())
except Exception as e:
    st.error(f"Could not connect to database: {e}")
    st.stop()

selected_types = st.sidebar.multiselect(
    "Transaction Type", all_types, default=all_types
)
st.sidebar.caption(f"Auto-refreshing every {REFRESH_SECONDS}s — only data panels update, no full-page reload.")


# ----------------------------------------------------------------------------
# Live-refreshing fragment — this is the piece that reruns every 5s,
# so the sidebar / page chrome never flickers.
# ----------------------------------------------------------------------------
@st.fragment(run_every=REFRESH_SECONDS)
def live_dashboard():
    transactions, frauds = load_data()
    transactions = transactions[transactions["transaction_type"].isin(selected_types)]

    tcount = len(transactions)
    fcount = len(frauds)
    total_amount = transactions["amount"].sum() if tcount else 0
    frate = (fcount / tcount * 100) if tcount else 0

    # Top bar
    st.markdown(f"""
    <div class="topbar">
        <div>
            <div class="brand">🏦 Fin<span>Vent</span></div>
            <div class="subtitle">Real-Time Event-Driven Fraud Detection Platform</div>
        </div>
        <div style="text-align:right;">
            <div class="status-pill"><div class="dot"></div>LIVE</div>
            <div class="clock">Last refresh · {datetime.now().strftime("%H:%M:%S")}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # KPI cards
    k1, k2, k3, k4 = st.columns(4)
    kpis = [
        (k1, "kpi-blue", "Transactions", f"{tcount:,}"),
        (k2, "kpi-red", "Fraud Alerts", f"{fcount:,}"),
        (k3, "kpi-green", "Total Volume", format_inr(total_amount)),
        (k4, "kpi-amber", "Fraud Rate", f"{frate:.2f}%"),
    ]
    for col, cls, label, val in kpis:
        with col:
            st.markdown(f"""
            <div class="kpi-card {cls}">
                <div class="kpi-label">{label}</div>
                <div class="kpi-value">{val}</div>
            </div>
            """, unsafe_allow_html=True)

    st.write("")
    st.markdown('<div class="section-title">📊 Analytics</div>', unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        if tcount:
            fig = px.pie(
                transactions, names="transaction_type", hole=0.55,
                title="Transaction Type Distribution",
                color_discrete_sequence=["#3b82f6", "#22c55e", "#f59e0b", "#ef4444", "#a855f7", "#06b6d4"],
            )
            fig.update_traces(textinfo="percent", textfont_color="#0a0f1e")
            fig.update_layout(**PLOTLY_LAYOUT, showlegend=True)
            st.plotly_chart(fig, use_container_width=True, key="chart_type")
        else:
            st.info("No transactions for selected filters.")
        st.markdown('</div>', unsafe_allow_html=True)

    with c2:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        if tcount:
            fig = px.histogram(
                transactions, x="amount", nbins=40,
                title="Transaction Amount Distribution",
                color_discrete_sequence=["#3b82f6"],
            )
            fig.update_layout(**PLOTLY_LAYOUT, bargap=0.05,
                               xaxis_title="Amount (₹)", yaxis_title="Count")
            st.plotly_chart(fig, use_container_width=True, key="chart_amount")
        else:
            st.info("No transactions for selected filters.")
        st.markdown('</div>', unsafe_allow_html=True)

    with c3:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        if fcount and "risk_level" in frauds.columns:
            risk = frauds["risk_level"].value_counts().reset_index()
            risk.columns = ["Risk", "Count"]
            fig = px.bar(
                risk, x="Risk", y="Count", title="Fraud Risk Level Distribution",
                color="Risk", color_discrete_map=RISK_COLORS,
            )
            fig.update_layout(**PLOTLY_LAYOUT, showlegend=False)
            st.plotly_chart(fig, use_container_width=True, key="chart_risk")
        else:
            st.info("No fraud alerts yet.")
        st.markdown('</div>', unsafe_allow_html=True)

    st.write("")

    # Fraud alerts table
    st.markdown('<div class="section-title">🚨 Latest Fraud Alerts</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    if fcount:
        show_cols = [c for c in ["amount", "sender", "receiver", "risk_score",
                                  "risk_level", "reasons", "detected_at"] if c in frauds.columns]
        fraud_view = frauds.sort_values("alert_id", ascending=False).head(10)[show_cols].copy()
        if "amount" in fraud_view.columns:
            fraud_view["amount"] = fraud_view["amount"].apply(format_inr)
        st.dataframe(fraud_view, use_container_width=True, hide_index=True)
    else:
        st.info("No fraud alerts yet. System is monitoring transactions in real time.")
    st.markdown('</div>', unsafe_allow_html=True)

    st.write("")

    # Transactions table
    st.markdown('<div class="section-title">📄 Latest Transactions</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    if tcount:
        tx_view = transactions.sort_values("transaction_id", ascending=False).head(15).copy()
        if "amount" in tx_view.columns:
            tx_view["amount"] = tx_view["amount"].apply(format_inr)
        st.dataframe(tx_view, use_container_width=True, hide_index=True)
    else:
        st.info("No transactions for selected filters.")
    st.markdown('</div>', unsafe_allow_html=True)


live_dashboard()