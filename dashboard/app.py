import streamlit as st
import pandas as pd
import psycopg2


# ---------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------

st.set_page_config(
    page_title="E-Commerce Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ---------------------------------------------------------
# CUSTOM CSS
# ---------------------------------------------------------

st.markdown(
    """
    <style>

    /* Main background */
    .stApp {
        background-color: #0b1220;
        color: #e5e7eb;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #080f1c;
        border-right: 1px solid #1f2937;
    }

    section[data-testid="stSidebar"] .block-container {
        padding-top: 2rem;
    }

    /* Remove Streamlit top padding */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1450px;
    }

    /* Header */
    .dashboard-title {
        font-size: 32px;
        font-weight: 700;
        color: #f8fafc;
        margin-bottom: 2px;
    }

    .dashboard-subtitle {
        color: #94a3b8;
        font-size: 14px;
        margin-bottom: 20px;
    }

    /* KPI cards */
    .metric-card {
        background: #111c2e;
        border: 1px solid #24344d;
        border-radius: 10px;
        padding: 18px;
        min-height: 125px;
    }

    .metric-label {
        color: #94a3b8;
        font-size: 13px;
        margin-bottom: 8px;
    }

    .metric-value {
        color: #f8fafc;
        font-size: 27px;
        font-weight: 700;
    }

    .metric-note {
        color: #64748b;
        font-size: 12px;
        margin-top: 8px;
    }

    /* Section titles */
    .section-title {
        color: #e2e8f0;
        font-size: 18px;
        font-weight: 600;
        margin-top: 15px;
        margin-bottom: 10px;
    }

    /* Pipeline status */
    .status-box {
        background: #0f1929;
        border: 1px solid #24344d;
        border-radius: 8px;
        padding: 12px 14px;
        margin-bottom: 8px;
    }

    .status-dot {
        color: #22c55e;
        font-size: 12px;
    }

    .status-text {
        color: #cbd5e1;
        font-size: 13px;
        margin-left: 6px;
    }

    /* Sidebar logo */
    .side-title {
        font-size: 20px;
        font-weight: 700;
        color: #f8fafc;
    }

    .side-subtitle {
        color: #64748b;
        font-size: 12px;
        margin-bottom: 25px;
    }

    /* Footer */
    .footer {
        border-top: 1px solid #1f2937;
        margin-top: 30px;
        padding-top: 15px;
        color: #64748b;
        font-size: 12px;
    }

    /* Dataframe */
    div[data-testid="stDataFrame"] {
        border: 1px solid #24344d;
        border-radius: 8px;
    }

    </style>
    """,
    unsafe_allow_html=True
)


# ---------------------------------------------------------
# DATABASE CONNECTION
# ---------------------------------------------------------

def get_connection():

    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="ecommerce_db",
        user="ecommerce_user",
        password="ecommerce_pass"
    )


# ---------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------

@st.cache_data(ttl=10)
def load_metrics():

    conn = get_connection()

    query = """
        SELECT
            category,
            total_revenue,
            total_units_sold,
            total_purchases,
            window_start,
            window_end
        FROM realtime_metrics
        ORDER BY window_start DESC;
    """

    df = pd.read_sql(query, conn)

    conn.close()

    return df

@st.cache_data(ttl=10)
def load_top_products():

    conn = get_connection()

    query = """
        SELECT
            product_id,
            product_name,
            units_sold,
            revenue
        FROM top_products
        ORDER BY revenue DESC
        LIMIT 10;
    """

    df = pd.read_sql(query, conn)

    conn.close()

    return df

# ---------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------

with st.sidebar:

    st.markdown(
        '<div class="side-title">🛒 E-Commerce</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="side-subtitle">Real-Time Analytics</div>',
        unsafe_allow_html=True
    )

    st.markdown("---")

    st.markdown("### Navigation")

    page = st.radio(
        "",
        [
            "Overview",
            "Categories",
            "Live Data"
        ],
        label_visibility="collapsed"
    )

    st.markdown("---")

    st.markdown("### Pipeline Status")

    st.markdown(
        """
        <div class="status-box">
            <span class="status-dot">●</span>
            <span class="status-text">Kafka — Running</span>
        </div>

        <div class="status-box">
            <span class="status-dot">●</span>
            <span class="status-text">PySpark — Running</span>
        </div>

        <div class="status-box">
            <span class="status-dot">●</span>
            <span class="status-text">PostgreSQL — Running</span>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown("---")

    if st.button("↻ Refresh Data", use_container_width=True):

        st.cache_data.clear()
        st.rerun()


# ---------------------------------------------------------
# LOAD
# ---------------------------------------------------------

try:

    df = load_metrics()
    top_products = load_top_products()

except Exception as e:

    st.error(
        "Unable to connect to PostgreSQL. "
        "Make sure the PostgreSQL container is running."
    )

    st.stop()


# ---------------------------------------------------------
# HEADER
# ---------------------------------------------------------

st.markdown(
    '<div class="dashboard-title">Real-Time E-Commerce Analytics</div>',
    unsafe_allow_html=True
)

st.markdown(
    '<div class="dashboard-subtitle">'
    'Kafka → PySpark Structured Streaming → Gold → PostgreSQL'
    '</div>',
    unsafe_allow_html=True
)


# ---------------------------------------------------------
# EMPTY STATE
# ---------------------------------------------------------

if df.empty:

    st.info(
        "No analytics data available yet. "
        "Start the Kafka producer and streaming pipeline."
    )

    st.stop()


# ---------------------------------------------------------
# KPI CALCULATIONS
# ---------------------------------------------------------

total_revenue = df["total_revenue"].sum()

total_units = df["total_units_sold"].sum()

total_purchases = df["total_purchases"].sum()

active_categories = df["category"].nunique()


# ---------------------------------------------------------
# KPI CARDS
# ---------------------------------------------------------

c1, c2, c3, c4 = st.columns(4)


with c1:

    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">TOTAL REVENUE</div>
            <div class="metric-value">₹{total_revenue:,.0f}</div>
            <div class="metric-note">
                Aggregated purchase revenue
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


with c2:

    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">UNITS SOLD</div>
            <div class="metric-value">{total_units:,}</div>
            <div class="metric-note">
                Total quantity purchased
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


with c3:

    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">PURCHASE EVENTS</div>
            <div class="metric-value">{total_purchases:,}</div>
            <div class="metric-note">
                Processed purchase events
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


with c4:

    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">CATEGORIES</div>
            <div class="metric-value">{active_categories}</div>
            <div class="metric-note">
                Active product categories
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


st.markdown("")


# ---------------------------------------------------------
# OVERVIEW PAGE
# ---------------------------------------------------------

if page == "Overview":

    left, right = st.columns(2)

    # Revenue by category
    with left:

        st.markdown(
            '<div class="section-title">Revenue by Category</div>',
            unsafe_allow_html=True
        )

        category_df = (
            df.groupby("category", as_index=False)
            ["total_revenue"]
            .sum()
            .sort_values(
                "total_revenue",
                ascending=False
            )
        )

        st.bar_chart(
            category_df.set_index("category"),
            height=330
        )


    # Revenue trend
    with right:

        st.markdown(
            '<div class="section-title">Revenue Trend</div>',
            unsafe_allow_html=True
        )

        trend_df = (
            df.groupby(
                "window_start",
                as_index=False
            )["total_revenue"]
            .sum()
            .sort_values("window_start")
        )

        st.line_chart(
            trend_df.set_index("window_start"),
            height=330
        )


    # Category summary
    st.markdown(
        '<div class="section-title">Category Summary</div>',
        unsafe_allow_html=True
    )

    summary = (
        df.groupby("category")
        .agg(
            revenue=("total_revenue", "sum"),
            units=("total_units_sold", "sum"),
            purchases=("total_purchases", "sum")
        )
        .reset_index()
        .sort_values(
            "revenue",
            ascending=False
        )
    )

    summary["revenue"] = summary["revenue"].round(2)

    summary.columns = [
        "Category",
        "Revenue",
        "Units Sold",
        "Purchases"
    ]

    st.dataframe(
        summary,
        use_container_width=True,
        hide_index=True
    )

    st.markdown(
        '<div class="section-title">Top Products</div>',
        unsafe_allow_html=True
    )

    products_display = top_products.copy()

    products_display["revenue"] = (
        products_display["revenue"].round(2)
    )

    products_display.columns = [
        "Product ID",
        "Product",
        "Units Sold",
        "Revenue"
    ]

    st.dataframe(
        products_display,
        use_container_width=True,
        hide_index=True
    )

# ---------------------------------------------------------
# CATEGORIES PAGE
# ---------------------------------------------------------

elif page == "Categories":

    st.markdown(
        '<div class="section-title">Category Performance</div>',
        unsafe_allow_html=True
    )

    category_df = (
        df.groupby("category")
        .agg(
            Revenue=("total_revenue", "sum"),
            Units=("total_units_sold", "sum"),
            Purchases=("total_purchases", "sum")
        )
        .reset_index()
        .sort_values(
            "Revenue",
            ascending=False
        )
    )

    category_df["Revenue"] = category_df["Revenue"].round(2)

    st.dataframe(
        category_df,
        use_container_width=True,
        hide_index=True
    )

    st.markdown(
        '<div class="section-title">Units Sold by Category</div>',
        unsafe_allow_html=True
    )

    units_chart = (
        category_df
        .set_index("category")["Units"]
    )

    st.bar_chart(
        units_chart,
        height=350
    )


# ---------------------------------------------------------
# LIVE DATA PAGE
# ---------------------------------------------------------

elif page == "Live Data":

    st.markdown(
        '<div class="section-title">Recent Streaming Windows</div>',
        unsafe_allow_html=True
    )

    recent = df.head(30).copy()

    recent["total_revenue"] = (
        recent["total_revenue"].round(2)
    )

    recent.columns = [
        "Category",
        "Revenue",
        "Units Sold",
        "Purchases",
        "Window Start",
        "Window End"
    ]

    st.dataframe(
        recent,
        use_container_width=True,
        hide_index=True
    )

    st.markdown(
        '<div class="section-title">Latest Window</div>',
        unsafe_allow_html=True
    )

    latest = df.iloc[0]

    st.write(
        f"**{latest['category']}**  |  "
        f"Revenue: ₹{latest['total_revenue']:,.2f}  |  "
        f"Units: {int(latest['total_units_sold'])}  |  "
        f"Purchases: {int(latest['total_purchases'])}"
    )


# ---------------------------------------------------------
# FOOTER
# ---------------------------------------------------------

st.markdown(
    """
    <div class="footer">
        Real-Time E-Commerce Data Engineering Pipeline
        &nbsp; • &nbsp;
        Kafka • PySpark • PostgreSQL • Streamlit
    </div>
    """,
    unsafe_allow_html=True
)

