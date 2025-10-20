import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import altair as alt
from streamlit_autorefresh import st_autorefresh

# تحديث الصفحة تلقائي كل 10 ثواني
st_autorefresh(interval=10_000, key="real_time_refresh")

st.set_page_config(page_title="📈 Real-Time Stock Dashboard", layout="wide")
st.title("📈 Real-Time Stock Dashboard")

# Fetch data from PostgreSQL
@st.cache_data(ttl=10)
def get_stock_data():
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="stock_data",
            user="admin",
            password="admin"
        )
        query = "SELECT * FROM stock_metrics ORDER BY window_end DESC LIMIT 500;"
        df = pd.read_sql(query, conn)
        df['window_end'] = pd.to_datetime(df['window_end'])
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

df = get_stock_data()

if not df.empty:
    st.subheader("Latest Metrics Table")
    st.dataframe(df.sort_values(by='window_end', ascending=False))

    # ======= Layout: 2 Columns for Price and Quantity =======
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Average Price Over Time")
        fig_price = px.line(df, x='window_end', y='avg_price', color='s', title='Average Price')
        st.plotly_chart(fig_price, use_container_width=True)

    with col2:
        st.subheader("📊 Total Quantity Over Time")
        fig_quantity = px.bar(df, x='window_end', y='total_quantity', color='s', title='Total Quantity')
        st.plotly_chart(fig_quantity, use_container_width=True)

    # ======= Layout: 2 Columns for Traded Value and Buy/Sell Ratio =======
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("📊 Traded Value Over Time")
        fig_traded_value = px.line(df, x='window_end', y='traded_value', color='s', title='Traded Value')
        st.plotly_chart(fig_traded_value, use_container_width=True)

    with col4:
        st.subheader("📊 Buy/Sell Ratio Over Time")
        fig_ratio = px.line(df, x='window_end', y='buy_sell_ratio', color='s', title='Buy/Sell Ratio')
        st.plotly_chart(fig_ratio, use_container_width=True)

    # ======= Volatility Heatmap =======
    st.subheader("⚡ Volatility Heatmap")
    heatmap_df = df.groupby(['window_end', 's'])['volatility'].mean().reset_index()
    heatmap_df_pivot = heatmap_df.pivot(index='window_end', columns='s', values='volatility').fillna(0)
    fig_volatility = px.imshow(
        heatmap_df_pivot.T,
        labels=dict(x="Time", y="Stock", color="Volatility"),
        x=heatmap_df_pivot.index,
        y=heatmap_df_pivot.columns,
        aspect="auto"
    )
    st.plotly_chart(fig_volatility, use_container_width=True)

    # ======= Scatter Chart =======
    st.subheader("📈 Scatter: Total Quantity vs Average Price")
    scatter_chart = alt.Chart(df).mark_circle(size=60).encode(
        x='avg_price',
        y='total_quantity',
        color='s',
        tooltip=['s', 'avg_price', 'total_quantity', 'volatility', 'buy_sell_ratio']
    ).interactive()
    st.altair_chart(scatter_chart, use_container_width=True)


else:
    st.info("No data available yet.")
