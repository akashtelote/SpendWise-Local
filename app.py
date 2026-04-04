import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import os

# Set page configuration
st.set_page_config(page_title="SpendWise Local", layout="wide")

DB_PATH = "data/processed/expenses.db"

@st.cache_data
def load_data():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM transactions WHERE Transaction_Type IN ('Debit', 'Credit')"
    df = pd.read_sql_query(query, conn)
    conn.close()

    if not df.empty:
        df['Date'] = pd.to_datetime(df['Date'])
        # Sort by date for proper trend lines
        df = df.sort_values('Date')

    return df

def run_pipeline():
    """Triggers the ingestion, parsing, and processing pipeline."""
    with st.spinner("Running Data Pipeline..."):
        try:
            import src.ingestion
            import src.parser
            import src.processor

            st.toast("Downloading statements...")
            src.ingestion.download_statements()

            st.toast("Parsing PDFs...")
            src.parser.parse_all_pdfs()

            st.toast("Processing and Storing data...")
            raw_path = "data/processed/raw_transactions.csv"
            if os.path.exists(raw_path):
                raw_df = pd.read_csv(raw_path)
                src.processor.process_and_store(raw_df)
                st.success("Data pipeline completed successfully!")
                # Clear the cache so new data is loaded
                load_data.clear()
                st.rerun()
            else:
                st.error("Pipeline failed: raw_transactions.csv not found.")
        except Exception as e:
            st.error(f"Error running pipeline: {e}")

def main():
    st.title("SpendWise Local Dashboard")
    st.write("Welcome to your local expense tracker dashboard.")

    # --- Sidebar Layout ---
    st.sidebar.header("Controls")
    if st.sidebar.button("Refresh Data", use_container_width=True):
        run_pipeline()

    df = load_data()

    if df.empty:
        st.warning("No data found. Please ensure the pipeline has run and generated data.")
        return

    # Date Range Filter
    st.sidebar.subheader("Filters")
    min_date = df['Date'].min().date()
    max_date = df['Date'].min().date()
    if not df.empty:
        max_date = df['Date'].max().date()

    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    if len(date_range) == 2:
        start_date, end_date = date_range
        # Filter dataframe based on date range
        mask = (df['Date'].dt.date >= start_date) & (df['Date'].dt.date <= end_date)
        filtered_df = df.loc[mask]
    else:
        filtered_df = df

    if filtered_df.empty:
        st.warning("No data found for the selected date range.")
        return

    # --- Metrics Logic ---
    debit_df = filtered_df[filtered_df['Transaction_Type'] == 'Debit']

    # 1. Total Spend
    total_spend = debit_df['Amount'].sum()

    # 2. Top Category
    if not debit_df.empty:
        top_category_series = debit_df.groupby('Category')['Amount'].sum().sort_values(ascending=False)
        top_category = top_category_series.index[0] if not top_category_series.empty else "N/A"
        top_category_amount = top_category_series.iloc[0] if not top_category_series.empty else 0
    else:
        top_category = "N/A"
        top_category_amount = 0

    # 3. MoM Change
    # To calculate MoM accurately, we look at the entire dataset (df, not filtered_df)
    # We find the latest month in df, sum its debits, then find the previous month and sum its debits.
    all_debits = df[df['Transaction_Type'] == 'Debit'].copy()
    if not all_debits.empty:
        all_debits['YearMonth'] = all_debits['Date'].dt.to_period('M')
        monthly_spend = all_debits.groupby('YearMonth')['Amount'].sum()

        # Sort months
        monthly_spend = monthly_spend.sort_index()

        if len(monthly_spend) >= 2:
            current_month = monthly_spend.index[-1]
            previous_month = monthly_spend.index[-2]

            current_spend = monthly_spend.loc[current_month]
            previous_spend = monthly_spend.loc[previous_month]

            if previous_spend > 0:
                mom_change = ((current_spend - previous_spend) / previous_spend) * 100
            else:
                mom_change = 0.0
        else:
            mom_change = 0.0
    else:
        mom_change = 0.0

    # --- Top-Level Metrics Layout ---
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(label="Total Spend", value=f"₹{total_spend:,.2f}")
    with col2:
        st.metric(label="Top Category", value=top_category, delta=f"₹{top_category_amount:,.2f} spent", delta_color="off")
    with col3:
        st.metric(label="MoM Change (Total Spend)", value=f"{mom_change:.1f}%", delta=f"{mom_change:.1f}% vs last month", delta_color="inverse")

    st.markdown("---")

    # --- Visualizations ---
    st.subheader("Visualizations")

    viz_col1, viz_col2 = st.columns(2)

    with viz_col1:
        # Donut Chart: Spend by Category
        if not debit_df.empty:
            cat_df = debit_df.groupby('Category')['Amount'].sum().reset_index()
            fig_donut = px.pie(cat_df, values='Amount', names='Category', hole=0.4, title="Spend by Category")
            fig_donut.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_donut, use_container_width=True)
        else:
            st.info("No expense data available for the Donut Chart.")

    with viz_col2:
        # Bar Chart: Monthly Cashflow (Income vs Expenses)
        cashflow_df = filtered_df.copy()
        if not cashflow_df.empty:
            cashflow_df['YearMonth'] = cashflow_df['Date'].dt.strftime('%Y-%m')
            # Group by Month and Transaction_Type
            monthly_cf = cashflow_df.groupby(['YearMonth', 'Transaction_Type'])['Amount'].sum().reset_index()

            fig_bar = px.bar(
                monthly_cf,
                x='YearMonth',
                y='Amount',
                color='Transaction_Type',
                barmode='group',
                title="Monthly Cashflow (Income vs Expenses)",
                color_discrete_map={'Debit': 'red', 'Credit': 'green'}
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No data available for Monthly Cashflow Bar Chart.")

    st.markdown("---")

    # Trend Line: Cumulative Spend (Current vs Previous Month)
    st.subheader("Spending Pace (Current vs Previous Month)")
    if len(all_debits) > 0 and 'YearMonth' in all_debits.columns:
        unique_months = sorted(all_debits['YearMonth'].unique())

        if len(unique_months) >= 2:
            cur_month = unique_months[-1]
            prev_month = unique_months[-2]

            # Extract data for these two months
            cur_month_data = all_debits[all_debits['YearMonth'] == cur_month].copy()
            prev_month_data = all_debits[all_debits['YearMonth'] == prev_month].copy()

            # Sort by date
            cur_month_data = cur_month_data.sort_values('Date')
            prev_month_data = prev_month_data.sort_values('Date')

            # Add 'DayOfMonth'
            cur_month_data['DayOfMonth'] = cur_month_data['Date'].dt.day
            prev_month_data['DayOfMonth'] = prev_month_data['Date'].dt.day

            # Group by day and calculate cumulative sum
            cur_daily = cur_month_data.groupby('DayOfMonth')['Amount'].sum().reset_index()
            cur_daily['Cumulative Spend'] = cur_daily['Amount'].cumsum()
            cur_daily['Month'] = 'Current Month'

            prev_daily = prev_month_data.groupby('DayOfMonth')['Amount'].sum().reset_index()
            prev_daily['Cumulative Spend'] = prev_daily['Amount'].cumsum()
            prev_daily['Month'] = 'Previous Month'

            # Combine
            trend_df = pd.concat([cur_daily, prev_daily])

            fig_trend = px.line(
                trend_df,
                x='DayOfMonth',
                y='Cumulative Spend',
                color='Month',
                markers=True,
                title="Cumulative Spend Pace"
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("Need at least two months of data to show spending pace comparison.")

    st.markdown("---")

    # --- Smart Optimization Tips ---
    st.subheader("Smart Optimization Tips")

    tips = []

    if not debit_df.empty:
        # Category breakdown
        cat_breakdown = debit_df.groupby('Category')['Amount'].sum()
        total_filtered_spend = cat_breakdown.sum()

        # 1. Food & Dining Tip
        if 'Food & Dining' in cat_breakdown:
            food_spend = cat_breakdown['Food & Dining']
            food_pct = (food_spend / total_filtered_spend) * 100
            if food_pct > 20:
                # Check cards used for Food & Dining
                food_cards = debit_df[debit_df['Category'] == 'Food & Dining']['Source_Card'].unique()
                if not any('hdfc millenia' in str(card).lower() for card in food_cards):
                    tips.append("🍔 **High Dining Spend Detected:** Your Food & Dining spend is over 20% of your total expenses. Tip: Move your Dining/Swiggy/Zomato spends to HDFC Millenia for 5% cashback.")

        # 2. Shopping Tip
        if 'Shopping' in cat_breakdown:
            shopping_spend = cat_breakdown['Shopping']
            shopping_pct = (shopping_spend / total_filtered_spend) * 100
            if shopping_pct > 15:
                shopping_cards = debit_df[debit_df['Category'] == 'Shopping']['Source_Card'].unique()
                # Check if they are using a non-optimized card (e.g., Generic)
                if any('generic' in str(card).lower() for card in shopping_cards):
                    tips.append("🛍️ **High Shopping Spend:** You're using a generic card for shopping. Tip: Suggest using Amazon Pay ICICI (for Amazon) or Flipkart Axis (for Flipkart) to maximize rewards.")

        # 3. Fuel Tip
        if 'Fuel' in cat_breakdown:
            fuel_cards = debit_df[debit_df['Category'] == 'Fuel']['Source_Card'].unique()
            if not any('sbi' in str(card).lower() for card in fuel_cards):
                tips.append("⛽ **Fuel Spends Detected:** Tip: You are not using an SBI card for fuel. Consider the SBI BPCL card for better fuel surcharges and rewards.")

    if tips:
        for tip in tips:
            st.warning(tip)
    else:
        st.success("You are spending optimally based on our current checks! Good job.")

if __name__ == "__main__":
    main()
