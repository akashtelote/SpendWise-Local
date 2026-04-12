import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Set page configuration
st.set_page_config(page_title="SpendWise Local", layout="wide")

DB_PATH = Path("data/processed/expenses.db")

@st.cache_data
def load_data():
    if not DB_PATH.exists():
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    query = "SELECT date, description, amount, category, source_card, transaction_type FROM transactions WHERE transaction_type IN ('debit', 'credit')"
    df = pd.read_sql_query(query, conn)
    conn.close()

    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])
        # Sort by date for proper trend lines
        df = df.sort_values('date')

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
            parsed_df = src.parser.parse_all_pdfs()

            st.toast("Processing and Storing data...")
            if not parsed_df.empty:
                src.processor.process_and_store(parsed_df)

                # Optional: still save raw transactions for reference if needed
                raw_path = Path("data/processed/raw_transactions.csv")
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                parsed_df.to_csv(raw_path, index=False)

                st.success("Data pipeline completed successfully!")
                st.cache_data.clear()
                st.rerun()
            else:
                st.warning("Pipeline completed but no new transactions were found.")
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
    from datetime import date, timedelta
    today = date.today()
    df_min_date = df['date'].min().date()
    df_max_date = df['date'].max().date()

    # Calculate resilient bounds
    slider_min_value = min(df_min_date, today - timedelta(days=60))
    slider_max_value = max(df_max_date, today)
    default_start = max(df_min_date, today - timedelta(days=60))

    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(default_start, today),
        min_value=slider_min_value,
        max_value=slider_max_value
    )

    if len(date_range) == 2:
        start_date, end_date = date_range
        # Filter dataframe based on date range
        mask = (df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)
        filtered_df = df.loc[mask]
    else:
        filtered_df = df

    if filtered_df.empty:
        st.info('No transactions found for the selected range. Total rows in database: ' + str(len(df)))
        return

    # --- Metrics Logic ---
    debit_df = filtered_df[filtered_df['transaction_type'] == 'debit']

    # 1. Total Spend
    total_spend = float(debit_df['amount'].sum())

    # 2. Top Category
    if not debit_df.empty:
        top_category_series = debit_df.groupby('category')['amount'].sum().sort_values(ascending=False)
        top_category = top_category_series.index[0] if not top_category_series.empty else "N/A"
        top_category_amount = top_category_series.iloc[0] if not top_category_series.empty else 0
    else:
        top_category = "N/A"
        top_category_amount = 0

    # 3. Transaction Count
    transaction_count = len(filtered_df)

    # 4. Rolling Spend Velocity (30d)
    # Window A (Current): Sum of debits from today back to today - 30.
    # Window B (Baseline): Sum of debits from today - 31 back to today - 60.
    all_debits = df[df['transaction_type'] == 'debit'].copy()
    if not all_debits.empty:
        today_datetime = pd.to_datetime(today)
        window_a_start = today_datetime - pd.Timedelta(days=30)
        window_b_start = today_datetime - pd.Timedelta(days=60)
        window_b_end = today_datetime - pd.Timedelta(days=31)

        mask_a = (all_debits['date'] >= window_a_start) & (all_debits['date'] <= today_datetime)
        mask_b = (all_debits['date'] >= window_b_start) & (all_debits['date'] <= window_b_end)

        current_spend = float(all_debits.loc[mask_a, 'amount'].sum())
        previous_spend = float(all_debits.loc[mask_b, 'amount'].sum())

        if previous_spend > 0:
            velocity_change = ((current_spend - previous_spend) / previous_spend) * 100
        else:
            velocity_change = 0.0
    else:
        velocity_change = 0.0

    # --- Top-Level Metrics Layout ---
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(label="Total Spend", value=f"₹{total_spend:,.2f}")
    with col2:
        st.metric(label="Top Category", value=top_category, delta=f"₹{top_category_amount:,.2f} spent", delta_color="off")
    with col3:
        st.metric(label="Transaction Count", value=f"{transaction_count:,}")
    with col4:
        st.metric(label="Rolling Spend Velocity (30d)", value=f"{velocity_change:.1f}%", delta=f"{velocity_change:.1f}% vs previous 30d", delta_color="inverse")

    st.markdown("---")

    # --- Visualizations ---
    st.subheader("Visualizations")

    viz_col1, viz_col2 = st.columns(2)

    with viz_col1:
        # Donut Chart: Spend by Category
        if not debit_df.empty:
            cat_df = debit_df.groupby('category')['amount'].sum().reset_index()
            fig_donut = px.pie(cat_df, values='amount', names='category', hole=0.4, title="Spend by Category")
            fig_donut.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_donut, use_container_width=True)
        else:
            st.info("No expense data available for the Donut Chart.")

    with viz_col2:
        # Bar Chart: Monthly Cashflow (Income vs Expenses)
        cashflow_df = filtered_df.copy()
        if not cashflow_df.empty:
            cashflow_df['YearMonth'] = cashflow_df['date'].dt.strftime('%Y-%m')
            # Group by Month and Transaction_Type
            monthly_cf = cashflow_df.groupby(['YearMonth', 'transaction_type'])['amount'].sum().reset_index()

            fig_bar = px.bar(
                monthly_cf,
                x='YearMonth',
                y='amount',
                color='transaction_type',
                barmode='group',
                title="Monthly Cashflow (Income vs Expenses)",
                color_discrete_map={'debit': 'red', 'credit': 'green'}
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No data available for Monthly Cashflow Bar Chart.")

    st.markdown("---")

    # Weekly Spend Pattern Bar Chart
    st.subheader("Weekly Spend Pattern")
    if not debit_df.empty:
        # Group by week (starting on Monday by default in pandas)
        # Using the currently filtered dataframe to show patterns within the selected range
        weekly_df = debit_df.copy()

        # Determine the start of the week for each date
        weekly_df['Week'] = weekly_df['date'].dt.to_period('W').dt.start_time

        weekly_spend = weekly_df.groupby('Week')['amount'].sum().reset_index()
        weekly_spend = weekly_spend.sort_values('Week')

        # Calculate daily sum for 7-day rolling average
        daily_spend = debit_df.groupby('date')['amount'].sum().reset_index()
        daily_spend = daily_spend.sort_values('date')

        # Reindex to ensure missing days have 0 spend before rolling average
        all_days = pd.date_range(start=daily_spend['date'].min(), end=daily_spend['date'].max())
        daily_spend = daily_spend.set_index('date').reindex(all_days, fill_value=0).reset_index()
        daily_spend = daily_spend.rename(columns={'index': 'date'})

        daily_spend['7-Day Rolling Avg'] = daily_spend['amount'].rolling(window=7, min_periods=1).mean()

        # Create a figure with secondary y-axis style using plotly graph objects to overlay line and bar
        fig_trend = go.Figure()

        # Add Weekly Spend Bars
        fig_trend.add_trace(go.Bar(
            x=weekly_spend['Week'],
            y=weekly_spend['amount'],
            name='Weekly Spend',
            marker_color='red',
            opacity=0.7
        ))

        # Add 7-Day Rolling Average Line
        fig_trend.add_trace(go.Scatter(
            x=daily_spend['date'],
            y=daily_spend['7-Day Rolling Avg'],
            name='7-Day Rolling Avg',
            mode='lines',
            line=dict(color='blue', width=2)
        ))

        fig_trend.update_layout(
            title="Weekly Spend Pattern with 7-Day Trend",
            xaxis_title="Date",
            yaxis_title="Amount",
            barmode='group',
            hovermode='x unified'
        )

        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("No expense data available in the selected range to show weekly patterns.")

    st.markdown("---")

    # --- Smart Optimization Tips ---
    st.subheader("Smart Optimization Tips")

    tips = []

    if not debit_df.empty:
        # Category breakdown
        cat_breakdown = debit_df.groupby('category')['amount'].sum()
        total_filtered_spend = cat_breakdown.sum()

        # 1. Food & Dining Tip
        if 'Food & Dining' in cat_breakdown:
            food_spend = cat_breakdown['Food & Dining']
            food_pct = (food_spend / total_filtered_spend) * 100
            if food_pct > 20:
                # Check cards used for Food & Dining
                food_cards = debit_df[debit_df['category'] == 'Food & Dining']['source_card'].unique()
                if not any('hdfc millenia' in str(card).lower() for card in food_cards):
                    tips.append("🍔 **High Dining Spend Detected:** Your Food & Dining spend is over 20% of your total expenses. Tip: Move your Dining/Swiggy/Zomato spends to HDFC Millenia for 5% cashback.")

        # 2. Shopping Tip
        if 'Shopping' in cat_breakdown:
            shopping_spend = cat_breakdown['Shopping']
            shopping_pct = (shopping_spend / total_filtered_spend) * 100
            if shopping_pct > 15:
                shopping_cards = debit_df[debit_df['category'] == 'Shopping']['source_card'].unique()
                # Check if they are using a non-optimized card (e.g., Generic)
                if any('generic' in str(card).lower() for card in shopping_cards):
                    tips.append("🛍️ **High Shopping Spend:** You're using a generic card for shopping. Tip: Suggest using Amazon Pay ICICI (for Amazon) or Flipkart Axis (for Flipkart) to maximize rewards.")

        # 3. Fuel Tip
        if 'Fuel' in cat_breakdown:
            fuel_cards = debit_df[debit_df['category'] == 'Fuel']['source_card'].unique()
            if not any('sbi' in str(card).lower() for card in fuel_cards):
                tips.append("⛽ **Fuel Spends Detected:** Tip: You are not using an SBI card for fuel. Consider the SBI BPCL card for better fuel surcharges and rewards.")

    if tips:
        for tip in tips:
            st.warning(tip)
    else:
        st.success("You are spending optimally based on our current checks! Good job.")

    st.markdown("---")

    # --- Recent Transactions ---
    st.subheader("Recent Transactions")

    # Sort the table by date descending
    recent_transactions = filtered_df.sort_values('date', ascending=False).copy()

    # Format amount for display in the dataframe without modifying the numeric data type if possible,
    # but Streamlit's style can be applied, or we can just round/format the column.
    if 'amount' in recent_transactions.columns:
        # Instead of replacing the column with a string which might break sorting/filtering,
        # Streamlit 1.23+ st.dataframe allows pandas Styler for formatting
        styled_df = recent_transactions.style.format({
            "amount": lambda x: f"₹{x:,.2f}"
        })
        st.dataframe(styled_df, use_container_width=True)
    else:
        st.dataframe(recent_transactions, use_container_width=True)

if __name__ == "__main__":
    main()
