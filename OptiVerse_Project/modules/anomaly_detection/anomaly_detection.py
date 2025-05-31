import streamlit as st
from datetime import datetime, timedelta, timezone
from shared.snowflake_connector import connect_to_snowflake
import pandas as pd
from shared.llm_client import call_llm  # Add LLM support

def render(conn_dict):
    st.header("\U0001F4CA Anomaly Detection")

    if not conn_dict:
        st.error("❌ No active Snowflake connection. Please connect from the 'Connections' tab.")
        return

    threshold_date = datetime.now(timezone.utc) - timedelta(days=30)
    keyword_filter = ["temp", "test", "staging", "tmp"]

    if "show_stale_detail" not in st.session_state:
        st.session_state.show_stale_detail = False

    try:
        conn = connect_to_snowflake(conn_dict)
        cursor = conn.cursor()

        keyword_conditions = " OR ".join(
            [f"LOWER(t.table_name) ILIKE '%{kw.lower()}%'" for kw in keyword_filter]
        )

        sql_query = f"""
            SELECT t.table_schema, t.table_name, t.last_altered, t.created, t.bytes AS size_bytes,
                   t.LAST_DDL_BY
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_TYPE = 'BASE TABLE'
              AND ({keyword_conditions})
            ORDER BY t.bytes DESC NULLS LAST
        """
        cursor.execute(sql_query)
        tables = cursor.fetchall()

        stale_tables = []
        for schema, table, last_altered, created, size_bytes, last_altered_by in tables:
            if last_altered is None or last_altered.replace(tzinfo=timezone.utc) < threshold_date:
                stale_tables.append((schema, table, last_altered, created, size_bytes, last_altered_by))

        if not stale_tables:
            st.success("🎉 No stale tables found based on 30-day inactivity rule and keyword filter.")
        else:
            df = pd.DataFrame(stale_tables, columns=[
                "Schema", "Table", "Last Altered", "Created", "Size (Bytes)", "Last Altered By"])

            top_users = df['Last Altered By'].value_counts().head(5)
            top_users_str = ", ".join([f"{user}: {count}" for user, count in top_users.items()])

            total_size_gb = df['Size (Bytes)'].sum() / 1e9
            estimated_cost = round(total_size_gb * 23, 2)  # $23/GB

            if not st.session_state.show_stale_detail:
                with st.expander("\U0001F9F9 Stale Table Insight Summary", expanded=True):
                    st.markdown(f"**Detected:** {len(stale_tables)} stale tables")
                    st.markdown(f"**Estimated Monthly Cost:** ${estimated_cost:.2f}")
                    st.markdown(f"**Top Creators:** {top_users_str}")
                    if st.button("\U0001F50E View Detailed Insight"):
                        st.session_state.show_stale_detail = True
                        st.rerun()
            else:
                prompt = f"""
                Analyze the following stale Snowflake tables (not modified in last 30 days).
                Provide cost-saving insights, top owners, and risks of keeping them:

                {df[['Schema', 'Table', 'Size (Bytes)', 'Last Altered By']].to_string(index=False)}

                Top 5 Users by Stale Tables:
                {top_users.to_string()}

                Total Stale Size: {total_size_gb:.2f} GB
                Estimated Monthly Storage Cost: ${estimated_cost:.2f}
                """
                provider = st.session_state.get("llm_provider", "together")
                model = st.session_state.get("llm_model", "meta-llama/llama-4-scout-17b-16e-instruct")
                insights = call_llm(prompt, model=model, provider=provider)
                st.markdown("### \U0001F4CB Detailed Insight")
                st.info(insights)

                if st.button("⬅️ Back to Summary"):
                    st.session_state.show_stale_detail = False
                    st.rerun()

        cursor.close()
        conn.close()

    except Exception as e:
        st.error(f"❌ Error loading anomaly data: {e}")
