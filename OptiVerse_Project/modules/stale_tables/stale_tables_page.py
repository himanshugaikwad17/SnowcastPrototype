import streamlit as st
from datetime import datetime, timedelta, timezone
from shared.snowflake_connector import connect_to_snowflake
import pytz
import pandas as pd
import io
from shared.llm_client import call_llm  # Add LLM support


def render(conn_dict):
    st.header("🧹 Stale Table Detection")

    if not conn_dict:
        st.error("❌ No active Snowflake connection. Please connect from the 'Connections' tab.")
        return

    inactivity_days = st.slider("Mark tables as stale if not altered in the last N days", min_value=1, max_value=365, value=30)

    default_keywords = ["temp", "test", "staging", "tmp"]
    all_keywords = list(set(default_keywords))

    keyword_filter = st.multiselect(
        "Detect stale tables using these keywords",
        options=all_keywords,
        default=all_keywords
    )

    confirm_delete = st.checkbox("Enable deletion of selected stale tables")

    threshold_date = datetime.now(timezone.utc) - timedelta(days=inactivity_days)

    try:
        conn = connect_to_snowflake(conn_dict)
        cursor = conn.cursor()

        # Build WHERE clause for keywords
        keyword_conditions = " OR ".join(
            [f"LOWER(t.table_name) ILIKE '%{kw.lower()}%'" for kw in keyword_filter]
        )

        # Final SQL query with keyword filtering, user, and size info
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
            st.success("🎉 No stale tables found based on current criteria.")
        else:
            st.warning(f"⚠️ Found {len(stale_tables)} stale tables")

            to_delete = []
            for schema, table, last_altered, created, size_bytes, last_altered_by in stale_tables:
                with st.expander(f"{schema}.{table}", expanded=False):
                    st.write(f"Last Altered: `{last_altered}`")
                    st.write(f"Created: `{created}`")
                    st.write(f"Size (bytes): `{size_bytes}`")
                    st.write(f"Last Altered By: `{last_altered_by}`")
                    if confirm_delete:
                        if st.checkbox(f"Drop {schema}.{table}", key=f"drop_{schema}_{table}"):
                            to_delete.append((schema, table))

            if confirm_delete and to_delete:
                if st.button("💣 Drop Selected Tables"):
                    for schema, table in to_delete:
                        drop_query = f"DROP TABLE IF EXISTS {conn_dict['database']}.{schema}.{table}"
                        cursor.execute(drop_query)
                        st.success(f"✅ Dropped table: {schema}.{table}")

            # Add download as CSV feature
            df = pd.DataFrame(stale_tables, columns=[
                "Schema", "Table", "Last Altered", "Created", "Size (Bytes)", "Last Altered By"])
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button("🗅️ Download Stale Tables as CSV", data=csv, file_name="stale_tables.csv", mime="text/csv")

            # Add LLM Summary
            if st.checkbox("🤖 Generate LLM Summary of Stale Tables"):
                # Top 5 users with most stale tables
                top_users = df['Last Altered By'].value_counts().head(5)
                top_users_str = "\n".join([f"{user}: {count} tables" for user, count in top_users.items()])

                prompt = f"""
                Analyze the following stale Snowflake tables and provide a short summary of what types of tables they are (e.g., temp, historical, unused), and if they can be safely deleted.
                Also, list the top 5 users who have created the most stale tables:

                {df[['Schema', 'Table', 'Size (Bytes)', 'Last Altered By']].to_string(index=False)}

                Top 5 Users by number of stale tables:
                {top_users_str}
                """
                provider = st.session_state.get("llm_provider", "together")
                model = st.session_state.get("llm_model", "meta-llama/llama-4-scout-17b-16e-instruct")
                insights = call_llm(prompt, model=model, provider=provider)
                st.markdown("### 🔍 LLM Insights")
                st.info(insights)

        cursor.close()
        conn.close()

    except Exception as e:
        st.error(f"❌ Error loading tables: {e}")
