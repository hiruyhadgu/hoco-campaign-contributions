import os
import re
from datetime import datetime
import pandas as pd
import streamlit as st
import altair as alt
from supabase import create_client


# -----------------------------
# Helpers
# -----------------------------
def title_case_name(s: str) -> str:
    """Light title-case that won't explode on None."""
    if not s:
        return s
    keep_lower = {"of", "and", "the", "for", "to", "in", "on", "at"}
    parts = re.split(r"(\s+)", s.strip())
    out = []
    for p in parts:
        if p.isspace():
            out.append(p)
        else:
            low = p.lower()
            out.append(low if low in keep_lower else low.capitalize())
    return "".join(out)


def to_df(rows):
    return pd.DataFrame(rows or [])


@st.cache_resource
def get_supabase():
    url = os.environ.get("SUPABASE_URL") or st.secrets.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_ANON_KEY") or st.secrets.get("SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError(
            "Missing SUPABASE_URL or SUPABASE_ANON_KEY. "
            "Set env vars or create .streamlit/secrets.toml"
        )
    return create_client(url, key)


def download_button(df: pd.DataFrame, filename: str, label: str):
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=label,
        data=csv,
        file_name=filename,
        mime="text/csv",
        use_container_width=True,
    )


def cf_table(sb, name: str):
    """
    Always read from the 'cf' schema. This avoids PostgREST schema-cache issues
    (it otherwise defaults to public.*).
    """
    return sb.schema("cf").table(name)


def cf_rpc(sb, fn: str, payload: dict):
    """
    Always call RPC in the 'cf' schema.
    """
    return sb.schema("cf").rpc(fn, payload).execute()


# -----------------------------
# Fixed scope for this public dashboard
# -----------------------------
SCOPE_YEAR = 2026
SCOPE_OFFICE = "County Executive"
SCOPE_SEAT = "AtLarge"


# -----------------------------
# Cached fetchers
# -----------------------------
@st.cache_data(show_spinner=False)
def fetch_candidates_ce_2026():
    sb = get_supabase()
    resp = cf_table(sb, "v_candidates_ce_2026").select("*").execute()
    return resp.data or []


@st.cache_data(show_spinner=False)
def fetch_candidate_totals_fixed():
    sb = get_supabase()
    q = (
        cf_table(sb, "v_candidate_totals")
        .select("*")
        .eq("election_year", SCOPE_YEAR)
        .eq("office", SCOPE_OFFICE)
        .eq("seat", SCOPE_SEAT)
    )
    return q.execute().data or []


@st.cache_data(show_spinner=False)
def fetch_totals_by_type_fixed():
    sb = get_supabase()
    q = (
        cf_table(sb, "v_candidate_totals_by_contributor_type")
        .select("*")
        .eq("election_year", SCOPE_YEAR)
        .eq("office", SCOPE_OFFICE)
        .eq("seat", SCOPE_SEAT)
    )
    return q.execute().data or []


@st.cache_data(show_spinner=False)
def fetch_overlap_summary_fixed(candidate_last: str, contributor_type: str | None):
    sb = get_supabase()
    resp = cf_rpc(sb, "get_overlap_summary_for_candidate", {
        "p_candidate_last": candidate_last,
        "p_election_year": SCOPE_YEAR,
        "p_office": SCOPE_OFFICE,
        "p_seat": SCOPE_SEAT,
        "p_contributor_type": contributor_type,
    })
    return getattr(resp, "data", None) or []


@st.cache_data(show_spinner=False)
def fetch_overlap_pairwise_fixed(a_last: str, b_last: str, contributor_type: str | None):
    sb = get_supabase()
    resp = cf_rpc(sb, "get_donor_overlap_pairwise", {
        "p_candidate_a_last": a_last,
        "p_candidate_b_last": b_last,
        "p_election_year": SCOPE_YEAR,
        "p_office": SCOPE_OFFICE,
        "p_seat": SCOPE_SEAT,
        "p_contributor_type": contributor_type,
    })
    return getattr(resp, "data", None) or []


def fetch_donor_history_fixed(candidate_last: str, contributor_type: str | None, limit: int):
    """
    Deliberately NOT cached: can be expensive and depends on limit.
    Requires the DB function to accept p_limit (recommended).
    """
    sb = get_supabase()
    resp = cf_rpc(sb, "get_donor_history_for_candidate", {
        "p_candidate_last": candidate_last,
        "p_election_year": SCOPE_YEAR,
        "p_office": SCOPE_OFFICE,
        "p_seat": SCOPE_SEAT,
        "p_contributor_type": contributor_type,
        "p_limit": limit,
    })
    return getattr(resp, "data", None) or []


def fetch_entity_history_fixed(candidate_last: str, candidate_first: str, limit: int, current_year_only: bool):
    sb = get_supabase()
    resp = cf_rpc(sb, "get_entity_donors_with_other_candidate_history", {
        "p_candidate_last": candidate_last,
        "p_candidate_first": candidate_first,
        "p_election_year": SCOPE_YEAR,
        "p_office": SCOPE_OFFICE,
        "p_seat": SCOPE_SEAT,
        "p_limit": limit,
        "p_current_year_only": current_year_only,
    })
    return getattr(resp, "data", None) or []


@st.cache_data(ttl=300)
def fetch_top_donors_for_candidate_fixed(candidate_last: str, contributor_type_group: str | None, top_n: int):
    """
    Top donors to the selected candidate within the locked scope (2026 / County Executive / AtLarge),
    using cf.v_donor_to_candidate_typed.
    """
    sb = get_supabase()

    q = (
        sb.schema("cf")
        .table("v_donor_to_candidate_typed")
        .select(
            "donor_key, contributor_name, contributor_type, city, state, "
            "total_amount, txn_count, first_txn, last_txn, contributor_type_group"
        )
        .eq("election_year", SCOPE_YEAR)
        .eq("office", SCOPE_OFFICE)
        .eq("seat", SCOPE_SEAT)
        .eq("candidate_last", candidate_last)
        .order("total_amount", desc=True)
        .limit(int(top_n))
    )

    # contributor_type_group is what we use in the view ("Individual", "Entity", "Public Financing", "Unknown")
    if contributor_type_group:
        q = q.eq("contributor_type_group", contributor_type_group)

    resp = q.execute()
    return getattr(resp, "data", None) or []


# -----------------------------
# App
# -----------------------------
st.set_page_config(page_title="MD Campaign Contributions", layout="wide")

st.title("Howard County 2026 County Executive — Campaign Contributions")
st.caption("Built on Supabase + Streamlit. Data from MDCRIS exports.")

st.markdown(
    """
<div style="padding: 10px 12px; border-left: 6px solid #1f77b4; background: #f6f8fa; border-radius: 10px;">
<b>How to use this page:</b><br/>
1) Choose a candidate on the left.<br/>
2) Use the tabs to explore totals, shared donors (overlap), and donor history.
</div>
""",
    unsafe_allow_html=True,
)

sb = get_supabase()

# -----------------------------
# Sidebar (Candidate first + prominent)
# -----------------------------
st.sidebar.markdown(
        f"""
<div style="padding: 10px 12px; border: 2px solid #1f77b4; background: #f6f8fa; border-radius: 12px;">
    <div style="font-size: 18px; font-weight: 800; margin-bottom: 6px;">Choose a candidate</div>
</div>
""",
    unsafe_allow_html=True,
)

cand_rows = fetch_candidates_ce_2026()

cand_display = []
cand_last_list = []
cand_first_list = []
for r in cand_rows:
    last = r.get("candidate_last")
    first = r.get("candidate_first")
    if last and first:
        cand_display.append(f"{title_case_name(last)}, {title_case_name(first)}")
        cand_last_list.append(last)
        cand_first_list.append(first)

if not cand_display:
    st.warning("No candidates found in v_candidates_ce_2026. Confirm the view exists and that the API exposes schema 'cf'.")
    st.stop()

selected_display = st.sidebar.selectbox("Candidate", options=cand_display, index=0, label_visibility="collapsed")
sel_idx = cand_display.index(selected_display)
selected_last = cand_last_list[sel_idx]
selected_first = cand_first_list[sel_idx]

st.sidebar.markdown("### Donor list scope")
current_year_only = st.sidebar.toggle(
    f"Only donors to this candidate in {SCOPE_YEAR}",
    value=False,
    help="If on, only counts donations to the selected candidate during the current election year scope. "
         "If off, counts donations to the candidate across all years/committees.",
)

st.sidebar.markdown("---")

contrib_type_ui = st.sidebar.selectbox(
    "Contributor type",
    options=["All", "Individual", "Entity"],
    index=0,
    help="Filter analysis to individuals or entities. 'All' includes everything."
)
p_contributor_type = None if contrib_type_ui == "All" else contrib_type_ui

if p_contributor_type == "Entity":
    st.sidebar.info(
        "Entity results can be empty if the selected campaign has no entity donors "
        "(common for publicly financed candidates). Try 'All' or 'Individual' to compare donors."
    )

show_charts = st.sidebar.checkbox("Show charts", value=True)

st.sidebar.divider()
st.sidebar.header("Stay in touch")
with st.sidebar.form("contact_form", clear_on_submit=True):
    email = st.text_input("Email*", placeholder="you@example.com")
    name = st.text_input("Name (optional)")
    zip5 = st.text_input("ZIP (optional)", max_chars=10)
    source = st.text_input("Source (optional)", value="streamlit_dashboard")
    submitted = st.form_submit_button("Submit", use_container_width=True)

    if submitted:
        if not email or "@" not in email:
            st.error("Please enter a valid email address.")
        else:
            try:
                # Keep in public.* as requested
                ins = sb.table("contact_signups").insert({
                    "email": email.strip(),
                    "name": name.strip() or None,
                    "zip5": (zip5.strip() or None),
                    "source": source.strip() or None,
                }).execute()
                if getattr(ins, "data", None) is not None:
                    st.success("Thanks! You're signed up.")
                else:
                    st.error("Signup failed. Please try again later.")
            except Exception:
                st.error("Signup failed. Please try again later.")


# -----------------------------
# Tabs
# -----------------------------
tab_overview, tab_overlap, tab_history, tab_notes = st.tabs(
    ["Overview", "Shared donors (overlap)", "Donor history", "Notes / Methodology"]
)

# -----------------------------
# Overview tab
# -----------------------------
with tab_overview:
    st.subheader("Overview: totals and donor mix")

    st.markdown(
        """
<div style="padding: 10px 12px; border-left: 6px solid #2ca02c; background: #f6f8fa; border-radius: 10px;">
<b>Plain-English meaning:</b> “Totals” shows how much money was raised in this race.
“Donor mix” breaks totals down by Individual vs Entity.
</div>
""",
        unsafe_allow_html=True,
    )

    totals_rows = fetch_candidate_totals_fixed()
    totals_df = to_df(totals_rows)

    if totals_df.empty:
        st.info("No rows returned for v_candidate_totals under this scope.")
    else:
        totals_df["candidate_display"] = (
            totals_df["candidate_last"].fillna("").map(title_case_name)
            + ", "
            + totals_df["candidate_first"].fillna("").map(title_case_name)
        )

        show_all_candidates = st.checkbox("Show all candidates", value=False)
        if not show_all_candidates:
            totals_df = totals_df[
                (totals_df["candidate_last"].fillna("").str.lower() == (selected_last or "").lower())
            ]

        total_amount = float(totals_df["total_amount"].fillna(0).sum()) if "total_amount" in totals_df.columns else 0.0
        txn_count = int(totals_df["txn_count"].fillna(0).sum()) if "txn_count" in totals_df.columns else 0

        c1, c2 = st.columns(2)
        c1.metric("Total raised", f"${total_amount:,.0f}")
        c2.metric("Transactions", f"{txn_count:,}")

        display_cols = [c for c in [
            "candidate_display",
            "total_amount", "txn_count", "first_txn", "last_txn"
        ] if c in totals_df.columns]

        st.dataframe(
            totals_df.sort_values("total_amount", ascending=False)[display_cols],
            use_container_width=True,
            hide_index=True,
        )

        download_button(
            totals_df[display_cols],
            filename=f"candidate_totals_{datetime.now().date()}.csv",
            label="Download totals CSV",
        )

        by_type_rows = fetch_totals_by_type_fixed()
        by_type_df = to_df(by_type_rows)

        if not by_type_df.empty:
            by_type_df["candidate_display"] = (
                by_type_df["candidate_last"].fillna("").map(title_case_name)
                + ", "
                + by_type_df["candidate_first"].fillna("").map(title_case_name)
            )

            if not show_all_candidates:
                by_type_df = by_type_df[
                    (by_type_df["candidate_last"].fillna("").str.lower() == (selected_last or "").lower())
                ]

            if p_contributor_type:
                by_type_df = by_type_df[by_type_df["contributor_type_group"] == p_contributor_type]

            st.markdown("### Donor mix (Individual vs Entity)")
            st.dataframe(
                by_type_df.sort_values(["total_amount"], ascending=False)[
                    ["candidate_display", "contributor_type_group", "total_amount", "txn_count", "unique_donors"]
                ],
                use_container_width=True,
                hide_index=True,
            )

            if show_charts and not by_type_df.empty:
                chart = (
                    alt.Chart(by_type_df)
                    .mark_bar()
                    .encode(
                        x=alt.X("contributor_type_group:N", title="Contributor type"),
                        y=alt.Y("total_amount:Q", title="Total amount"),
                        tooltip=["contributor_type_group:N", "total_amount:Q", "txn_count:Q", "unique_donors:Q"],
                    )
                )
                st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No rows found in v_candidate_totals_by_contributor_type under this scope.")

        # -----------------------------
        # Top donors to selected candidate (only when NOT showing all candidates)
        # -----------------------------
        if not show_all_candidates:
            st.markdown("### Top donors to selected candidate")

            top_n = st.slider("Show top N donors", min_value=10, max_value=200, value=50, step=10)

            # Map the sidebar filter (All/Individual/Entity) to contributor_type_group in the view
            # If your sidebar includes "All", p_contributor_type is None
            ct_group = None
            if p_contributor_type == "Individual":
                ct_group = "Individual"
            elif p_contributor_type == "Entity":
                ct_group = "Entity"

            top_rows = fetch_top_donors_for_candidate_fixed(selected_last, ct_group, top_n)
            top_df = to_df(top_rows)

            if top_df.empty:
                st.info("No donors found for this candidate under the current filters.")
            else:
                # Friendly formatting / column order
                show_cols = [c for c in [
                    "contributor_name",
                    "contributor_type_group",
                    "contributor_type",
                    "city",
                    "state",
                    "total_amount",
                    "txn_count",
                    "first_txn",
                    "last_txn",
                ] if c in top_df.columns]

                st.dataframe(
                    top_df[show_cols],
                    use_container_width=True,
                    hide_index=True,
                )

                download_button(
                    top_df[show_cols],
                    filename=f"top_donors_{selected_last}_{SCOPE_YEAR}_{datetime.now().date()}.csv",
                    label="Download top donors CSV",
                )

# -----------------------------
# Overlap tab
# -----------------------------
with tab_overlap:
    st.subheader("Shared donors (overlap)")
    st.caption("Donors who gave to more than one candidate in this race (2026 County Executive).")

    st.markdown(
        """
<div style="padding: 10px 12px; border-left: 6px solid #ff7f0e; background: #f6f8fa; border-radius: 10px;">
<b>Tip:</b> Start with the overlap summary, then click “Run comparison” to load the donor list for a specific pair.
</div>
""",
        unsafe_allow_html=True,
    )

    summary_rows = fetch_overlap_summary_fixed(selected_last, p_contributor_type)
    summary_df = to_df(summary_rows)

    if summary_df.empty:
        st.info("No overlap summary rows returned for this candidate in this scope.")
    else:
        summary_df["other_candidate_display"] = (
            summary_df["other_candidate_last"].fillna("").map(title_case_name)
            + ", "
            + summary_df["other_candidate_first"].fillna("").map(title_case_name)
        )
        st.markdown("### Overlap summary (this candidate vs others)")
        st.dataframe(
            summary_df[[
                "other_candidate_display",
                "shared_donors",
                "shared_amount_to_candidate",
                "shared_amount_to_other",
            ]].sort_values("shared_donors", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("### Drill-down: pick a pair")
    other_candidates = [r["other_candidate_last"] for r in (summary_rows or []) if r.get("other_candidate_last")]
    other_candidates = sorted(set(other_candidates), key=lambda x: (x or "").lower())

    if not other_candidates:
        st.info("No other candidates overlap with this candidate in the current scope.")
    else:
        selected_other_last = st.selectbox(
            "Compare against",
            options=other_candidates,
            format_func=lambda x: title_case_name(x),
        )

        run_pair = st.button("Run comparison", type="primary", use_container_width=True)

        pair_df = pd.DataFrame()
        if run_pair:
            pair_rows = fetch_overlap_pairwise_fixed(
                selected_last, selected_other_last,
                p_contributor_type
            )
            pair_df = to_df(pair_rows)

        if not run_pair:
            st.info("Click **Run comparison** to load the shared donor list for this pair.")
        elif pair_df.empty:
            st.info("No overlapping donors found for this pair in the selected scope.")
        else:
            st.dataframe(
                pair_df[[
                    "contributor_name", "contributor_type", "city", "state",
                    "amount_to_a", "amount_to_b",
                    "first_to_a", "last_to_a",
                    "first_to_b", "last_to_b",
                ]].sort_values(["amount_to_a", "amount_to_b"], ascending=[False, False]),
                use_container_width=True,
                hide_index=True,
            )

            download_button(
                pair_df,
                filename=f"overlap_{selected_last}_vs_{selected_other_last}_{datetime.now().date()}.csv",
                label="Download overlap CSV",
            )

            if show_charts:
                top = pair_df.copy()
                top["combined"] = top["amount_to_a"].fillna(0) + top["amount_to_b"].fillna(0)
                top = top.sort_values("combined", ascending=False).head(25)

                chart = (
                    alt.Chart(top)
                    .mark_bar()
                    .encode(
                        x=alt.X("combined:Q", title="Combined total to both candidates"),
                        y=alt.Y("contributor_name:N", sort="-x", title="Top shared donors"),
                        tooltip=["contributor_name:N", "amount_to_a:Q", "amount_to_b:Q", "combined:Q"],
                    )
                )
                st.altair_chart(chart, use_container_width=True)


# -----------------------------
# Donor history tab
# -----------------------------
with tab_history:
    st.subheader("Donor history across campaigns")
    st.caption("For donors who gave to the selected candidate (in scope), show what else they funded across all cycles in the database.")

    st.markdown(
        """
<div style="padding: 10px 12px; border-left: 6px solid #9467bd; background: #f6f8fa; border-radius: 10px;">
<b>Plain-English meaning:</b> This helps answer: “Did these donors back other candidates in prior years?”
</div>
""",
        unsafe_allow_html=True,
    )

    limit = st.slider("How many top donors to include", 100, 5000, 1500, step=100)
    run_history = st.button("Run donor history", type="primary", use_container_width=True)

    hist_df = pd.DataFrame()
    if run_history:
        if p_contributor_type == "Entity":
            history_rows = fetch_entity_history_fixed(selected_last, selected_first, limit, current_year_only)
        else:
            history_rows = fetch_donor_history_fixed(selected_last, p_contributor_type, limit)
        hist_df = to_df(history_rows)

    if not run_history:
        st.info("Click **Run donor history** to load cross-campaign history for the top donors in the selected scope.")
    elif hist_df.empty:
        st.info("No donor history rows returned for this candidate in the selected scope.")
    else:
        if p_contributor_type == "Entity":
            st.info(
                "If this candidate has no entity donors in 2026 County Executive (AtLarge), "
                "Entity donor history will be empty. Switch to 'All' or 'Individual' to explore donor history."
            )

        if p_contributor_type == "Entity":
            # Entity RPC returns aggregated "other_candidates" list
            # Make it human-readable for the table
            if "other_candidates" in hist_df.columns:
                other_series = hist_df["other_candidates"]
            else:
                other_series = pd.Series([None] * len(hist_df))

            hist_df["other_candidates_display"] = other_series.apply(
                lambda x: ", ".join(x) if isinstance(x, list) else (x or "")
            )

            st.dataframe(
                hist_df[[
                    "contributor_name",
                    "amount_to_candidate", "txn_to_candidate", "first_to_candidate", "last_to_candidate",
                    "amount_to_others",
                    "other_candidates_display",
                ]].sort_values(["amount_to_candidate", "amount_to_others"], ascending=[False, False]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            hist_df["other_candidate_display"] = (
                hist_df["other_candidate_last"].fillna("").map(title_case_name)
                + ", "
                + hist_df["other_candidate_first"].fillna("").map(title_case_name)
            )

            st.dataframe(
                hist_df[[
                    "contributor_name", "contributor_type", "city", "state",
                    "amount_to_candidate", "n_to_candidate", "first_to_candidate", "last_to_candidate",
                    "other_candidate_display", "other_election_year", "other_office", "other_seat",
                    "total_to_other", "n_to_other", "first_to_other", "last_to_other",
                ]].sort_values(["amount_to_candidate", "total_to_other"], ascending=[False, False]),
                use_container_width=True,
                hide_index=True,
            )

        download_button(
            hist_df,
            filename=f"donor_history_{selected_last}_{datetime.now().date()}.csv",
            label="Download donor history CSV",
        )


# -----------------------------
# Notes / Methodology tab
# -----------------------------
with tab_notes:
    st.subheader("Notes / Methodology")
    st.markdown(
        f"""
- Scope is fixed to **{SCOPE_YEAR} • {SCOPE_OFFICE} • {SCOPE_SEAT}**.
- “Overlap” shows shared donors between campaigns within that scope.
- “Donor history” shows what else those donors funded across all loaded elections.
"""
    )
