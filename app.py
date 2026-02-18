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
    # Keep common particles in lower-case (optional)
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
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_ANON_KEY env vars")
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


# -----------------------------
# App
# -----------------------------
st.set_page_config(page_title="MD Campaign Contributions", layout="wide")

st.title("Maryland Campaign Contributions Dashboard")
st.caption("Aggregate views only. Built on Supabase + Streamlit.")

sb = get_supabase()

# Sidebar controls
st.sidebar.header("Filters")

# Candidate dropdown from v_candidates (view)
cand_resp = sb.table("v_candidates").select("candidate").execute()
candidates = [r["candidate"] for r in (cand_resp.data or []) if r.get("candidate")]
candidates_sorted = sorted(candidates, key=lambda x: (x or "").lower())

selected_candidate = st.sidebar.selectbox(
    "Candidate",
    options=candidates_sorted,
    index=0 if candidates_sorted else None,
)

show_charts = st.sidebar.checkbox("Show charts", value=True)

# Optional: filter by year/office/district (if your data has these)
# We can populate from v_candidate_totals for existing values:
meta_resp = sb.table("v_candidate_totals").select("election_year,office,district").execute()
meta_rows = meta_resp.data or []
years = sorted({r.get("election_year") for r in meta_rows if r.get("election_year") is not None})
offices = sorted({r.get("office") for r in meta_rows if r.get("office")})
districts = sorted({r.get("district") for r in meta_rows if r.get("district")})

year_filter = st.sidebar.multiselect("Election year", options=years, default=years[-1:] if years else [])
office_filter = st.sidebar.multiselect("Office", options=offices, default=offices if offices else [])
district_filter = st.sidebar.multiselect("District", options=districts, default=districts if districts else [])

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
            ins = sb.table("contact_signups").insert({
                "email": email.strip(),
                "name": name.strip() or None,
                "zip5": (zip5.strip() or None),
                "source": source.strip() or None,
            }).execute()
            # With your RLS, INSERT should succeed; SELECT is blocked.
            if getattr(ins, "data", None) is not None:
                st.success("Thanks! You're signed up.")
            else:
                # supabase-py sometimes returns errors on .error, sometimes in exception; be defensive
                st.error("Signup failed. Please try again later.")

# -----------------------------
# Section 1: Candidate totals
# -----------------------------
st.subheader("Candidate totals (by donor bucket)")

totals_resp = sb.table("v_candidate_totals").select(
    "election_year,office,district,candidate,committee_name,committee_type,public_funding_requested,donor_bucket,txns,total_amount"
).execute()
totals_df = to_df(totals_resp.data)

if not totals_df.empty:
    # filters
    if year_filter:
        totals_df = totals_df[totals_df["election_year"].isin(year_filter)]
    if office_filter:
        totals_df = totals_df[totals_df["office"].isin(office_filter)]
    if district_filter:
        totals_df = totals_df[totals_df["district"].isin(district_filter)]

    # nicer display
    totals_df["candidate_display"] = totals_df["candidate"].fillna("").map(title_case_name)

    # default: show only selected candidate unless user wants all
    show_all = st.checkbox("Show all candidates", value=False)
    if not show_all and selected_candidate:
        totals_df = totals_df[totals_df["candidate"] == selected_candidate]

    totals_df = totals_df.sort_values(["total_amount"], ascending=False)

    st.dataframe(
        totals_df[[
            "election_year","office","district","candidate_display",
            "donor_bucket","txns","total_amount",
            "committee_name","committee_type","public_funding_requested"
        ]],
        use_container_width=True,
        hide_index=True,
    )

    download_button(
        totals_df[[
            "election_year","office","district","candidate","donor_bucket","txns","total_amount",
            "committee_name","committee_type","public_funding_requested"
        ]],
        filename=f"candidate_totals_{datetime.now().date()}.csv",
        label="Download candidate totals CSV",
    )

    if show_charts:
        chart_df = totals_df.copy()
        if not chart_df.empty:
            c = (
                alt.Chart(chart_df)
                .mark_bar()
                .encode(
                    x=alt.X("donor_bucket:N", title="Donor bucket"),
                    y=alt.Y("total_amount:Q", title="Total amount"),
                    tooltip=["candidate_display:N", "donor_bucket:N", "total_amount:Q", "txns:Q"],
                )
            )
            st.altair_chart(c, use_container_width=True)
else:
    st.info("No data found in v_candidate_totals. Confirm the view exists and data is ingested.")


# -----------------------------
# Section 2: Donor overlap (flattened) via RPC
# -----------------------------
st.subheader("Donor overlap (flattened)")

st.caption("Donors who gave to the selected candidate *and* at least one other candidate (separate totals per candidate).")

if selected_candidate:
    rpc = sb.rpc("get_donor_overlap_flat", {"p_candidate": selected_candidate}).execute()
    overlap_df = to_df(getattr(rpc, "data", None))

    if not overlap_df.empty:
        overlap_df["donor_name"] = overlap_df["donor_name"].fillna("")
        overlap_df["candidate_display"] = overlap_df["candidate"].fillna("").map(title_case_name)

        # Optional extra filtering
        if year_filter and "election_year" in overlap_df.columns:
            overlap_df = overlap_df[overlap_df["election_year"].isin(year_filter)]
        if office_filter and "office" in overlap_df.columns:
            overlap_df = overlap_df[overlap_df["office"].isin(office_filter)]
        if district_filter and "district" in overlap_df.columns:
            overlap_df = overlap_df[overlap_df["district"].isin(district_filter)]

        # Controls
        only_others = st.checkbox("Exclude selected candidate rows (show 'other candidates' only)", value=False)
        if only_others:
            overlap_df = overlap_df[overlap_df["candidate"] != selected_candidate]

        overlap_df = overlap_df.sort_values(["donor_name", "total_to_candidate"], ascending=[True, False])

        st.dataframe(
            overlap_df[[
                "donor_name","donor_bucket","candidate_display","total_to_candidate","txns",
                "election_year","office","district","first_date","last_date","donor_key"
            ]],
            use_container_width=True,
            hide_index=True,
        )

        download_button(
            overlap_df[[
                "donor_key","donor_name","donor_bucket","election_year","office","district",
                "candidate","total_to_candidate","txns","first_date","last_date"
            ]],
            filename=f"donor_overlap_flat_{selected_candidate}_{datetime.now().date()}.csv",
            label="Download donor overlap CSV",
        )

        if show_charts:
            # Simple: top 25 donors by total across all candidates in overlap set
            donor_totals = (
                overlap_df.groupby(["donor_name"], as_index=False)["total_to_candidate"].sum()
                .sort_values("total_to_candidate", ascending=False)
                .head(25)
            )
            c2 = (
                alt.Chart(donor_totals)
                .mark_bar()
                .encode(
                    x=alt.X("total_to_candidate:Q", title="Total across overlap set"),
                    y=alt.Y("donor_name:N", sort="-x", title="Top donors (overlap)"),
                    tooltip=["donor_name:N", "total_to_candidate:Q"],
                )
            )
            st.altair_chart(c2, use_container_width=True)
    else:
        st.info("No overlap found for this candidate (or the RPC/view has no rows).")
else:
    st.warning("No candidates found. Populate v_candidates first.")
