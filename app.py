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

# Optional: filter by year/office/district (metadata)
meta_resp = sb.table("v_candidate_totals").select("election_year,office,district").execute()
meta_rows = meta_resp.data or []
years = sorted({r.get("election_year") for r in meta_rows if r.get("election_year") is not None})
offices = sorted({r.get("office") for r in meta_rows if r.get("office")})
districts = sorted({r.get("district") for r in meta_rows if r.get("district")})

year_filter = st.sidebar.multiselect(
    "Election year",
    options=years,
    default=years[-1:] if years else []
)
office_filter = st.sidebar.multiselect("Office", options=offices, default=offices if offices else [])
district_filter = st.sidebar.multiselect("District", options=districts, default=districts if districts else [])

# Donor bucket filter used for name-based RPCs
donor_bucket_filter = st.sidebar.selectbox(
    "Donor bucket (overlap/history)",
    options=["All", "Entity", "Individual"],
    index=0,
)

p_donor_bucket = None if donor_bucket_filter == "All" else donor_bucket_filter

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
# Section 1: Candidate totals
# -----------------------------
st.subheader("Candidate totals (by donor bucket)")

totals_resp = sb.table("v_candidate_totals").select(
    "election_year,office,district,candidate,committee_name,committee_type,public_funding_requested,donor_bucket,txns,total_amount"
).execute()
totals_df = to_df(totals_resp.data)

if not totals_df.empty:
    if year_filter:
        totals_df = totals_df[totals_df["election_year"].isin(year_filter)]
    if office_filter:
        totals_df = totals_df[totals_df["office"].isin(office_filter)]
    if district_filter:
        totals_df = totals_df[totals_df["district"].isin(district_filter)]

    totals_df["candidate_display"] = totals_df["candidate"].fillna("").map(title_case_name)

    show_all = st.checkbox("Show all candidates", value=False)
    if not show_all and selected_candidate:
        totals_df = totals_df[totals_df["candidate"] == selected_candidate]

    totals_df = totals_df.sort_values(["total_amount"], ascending=False)

    # REMOVE election_year from display (per your request)
    display_cols = [
        "office","district","candidate_display",
        "donor_bucket","txns","total_amount",
        "committee_name","committee_type","public_funding_requested"
    ]
    st.dataframe(
        totals_df[display_cols],
        use_container_width=True,
        hide_index=True,
    )

    download_cols = [
        "office","district","candidate","donor_bucket","txns","total_amount",
        "committee_name","committee_type","public_funding_requested"
    ]
    download_button(
        totals_df[download_cols],
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
# Section 2: Name-based donor overlap (RPC)
# -----------------------------
st.subheader("Donor overlap (name-based)")
st.caption("Donors who gave to the selected candidate *and* at least one other candidate (matched by canonicalized donor name).")

if selected_candidate:
    rpc = sb.rpc(
        "get_name_overlap_for_candidate",
        {"p_candidate": selected_candidate, "p_donor_bucket": p_donor_bucket},
    ).execute()
    overlap_df = to_df(getattr(rpc, "data", None))

    if not overlap_df.empty:
        overlap_df["other_candidate_display"] = overlap_df["other_candidate"].fillna("").map(title_case_name)

        # Controls
        only_others = st.checkbox("Show only the 'other candidates' rows", value=True)
        if only_others:
            # this RPC already returns other_candidate rows, but keep checkbox in case function changes later
            pass

        overlap_df = overlap_df.sort_values(
            ["total_to_target", "total_to_other_candidate", "other_candidate"],
            ascending=[False, False, True],
        )

        st.dataframe(
            overlap_df[[
                "donor_bucket",
                "donor_name_canonical",
                "donor_name_variants",
                "total_to_target",
                "first_to_target",
                "last_to_target",
                "other_candidate_display",
                "total_to_other_candidate",
                "n_contributions",
                "first_to_other",
                "last_to_other",
            ]],
            use_container_width=True,
            hide_index=True,
        )

        download_button(
            overlap_df[[
                "donor_bucket",
                "donor_name_canonical",
                "donor_name_variants",
                "total_to_target",
                "first_to_target",
                "last_to_target",
                "other_candidate",
                "total_to_other_candidate",
                "n_contributions",
                "first_to_other",
                "last_to_other",
            ]],
            filename=f"name_overlap_{selected_candidate}_{datetime.now().date()}.csv",
            label="Download name overlap CSV",
        )

        if show_charts:
            # Top 25 overlap donors by total_to_target
            top = overlap_df.sort_values("total_to_target", ascending=False).head(25)
            c2 = (
                alt.Chart(top)
                .mark_bar()
                .encode(
                    x=alt.X("total_to_target:Q", title=f"Total to {selected_candidate}"),
                    y=alt.Y("donor_name_canonical:N", sort="-x", title="Top overlap donors"),
                    tooltip=["donor_name_canonical:N", "total_to_target:Q"],
                )
            )
            st.altair_chart(c2, use_container_width=True)
    else:
        st.info("No overlap found for this candidate (or RPC returned zero rows).")
else:
    st.warning("No candidates found. Populate v_candidates first.")


# -----------------------------
# Section 3: Donors to candidate + PRIOR history (before first donation to candidate)
# -----------------------------
st.subheader("Donors to candidate + prior history (before first donation)")

st.caption(
    "For each donor to the selected candidate, show which other candidates they supported *before* their first donation to the selected candidate."
)

if selected_candidate:
    # Default: flat table (best for dataframe)
    use_flat = st.checkbox("Use flat table (recommended)", value=True)

    if use_flat:
        rpc3 = sb.rpc(
            "get_prior_donor_history_flat",
            {"p_candidate": selected_candidate, "p_donor_bucket": p_donor_bucket},
        ).execute()
        prior_df = to_df(getattr(rpc3, "data", None))

        if not prior_df.empty:
            prior_df["other_candidate_display"] = prior_df["other_candidate"].fillna("").map(title_case_name)
            prior_df = prior_df.sort_values(
                ["amount_to_candidate_all_time", "total_to_other_candidate"],
                ascending=[False, False],
            )

            st.dataframe(
                prior_df[[
                    "donor_bucket",
                    "donor_name_canonical",
                    "donor_name_example",
                    "amount_to_candidate_all_time",
                    "first_to_candidate",
                    "last_to_candidate",
                    "other_candidate_display",
                    "total_to_other_candidate",
                    "n_to_other_candidate",
                    "first_to_other_candidate",
                    "last_to_other_candidate",
                ]],
                use_container_width=True,
                hide_index=True,
            )

            download_button(
                prior_df[[
                    "donor_bucket",
                    "donor_name_canonical",
                    "donor_name_example",
                    "amount_to_candidate_all_time",
                    "first_to_candidate",
                    "last_to_candidate",
                    "other_candidate",
                    "total_to_other_candidate",
                    "n_to_other_candidate",
                    "first_to_other_candidate",
                    "last_to_other_candidate",
                ]],
                filename=f"prior_history_flat_{selected_candidate}_{datetime.now().date()}.csv",
                label="Download prior history (flat) CSV",
            )
        else:
            st.info("No prior-history rows returned (either none exist, or RPC returned zero rows).")

    else:
        # Raw JSON version
        rpc3 = sb.rpc(
            "get_donors_prior_history_before_candidate",
            {"p_candidate": selected_candidate, "p_donor_bucket": p_donor_bucket},
        ).execute()
        prior_json_df = to_df(getattr(rpc3, "data", None))

        if not prior_json_df.empty:
            st.dataframe(
                prior_json_df[[
                    "donor_bucket",
                    "donor_name_canonical",
                    "donor_name_example",
                    "amount_to_candidate_all_time",
                    "first_to_candidate",
                    "last_to_candidate",
                    "other_candidate_history_prior",
                ]],
                use_container_width=True,
                hide_index=True,
            )
            download_button(
                prior_json_df,
                filename=f"prior_history_json_{selected_candidate}_{datetime.now().date()}.csv",
                label="Download prior history (JSON) CSV",
            )
        else:
            st.info("No prior-history rows returned (either none exist, or RPC returned zero rows).")
