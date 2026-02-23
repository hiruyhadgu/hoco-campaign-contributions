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


def build_star_dot(subject_name: str, tied_df: pd.DataFrame, max_nodes: int = 25) -> str:
    """
    Build a star graph (Graphviz DOT) centered on the subject entity.
    Expects tied_df to contain:
      - tied_node_name (or donor name)
      - total_amount
      - explanation (used to label edge as person/address)
    """
    def esc(s: str) -> str:
        return (s or "").replace('"', '\\"')

    lines = [
        "digraph G {",
        "rankdir=LR;",
        "graph [splines=true, overlap=false];",
        'node [shape=box, style="rounded,filled", fillcolor="#f6f8fa"];',
        'edge [color="#57606a"];',
    ]

    center_id = "SUBJECT"
    lines.append(f'{center_id} [shape=oval, fillcolor="#dbeafe", label="{esc(subject_name)}"];')

    if tied_df is None or tied_df.empty:
        lines.append("}")
        return "\n".join(lines)

    plot_df = tied_df.copy()
    if "total_amount" in plot_df.columns:
        plot_df = plot_df.sort_values("total_amount", ascending=False)
    plot_df = plot_df.head(int(max_nodes))

    for i, r in plot_df.iterrows():
        name = str(r.get("tied_node_name", "") or "")
        amt = r.get("total_amount", None)
        expl = str(r.get("explanation", "") or "")

        node_id = f"N{i}"
        if amt is None or (isinstance(amt, float) and pd.isna(amt)):
            label = name
        else:
            try:
                label = f"{name}\\n${float(amt):,.0f}"
            except Exception:
                label = name

        lines.append(f'{node_id} [label="{esc(label)}"];')

        expl_low = expl.lower()
        if "shared person" in expl_low:
            edge_label = "person"
        elif "shared address" in expl_low:
            edge_label = "address"
        else:
            edge_label = ""

        lines.append(f'{center_id} -> {node_id} [label="{esc(edge_label)}"];')

    lines.append("}")
    return "\n".join(lines)


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

    if contributor_type_group:
        q = q.eq("contributor_type_group", contributor_type_group)

    resp = q.execute()
    return getattr(resp, "data", None) or []


# -----------------------------
# Atterbeary Registry views (used ONLY when Atterbeary is selected)
# -----------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_v_reg_subject_entities_in_scope():
    sb = get_supabase()
    return cf_table(sb, "v_reg_subject_entities_in_scope").select("*").execute().data or []


@st.cache_data(ttl=300, show_spinner=False)
def fetch_v_reg_entity_donations_in_scope():
    sb = get_supabase()
    return cf_table(sb, "v_reg_entity_donations_in_scope").select("*").execute().data or []


@st.cache_data(ttl=300, show_spinner=False)
def fetch_v_reg_subject_tied_donors_in_scope():
    sb = get_supabase()
    return cf_table(sb, "v_reg_subject_tied_donors_in_scope").select("*").execute().data or []


@st.cache_data(ttl=300, show_spinner=False)
def fetch_v_reg_subject_network_nodes():
    sb = get_supabase()
    return cf_table(sb, "v_reg_subject_network_nodes").select("*").execute().data or []


@st.cache_data(ttl=300, show_spinner=False)
def fetch_v_reg_subject_concentration_power():
    sb = get_supabase()
    return cf_table(sb, "v_reg_subject_concentration_power").select("*").execute().data or []


@st.cache_data(ttl=300, show_spinner=False)
def fetch_reg_contribution_all_registry():
    sb = get_supabase()
    return cf_table(sb, "reg_contribution").select("*").execute().data or []


# -----------------------------
# Campaign Finance Snapshot (formal "nutrition label")
# -----------------------------
def _safe_pct(n: float, d: float) -> float:
    if d and d != 0:
        return 100.0 * (n / d)
    return 0.0


@st.cache_data(ttl=300, show_spinner=False)
def fetch_candidate_donors_typed_all(candidate_last: str):
    sb = get_supabase()
    q = (
        sb.schema("cf")
        .table("v_donor_to_candidate_typed")
        .select(
            "contributor_name, contributor_type_group, state, total_amount, txn_count, first_txn, last_txn"
        )
        .eq("election_year", SCOPE_YEAR)
        .eq("office", SCOPE_OFFICE)
        .eq("seat", SCOPE_SEAT)
        .eq("candidate_last", candidate_last)
        .order("total_amount", desc=True)
        .limit(10000)
    )
    resp = q.execute()
    return getattr(resp, "data", None) or []


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_public_finance_donor_id():
    sb = get_supabase()
    try:
        resp = (
            sb.schema("cf")
            .table("donors_canonical")
            .select("id, contributor_name")
            .ilike("contributor_name", "howard public election fund")
            .limit(1)
            .execute()
        )
        rows = getattr(resp, "data", None) or []
        return rows[0]["id"] if rows else None
    except Exception:
        return None


@st.cache_data(ttl=300, show_spinner=False)
def fetch_small_dollar_depth_private(candidate_last: str, candidate_first: str):
    sb = get_supabase()

    cand_resp = (
        sb.schema("cf")
        .table("candidates")
        .select("id")
        .ilike("last_name", candidate_last)
        .ilike("first_name", candidate_first)
        .limit(1)
        .execute()
    )
    cand_rows = getattr(cand_resp, "data", None) or []
    if not cand_rows:
        return None
    candidate_id = cand_rows[0]["id"]

    cc_resp = (
        sb.schema("cf")
        .table("candidate_committees")
        .select("id")
        .eq("candidate_id", candidate_id)
        .limit(1000)
        .execute()
    )
    cc_rows = getattr(cc_resp, "data", None) or []
    committee_ids = [r["id"] for r in cc_rows if r.get("id") is not None]
    if not committee_ids:
        return None

    public_fund_id = fetch_public_finance_donor_id()

    q = (
        sb.schema("cf")
        .table("contributions")
        .select("amount")
        .in_("candidate_committee_id", committee_ids)
        .limit(10000)
    )
    if public_fund_id is not None:
        q = q.neq("canonical_donor_id", public_fund_id)

    resp = q.execute()
    rows = getattr(resp, "data", None) or []
    if not rows:
        return 0.0

    amounts = pd.Series([r.get("amount") for r in rows]).fillna(0).astype(float)
    total = int(amounts.shape[0])
    small = int((amounts <= 100.0).sum())
    return (100.0 * small / total) if total else 0.0


def compute_snapshot_metrics(
    candidate_last: str,
    candidate_first: str,
    totals_all_df: pd.DataFrame,
    by_type_all_df: pd.DataFrame,
):
    tdf = totals_all_df[totals_all_df["candidate_last"].fillna("").str.lower() == (candidate_last or "").lower()].copy()
    bdf = by_type_all_df[by_type_all_df["candidate_last"].fillna("").str.lower() == (candidate_last or "").lower()].copy()

    total_amount = float(tdf["total_amount"].fillna(0).sum()) if "total_amount" in tdf.columns else 0.0

    def _sum_group(group_name: str) -> float:
        if bdf.empty:
            return 0.0
        if "contributor_type_group" not in bdf.columns or "total_amount" not in bdf.columns:
            return 0.0
        return float(bdf.loc[bdf["contributor_type_group"] == group_name, "total_amount"].fillna(0).sum())

    amt_individual = _sum_group("Individual")
    amt_entity = _sum_group("Entity")
    amt_public = _sum_group("Public Financing")
    amt_unknown = _sum_group("Unknown")

    private_total = max(total_amount - amt_public, 0.0)

    public_share_pct = _safe_pct(amt_public, total_amount)
    entity_share_private_pct = _safe_pct(amt_entity, private_total)

    donor_rows = fetch_candidate_donors_typed_all(candidate_last)
    donor_df = to_df(donor_rows)

    if not donor_df.empty:
        if "contributor_type_group" in donor_df.columns:
            donor_private = donor_df[donor_df["contributor_type_group"].isin(["Individual", "Entity"])].copy()
        else:
            donor_private = donor_df.copy()
    else:
        donor_private = donor_df

    private_from_view = float(donor_private["total_amount"].fillna(0).sum()) if (not donor_private.empty and "total_amount" in donor_private.columns) else 0.0

    outsider_amt = 0.0
    if not donor_private.empty and "state" not in donor_private.columns or "total_amount" not in donor_private.columns:
        outsider_amt = 0.0
    elif not donor_private.empty:
        outsider_amt = float(
            donor_private.loc[donor_private["state"].fillna("").str.upper() != "MD", "total_amount"].fillna(0).sum()
        )
    outsider_share_private_pct = _safe_pct(outsider_amt, private_from_view)

    top10_amt = 0.0
    if not donor_private.empty and "total_amount" in donor_private.columns:
        top10_amt = float(
            donor_private.sort_values("total_amount", ascending=False)["total_amount"].head(10).fillna(0).sum()
        )
    top10_share_private_pct = _safe_pct(top10_amt, private_from_view)

    small_dollar_depth_pct = fetch_small_dollar_depth_private(candidate_last, candidate_first)

    return {
        "total_amount": total_amount,
        "small_dollar_depth_pct": small_dollar_depth_pct,
        "entity_share_private_pct": entity_share_private_pct,
        "public_share_pct": public_share_pct,
        "outsider_share_private_pct": outsider_share_private_pct,
        "top10_share_private_pct": top10_share_private_pct,
        "amt_individual": amt_individual,
        "amt_entity": amt_entity,
        "amt_public": amt_public,
        "amt_unknown": amt_unknown,
        "private_total": private_total,
    }


def render_campaign_finance_snapshot(
    candidate_last: str,
    candidate_first: str,
    totals_all_df: pd.DataFrame,
    by_type_all_df: pd.DataFrame,
):
    metrics = compute_snapshot_metrics(candidate_last, candidate_first, totals_all_df, by_type_all_df)

    total_amount = metrics["total_amount"]
    small_pct = metrics["small_dollar_depth_pct"]
    entity_pct = metrics["entity_share_private_pct"]
    public_pct = metrics["public_share_pct"]
    outsider_pct = metrics["outsider_share_private_pct"]
    top10_pct = metrics["top10_share_private_pct"]

    small_text = f"{small_pct:,.1f}%" if small_pct is not None else "—"

    st.markdown("### Campaign Finance Snapshot")
    st.markdown(
        f"""
<div style="padding: 12px 14px; border: 1px solid #d0d7de; background: #ffffff; border-radius: 12px;">
  <div style="font-size: 14px; font-weight: 800; margin-bottom: 8px;">
    Campaign Finance Snapshot (scope: {SCOPE_YEAR} • {SCOPE_OFFICE} • {SCOPE_SEAT})
  </div>

  <div style="display: grid; grid-template-columns: 1fr auto; gap: 6px 12px; font-size: 13px;">
    <div><b>Total raised</b></div><div>${total_amount:,.0f}</div>
    <div><b>Small-dollar depth</b> (share of donations ≤ $100)</div><div>{small_text}</div>
    <div><b>Entity share</b> (private $ only)</div><div>{entity_pct:,.1f}%</div>
    <div><b>Public financing share</b></div><div>{public_pct:,.1f}%</div>
    <div><b>Outsider share</b> (outside MD, private $ only)</div><div>{outsider_pct:,.1f}%</div>
    <div><b>Concentration</b> (Top-10 donors’ share of private $)</div><div>{top10_pct:,.1f}%</div>
  </div>

  <div style="margin-top: 10px; color: #57606a; font-size: 12px;">
    Notes: “Private $ only” excludes Howard Public Election Fund. “Outsider share” uses donor state from the donor-aggregated view.
  </div>
</div>
""",
        unsafe_allow_html=True,
    )


# -----------------------------
# Influence & Patterns tab helpers
# -----------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_candidate_entities_top(candidate_last: str, top_n: int):
    sb = get_supabase()
    q = (
        sb.schema("cf")
        .table("v_donor_to_candidate_typed")
        .select("contributor_name, contributor_type_group, contributor_type, city, state, total_amount, txn_count, first_txn, last_txn")
        .eq("election_year", SCOPE_YEAR)
        .eq("office", SCOPE_OFFICE)
        .eq("seat", SCOPE_SEAT)
        .eq("candidate_last", candidate_last)
        .eq("contributor_type_group", "Entity")
        .order("total_amount", desc=True)
        .limit(int(top_n))
    )
    resp = q.execute()
    return getattr(resp, "data", None) or []


@st.cache_data(ttl=300, show_spinner=False)
def fetch_candidate_state_totals(candidate_last: str):
    rows = fetch_candidate_donors_typed_all(candidate_last)
    df = to_df(rows)
    if df.empty or "state" not in df.columns or "total_amount" not in df.columns:
        return []
    df["state"] = df["state"].fillna("").str.upper().replace({"": "UNKNOWN"})
    out = (
        df.groupby("state", as_index=False)["total_amount"]
        .sum()
        .sort_values("total_amount", ascending=False)
    )
    return out.to_dict(orient="records")


# -----------------------------
# Bundler / fundraiser fingerprint RPC fetchers
# -----------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_candidate_daily_totals_rpc(candidate_last: str, candidate_first: str, limit: int = 60):
    sb = get_supabase()
    resp = cf_rpc(sb, "get_candidate_daily_totals", {
        "p_candidate_last": candidate_last,
        "p_candidate_first": candidate_first,
        "p_election_year": SCOPE_YEAR,
        "p_office": SCOPE_OFFICE,
        "p_seat": SCOPE_SEAT,
        "p_start_date": None,
        "p_end_date": None,
        "p_limit": int(limit),
    })
    return getattr(resp, "data", None) or []


@st.cache_data(ttl=300, show_spinner=False)
def fetch_candidate_contribs_on_date_rpc(candidate_last: str, candidate_first: str, txn_date: str, limit: int = 500):
    sb = get_supabase()
    resp = cf_rpc(sb, "get_candidate_contributions_on_date", {
        "p_candidate_last": candidate_last,
        "p_candidate_first": candidate_first,
        "p_txn_date": txn_date,
        "p_limit": int(limit),
    })
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
# Sidebar
# -----------------------------
st.sidebar.markdown(
    """
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
tab_overview, tab_overlap, tab_history, tab_influence, tab_registry, tab_notes = st.tabs(
    ["Overview", "Shared donors (overlap)", "Donor history", "Influence & Patterns", "Corporate concentration of power", "Notes / Methodology"]
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

        by_type_all = to_df(by_type_rows)

        if show_all_candidates:
            snap_rows = []
            all_candidates = (
                totals_df[["candidate_last", "candidate_first"]]
                .dropna()
                .drop_duplicates()
                .to_dict(orient="records")
            )

            for r in all_candidates:
                cl = r.get("candidate_last")
                cfst = r.get("candidate_first")
                if not cl or not cfst:
                    continue
                m = compute_snapshot_metrics(cl, cfst, totals_df, by_type_all)
                snap_rows.append({
                    "candidate": f"{title_case_name(cl)}, {title_case_name(cfst)}",
                    "total_raised": m["total_amount"],
                    "small_dollar_depth_pct": m["small_dollar_depth_pct"],
                    "entity_share_private_pct": m["entity_share_private_pct"],
                    "public_financing_share_pct": m["public_share_pct"],
                    "outsider_share_private_pct": m["outsider_share_private_pct"],
                    "top10_concentration_private_pct": m["top10_share_private_pct"],
                })

            snap_df = pd.DataFrame(snap_rows).sort_values("total_raised", ascending=False)

            if snap_df.empty:
                st.info("No snapshot metrics available.")
            else:
                st.markdown("### Campaign Finance Snapshot (all candidates)")
                st.dataframe(
                    snap_df,
                    use_container_width=True,
                    hide_index=True,
                )
                download_button(
                    snap_df,
                    filename=f"campaign_finance_snapshot_all_candidates_{SCOPE_YEAR}_{datetime.now().date()}.csv",
                    label="Download snapshot table CSV",
                )

        else:
            render_campaign_finance_snapshot(selected_last, selected_first, totals_df, by_type_all)

        if not show_all_candidates:
            st.markdown("### Top donors to selected candidate")

            top_n = st.slider("Show top N donors", min_value=10, max_value=200, value=50, step=10)

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
# Influence & Patterns tab
# -----------------------------
with tab_influence:
    st.subheader("Influence & Patterns")
    st.caption("Quick lenses for corporate/organized money, geography, and coordination fingerprints (scope-fixed).")

    st.markdown(
        """
<div style="padding: 10px 12px; border-left: 6px solid #d62728; background: #f6f8fa; border-radius: 10px;">
<b>Plain-English meaning:</b> These views help readers see “access signals” (entities), “outsider money” (geography),
and “coordination patterns” (fundraiser-style clustering) without making accusations.
</div>
""",
        unsafe_allow_html=True,
    )

    with st.expander("Corporate & special-interest (Entity donors)", expanded=True):
        top_n_entities = st.slider("Top entity donors", min_value=10, max_value=200, value=50, step=10, key="top_entities_slider")
        ent_rows = fetch_candidate_entities_top(selected_last, top_n_entities)
        ent_df = to_df(ent_rows)

        if ent_df.empty:
            st.info("No entity donors found for this candidate under the current scope.")
        else:
            show_cols = [c for c in [
                "contributor_name", "contributor_type", "city", "state",
                "total_amount", "txn_count", "first_txn", "last_txn"
            ] if c in ent_df.columns]
            st.dataframe(ent_df[show_cols], use_container_width=True, hide_index=True)

            download_button(
                ent_df[show_cols],
                filename=f"entity_donors_{selected_last}_{SCOPE_YEAR}_{datetime.now().date()}.csv",
                label="Download entity donors CSV",
            )

    with st.expander("Geography (state totals)", expanded=False):
        state_rows = fetch_candidate_state_totals(selected_last)
        state_df = to_df(state_rows)

        if state_df.empty:
            st.info("No geographic rows available (missing state or totals).")
        else:
            total_amt = float(state_df["total_amount"].fillna(0).sum()) if "total_amount" in state_df.columns else 0.0
            if total_amt:
                state_df["share_pct"] = state_df["total_amount"].fillna(0).apply(lambda x: _safe_pct(float(x), total_amt))
            st.dataframe(
                state_df[["state", "total_amount", "share_pct"]].sort_values("total_amount", ascending=False),
                use_container_width=True,
                hide_index=True,
            )

            if show_charts and not state_df.empty:
                chart = (
                    alt.Chart(state_df.sort_values("total_amount", ascending=False).head(25))
                    .mark_bar()
                    .encode(
                        x=alt.X("total_amount:Q", title="Total amount"),
                        y=alt.Y("state:N", sort="-x", title="State"),
                        tooltip=["state:N", "total_amount:Q", "share_pct:Q"],
                    )
                )
                st.altair_chart(chart, use_container_width=True)

            download_button(
                state_df,
                filename=f"state_totals_{selected_last}_{SCOPE_YEAR}_{datetime.now().date()}.csv",
                label="Download state totals CSV",
            )

    with st.expander("Bundler / fundraiser fingerprints (coordination patterns)", expanded=False):
        st.markdown("#### Spike days (highest total $ by date)")

        spike_limit = st.slider("How many spike days to show", 10, 120, 30, step=10, key="spike_days_limit")

        daily_rows = fetch_candidate_daily_totals_rpc(selected_last, selected_first, limit=spike_limit)
        daily_df = to_df(daily_rows)

        if daily_df.empty:
            st.info("No daily totals returned.")
        else:
            if "txn_date" in daily_df.columns:
                daily_df["txn_date"] = pd.to_datetime(daily_df["txn_date"]).dt.date

            st.dataframe(
                daily_df[["txn_date", "total_amount", "txn_count", "unique_donors", "max_txn_amount"]],
                use_container_width=True,
                hide_index=True,
            )

            if show_charts:
                chart = (
                    alt.Chart(daily_df)
                    .mark_bar()
                    .encode(
                        x=alt.X("total_amount:Q", title="Total amount (day)"),
                        y=alt.Y("txn_date:T", sort="-x", title="Date"),
                        tooltip=["txn_date:T", "total_amount:Q", "txn_count:Q", "unique_donors:Q", "max_txn_amount:Q"],
                    )
                )
                st.altair_chart(chart, use_container_width=True)

            chosen_date = st.selectbox(
                "Drill into a date",
                options=list(daily_df["txn_date"].astype(str)),
                index=0,
                key="drill_date_select",
            )

            st.markdown("#### Contributions on selected date")
            contrib_rows = fetch_candidate_contribs_on_date_rpc(selected_last, selected_first, chosen_date, limit=500)
            contrib_df = to_df(contrib_rows)

            if contrib_df.empty:
                st.info("No contributions returned for this date.")
            else:
                total_amt = float(contrib_df["amount"].fillna(0).sum()) if "amount" in contrib_df.columns else 0.0
                n_txn = int(contrib_df.shape[0])
                max_amt = float(contrib_df["amount"].fillna(0).max()) if "amount" in contrib_df.columns else 0.0

                near_max_count = int((contrib_df["amount"].fillna(0) >= (max_amt - 100)).sum()) if "amount" in contrib_df.columns else 0
                near_max_share = (100.0 * near_max_count / n_txn) if n_txn else 0.0

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Total on this date", f"${total_amt:,.0f}")
                c2.metric("Transactions", f"{n_txn:,}")
                c3.metric("Max contribution", f"${max_amt:,.0f}")
                c4.metric("Near-max share", f"{near_max_share:,.1f}%")

                st.caption(
                    "Interpretation note: clusters of many high-dollar donations on the same day are patterns commonly produced by hosted fundraisers/coordination. "
                    "This does not prove bundling."
                )

                show_cols = [c for c in [
                    "contributor_name",
                    "contributor_type_group",
                    "city",
                    "state",
                    "amount",
                    "contribution_type",
                    "fund_type",
                    "report",
                    "coordinated_contribution",
                ] if c in contrib_df.columns]

                st.dataframe(
                    contrib_df[show_cols].sort_values("amount", ascending=False),
                    use_container_width=True,
                    hide_index=True,
                )

                download_button(
                    contrib_df[show_cols],
                    filename=f"spike_day_{selected_last}_{chosen_date}.csv",
                    label="Download spike-day donor list CSV",
                )


# -----------------------------
# Corporate concentration of power tab
# -----------------------------
with tab_registry:
    st.subheader("Corporate concentration of power")

    # IMPORTANT: Gate the entire tab so Atterbeary registry data never appears for other candidates
    if (selected_last or "").strip().lower() != "atterbeary":
        st.info(
            "Note: Candidates using Maryland’s public financing program (CEF) are not allowed to accept corporate or PAC contributions. "
            "This tab currently displays Atterbeary-specific registry analysis only."
        )
        st.stop()

    st.markdown(
        """
<div style="padding: 10px 12px; border-left: 6px solid #003A65; background: #f6f8fa; border-radius: 10px;">
<b>Method (must-read):</b><br/>
<b>Cycle totals & ties</b>: contributions dated <b>2023-01-01</b> through <b>2026-12-31</b> (cycle_key = <b>2023_2026</b>).<br/>
<b>Concentration baseline (“power”)</b>: candidate total dollars dated <b>≥ 2025-01-01</b> (through <b>2026-12-31</b> unless extended).<br/>
</div>
""",
        unsafe_allow_html=True,
    )

    subj_df = to_df(fetch_v_reg_subject_entities_in_scope())
    tied_df = to_df(fetch_v_reg_subject_tied_donors_in_scope())
    conc_df = to_df(fetch_v_reg_subject_concentration_power())
    dons_df = to_df(fetch_v_reg_entity_donations_in_scope())
    net_nodes_df = to_df(fetch_v_reg_subject_network_nodes())
    contrib_df = to_df(fetch_reg_contribution_all_registry())

    if "entity_node_id" in dons_df.columns and "donor_node_id" not in dons_df.columns:
        dons_df = dons_df.rename(columns={"entity_node_id": "donor_node_id"})
    if "entity_node_id" in contrib_df.columns and "donor_node_id" not in contrib_df.columns:
        contrib_df = contrib_df.rename(columns={"entity_node_id": "donor_node_id"})

    if subj_df.empty:
        st.info("No subject entities in scope. Check cf.v_reg_subject_entities_in_scope.")
        st.stop()

    def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
        for c in candidates:
            if c in df.columns:
                return c
        return None

    name_col = _pick_col(subj_df, ["subject_entity_name", "subject_name", "entity_name", "display_name", "name"])
    id_col = _pick_col(subj_df, ["subject_entity_id", "subject_id", "entity_id", "node_id"])

    if not name_col or not id_col:
        st.error(
            "cf.v_reg_subject_entities_in_scope is missing expected columns. "
            f"Got columns: {list(subj_df.columns)}"
        )
        st.stop()

    subj_df = subj_df.sort_values(name_col)
    options = subj_df[name_col].astype(str).tolist()
    sel_name = st.selectbox("Select a subject entity", options=options, key="reg_subject_entity_view")

    subject_row = subj_df[subj_df[name_col].astype(str) == str(sel_name)].iloc[0]
    subject_id = subject_row[id_col]

    # 1) Direct donations (cycle)
    st.markdown("### 1) Direct donations (cycle)")

    direct = dons_df.copy()
    donor_id_col = _pick_col(direct, ["donor_node_id", "entity_node_id", "donor_id", "entity_id", "node_id"])
    if not donor_id_col:
        st.error(
            "cf.v_reg_entity_donations_in_scope is missing a donor id column. "
            f"Got columns: {list(direct.columns)}"
        )
        st.stop()

    if "candidate_key" in direct.columns:
        direct = direct[direct["candidate_key"] == "Atterbeary"]
    if "cycle_key" in direct.columns:
        direct = direct[direct["cycle_key"] == "2023_2026"]

    direct = direct[direct[donor_id_col] == subject_id]

    direct_total = float(direct["amount"].fillna(0).sum()) if not direct.empty else 0.0
    st.metric("Direct total (cycle)", f"${direct_total:,.2f}")

    if direct.empty:
        st.info("No direct donations found for this entity in the cycle window.")
    else:
        show_cols = [c for c in ["txn_date", "amount", "report", "contribution_type", "donor_name_as_reported"] if c in direct.columns]
        st.dataframe(direct.sort_values("txn_date")[show_cols], use_container_width=True, hide_index=True)

    # 2) Tied donors via hubs only
    st.markdown("### 2) Tied donors via evidence hubs (people + addresses only)")

    tied = tied_df.copy()
    if "subject_entity_id" in tied.columns:
        tied = tied[tied["subject_entity_id"] == subject_id].copy()
    else:
        st.error(f"Expected subject_entity_id in tied donors view. Columns: {list(tied.columns)}")
        st.stop()

    tied_total = float(tied["total_amount"].fillna(0).sum()) if not tied.empty else 0.0
    network_total_cycle = direct_total + tied_total
    st.metric("Network total (cycle, subject + tied donors)", f"${network_total_cycle:,.2f}")

    if tied.empty:
        st.info("No tied donors with in-cycle contributions found.")
    else:
        display_cols = [
            "tied_node_name",
            "tied_node_type",
            "total_amount",
            "donation_count",
            "first_date",
            "last_date",
            "explanation",
            "source_titles",
        ]
        display_cols = [c for c in display_cols if c in tied.columns]

        st.dataframe(
            tied.sort_values("total_amount", ascending=False)[display_cols],
            use_container_width=True,
            hide_index=True,
        )

    # 3) Concentration & coordination (power baseline)
    st.markdown("### 3) Concentration & coordination (power baseline)")
    conc = conc_df[conc_df["subject_entity_id"] == subject_id] if "subject_entity_id" in conc_df.columns else pd.DataFrame()
    if conc.empty:
        st.info("No concentration row found for this subject (power baseline).")
    else:
        r = conc.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("Candidate total (>= 2025-01-01)", f"${float(r['candidate_total_power']):,.2f}")
        c2.metric("Network total (>= 2025-01-01)", f"${float(r['network_total_power']):,.2f}")
        c3.metric("Network share of candidate total", f"{float(r['network_share_power_pct']):,.2f}%")

        c4, c5, c6 = st.columns(3)
        c4.metric("Same-day cluster total", f"${float(r['same_day_cluster_total_power']):,.2f}")
        c5.metric("Same-day cluster days", f"{int(r['same_day_cluster_days_power'])}")
        c6.metric("Max 3-day total", f"${float(r['max_3day_total_power']):,.2f}")

    # 4) Full ego-network contributions table (cycle)
    with st.expander("Show full contribution list for the entire ego-network (cycle)"):
        if net_nodes_df.empty or contrib_df.empty:
            st.info("Network nodes view or reg_contribution table not available.")
        else:
            nn = net_nodes_df[net_nodes_df["subject_entity_id"] == subject_id] if "subject_entity_id" in net_nodes_df.columns else pd.DataFrame()
            if nn.empty or "network_node_id" not in nn.columns:
                st.info("No network nodes found for this subject.")
            else:
                network_node_ids = nn["network_node_id"].dropna().tolist()

                tx = contrib_df.copy()
                if "candidate_key" in tx.columns:
                    tx = tx[tx["candidate_key"] == "Atterbeary"]
                if "cycle_key" in tx.columns:
                    tx = tx[tx["cycle_key"] == "2023_2026"]

                if "donor_node_id" not in tx.columns:
                    st.error(f"cf.reg_contribution missing donor_node_id. Columns: {list(tx.columns)}")
                    st.stop()

                tx = tx[tx["donor_node_id"].isin(network_node_ids)]

                if tx.empty:
                    st.info("No network transactions found in cycle window.")
                else:
                    show_cols = [c for c in ["txn_date", "donor_name_as_reported", "amount", "report", "contribution_type"] if c in tx.columns]
                    st.dataframe(tx.sort_values("txn_date")[show_cols], use_container_width=True, hide_index=True)
                    download_button(
                        tx[show_cols],
                        filename=f"atterbeary_registry_network_tx_{sel_name}_{datetime.now().date()}.csv",
                        label="Download network transactions CSV",
                    )

    # 5) Star visualization (figure)
    st.markdown("### 5) Star visualization (subject → tied donors)")
    if tied.empty:
        st.info("No tied donors to plot.")
    else:
        dot = build_star_dot(sel_name, tied, max_nodes=25)
        st.graphviz_chart(dot, use_container_width=True)

        # Keep the edge list table + download as an audit artifact
        star_cols = [
            "subject_entity_name",
            "tied_node_name",
            "tied_node_type",
            "total_amount",
            "first_date",
            "last_date",
            "explanation",
        ]
        star_cols = [c for c in star_cols if c in tied.columns]

        st.dataframe(tied[star_cols], use_container_width=True, hide_index=True)

        download_button(
            tied[star_cols],
            filename=f"atterbeary_registry_star_edges_{sel_name}_{datetime.now().date()}.csv",
            label="Download star edge list CSV",
        )


# -----------------------------
# Notes / Methodology tab
# -----------------------------
with tab_notes:
    st.subheader("Notes / Methodology")
    st.markdown(
        f"""- Scope is fixed to **{SCOPE_YEAR} • {SCOPE_OFFICE} • {SCOPE_SEAT}**.
- “Overlap” shows shared donors between campaigns within that scope.
- “Donor history” shows what else those donors funded across all loaded elections.
- “Campaign Finance Snapshot” uses:
  - **cf.v_candidate_totals** (total raised)
  - **cf.v_candidate_totals_by_contributor_type** (Individual vs Entity vs Public Financing totals)
  - **cf.v_donor_to_candidate_typed** (private outsider share + Top-10 concentration; excludes Public Financing)
  - **cf.contributions** (small-dollar depth: share of donations ≤ $100; excludes Howard Public Election Fund)
- “Bundler / fundraiser fingerprints” uses:
  - **cf.get_candidate_daily_totals** (spike days)
  - **cf.get_candidate_contributions_on_date** (drill-down donor list)"""
    )