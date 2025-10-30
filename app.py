# app.py — Local Lead Finder (Google Places)
# Branded login + logo in UI, organized layout, smart CSV export, Sheets-safe phones

import time
import re
from collections import Counter
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

st.set_page_config(page_title="Local Lead Finder", page_icon="🔎", layout="wide")

# ------------------------------------------------------------
# Utils: logo
# ------------------------------------------------------------
import os

def get_logo_path_or_url():
    """
    Returns path/URL to the logo.
    Priority:
      1) local 'logo.png' in repo root
      2) st.secrets['LOGO_URL'] if provided
    """
    local = os.path.join(os.path.dirname(__file__), "logo.png")
    if os.path.exists(local):
        return local
    return st.secrets.get("LOGO_URL", None)

# ------------------------------------------------------------
# Centered password gate (no colors changed)
# ------------------------------------------------------------
APP_PASSWORD = st.secrets.get("APP_PASSWORD", "")

def password_gate():
    st.markdown("""
    <style>
      .gate-wrap {min-height: 75vh; display:flex; align-items:center; justify-content:center;}
      .gate-card {background:#ffffff; border-radius:14px; padding:32px; box-shadow:0 10px 30px rgba(0,0,0,.06); max-width:460px; width:100%;}
      .gate-title {margin:0 0 8px 0; text-align:center;}
      .gate-sub {margin:0 0 18px 0; color:#666; text-align:center;}
      .gate-foot {margin-top:8px; font-size:12px; color:#888; text-align:center;}
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="gate-wrap"><div class="gate-card">', unsafe_allow_html=True)
    # Logo (centered)
    logo_src = get_logo_path_or_url()
    if logo_src:
        st.image(logo_src, use_column_width=True)

    st.markdown('<h3 class="gate-title">🔐 Local Lead Finder</h3>', unsafe_allow_html=True)
    st.markdown('<p class="gate-sub">Enter your access password to continue.</p>', unsafe_allow_html=True)

    with st.form("login_form", clear_on_submit=False):
        pw = st.text_input("App password", type="password", placeholder="••••••••")
        col_a, col_b = st.columns([1,1])
        ok = col_a.form_submit_button("Unlock")
        if ok:
            if pw == APP_PASSWORD:
                st.session_state["auth_ok"] = True
                st.rerun()
            else:
                st.error("Incorrect password. Try again.")

    st.markdown('<p class="gate-foot">© Home Comfort Marketing Group</p></div></div>', unsafe_allow_html=True)

if APP_PASSWORD and not st.session_state.get("auth_ok"):
    password_gate()
    st.stop()

# ------------------------------------------------------------
# Constants & endpoints
# ------------------------------------------------------------
PLACES_NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
PLACES_TEXT_URL   = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
HEADERS  = {"User-Agent": "LocalLeadFinder/1.0 (+local)"}
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.IGNORECASE)

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _safe_slug(s: str, maxlen=40):
    if not s:
        return "untitled"
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s[:maxlen] or "untitled"

def _guess_city_from_origin(df: pd.DataFrame):
    if "search_origin" not in df.columns or df["search_origin"].empty:
        return ""
    vals = df["search_origin"].dropna().astype(str).tolist()
    cities = []
    for v in vals[:800]:
        if ">" in v:
            cities.append(v.split(">", 1)[0].strip())
        elif ":" in v:
            q = v.split(":", 1)[1]
            if "," in q:
                cities.append(q.split(",")[-1].strip())
    if not cities:
        return ""
    return Counter(cities).most_common(1)[0][0]

def google_places_nearby(api_key: str, lat: float, lng: float, radius_m: int,
                         keyword: str = "", place_type: str = ""):
    params = {"key": api_key, "location": f"{lat},{lng}", "radius": radius_m}
    if keyword.strip():
        params["keyword"] = keyword.strip()
    if place_type.strip():
        params["type"] = place_type.strip()
    results, token, pages = [], None, 0
    while True:
        if token:
            params["pagetoken"] = token
            time.sleep(2)
        r = requests.get(PLACES_NEARBY_URL, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
        results.extend(data.get("results", []))
        token = data.get("next_page_token")
        pages += 1
        if not token or pages >= 3:
            break
    return results

def google_places_text_with_bias(api_key: str, query: str, lat: float | None, lng: float | None, radius_m: int | None):
    params = {"key": api_key, "query": query}
    if lat is not None and lng is not None and radius_m:
        params["location"] = f"{lat},{lng}"
        params["radius"] = radius_m
    results, token, pages = [], None, 0
    while True:
        if token:
            params["pagetoken"] = token
            time.sleep(2)
        r = requests.get(PLACES_TEXT_URL, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
        results.extend(data.get("results", []))
        token = data.get("next_page_token")
        pages += 1
        if not token or pages >= 3:
            break
    return results

def google_places_text_simple(api_key: str, query: str):
    return google_places_text_with_bias(api_key, query, None, None, None)

def google_place_details(api_key: str, place_id: str):
    fields = [
        "place_id","name","formatted_address","international_phone_number",
        "website","url","geometry/location","rating","user_ratings_total","types"
    ]
    params = {"key": api_key, "place_id": place_id, "fields": ",".join(fields)}
    r = requests.get(PLACE_DETAILS_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json().get("result", {})

def parse_places(results: list, origin_label: str = "") -> pd.DataFrame:
    rows = []
    for res in results:
        loc = (res.get("geometry") or {}).get("location") or {}
        rows.append({
            "place_id": res.get("place_id"),
            "name": res.get("name", ""),
            "address": res.get("formatted_address") or res.get("vicinity", ""),
            "lat": loc.get("lat"),
            "lng": loc.get("lng"),
            "rating": res.get("rating"),
            "types": ",".join(res.get("types", [])),
            "phone": "",
            "website": "",
            "google_maps_url": "",
            # user-enrichment
            "status": "", "contact_name": "", "email": "", "owner": "",
            "deal_value": "", "last_contacted": "", "notes": "",
            "search_origin": origin_label or ""
        })
    return pd.DataFrame(rows)

def enrich_with_details(api_key: str, df: pd.DataFrame, max_rows: int = 200) -> pd.DataFrame:
    if df.empty:
        return df
    limit = min(max_rows, len(df))
    progress = st.progress(0)
    for i in range(limit):
        pid = df.at[i, "place_id"]
        if not pid:
            continue
        try:
            d = google_place_details(api_key, pid)
            df.at[i, "address"] = d.get("formatted_address", df.at[i, "address"])
            df.at[i, "phone"] = d.get("international_phone_number", "")
            df.at[i, "website"] = d.get("website", "")
            df.at[i, "google_maps_url"] = d.get("url", "")
        except Exception:
            pass
        time.sleep(0.12)
        progress.progress(int((i + 1) / limit * 100))
    progress.empty()
    return df

def extract_emails_from_html(html: str):
    emails = set(EMAIL_RE.findall(html or ""))
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        if a["href"].startswith("mailto:"):
            addr = a["href"].split("mailto:")[1].split("?")[0]
            if addr:
                emails.add(addr.strip())
    return emails

def crawl_emails_from_site(website: str, max_pages: int = 3, timeout: int = 15):
    if not website:
        return set()
    if not urlparse(website).scheme:
        website = "http://" + website
    visited, queue, emails = set(), [website], set()
    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if "text/html" not in r.headers.get("Content-Type", ""):
                continue
            html = r.text
        except Exception:
            continue
        emails |= extract_emails_from_html(html)
        time.sleep(0.25)
    clean = set()
    for e in emails:
        e2 = e.strip().strip(";,.:()[]{}<>")
        if len(e2) > 5 and "@" in e2 and "." in e2:
            clean.add(e2)
    return clean

def enrich_emails_via_crawl(df: pd.DataFrame, max_rows: int = 50, max_pages_per_site: int = 3) -> pd.DataFrame:
    if df.empty:
        return df
    limit = min(max_rows, len(df))
    progress = st.progress(0)
    for i in range(limit):
        site = df.at[i, "website"] if "website" in df.columns else ""
        current_email = df.at[i, "email"] if "email" in df.columns else ""
        if site and not current_email:
            try:
                emails = crawl_emails_from_site(site, max_pages=max_pages_per_site)
                if emails:
                    df.at[i, "email"] = "; ".join(sorted(emails))
            except Exception:
                pass
        progress.progress(int((i + 1) / limit * 100))
        time.sleep(0.15)
    progress.empty()
    return df

def append_and_dedupe(master: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if master is None or master.empty:
        return new.reset_index(drop=True)
    if new is None or new.empty:
        return master.reset_index(drop=True)
    out = pd.concat([master, new], ignore_index=True)
    out = out.drop_duplicates(subset="place_id", keep="first").reset_index(drop=True)
    return out

def full_export(df: pd.DataFrame) -> pd.DataFrame:
    return df.copy()

# ------------------------------------------------------------
# Sidebar
# ------------------------------------------------------------
with st.sidebar:
    # Logo in sidebar (small)
    logo_src = get_logo_path_or_url()
    if logo_src:
        st.image(logo_src, use_column_width=True)
    st.header("Settings")
    api_key = st.text_input(
        "Google API Key",
        type="password",
        value=st.secrets.get("GOOGLE_API_KEY", "")
    )
    st.caption("Attribution: © Google — Data shown is provided by Google and used under the Maps Platform Terms.")

# ------------------------------------------------------------
# Session
# ------------------------------------------------------------
if "results_df" not in st.session_state:
    st.session_state["results_df"] = pd.DataFrame()
if "last_keyword" not in st.session_state:
    st.session_state["last_keyword"] = "leads"

# Header title (keeps your colors/theme)
st.markdown("## 🔎 Local Lead Finder")

# ------------------------------------------------------------
# Tabs
# ------------------------------------------------------------
tab_search, tab_results = st.tabs(["🔍 Search", "📊 Results"])

with tab_search:
    c1, c2 = st.columns([1,1])

    with c1:
        st.subheader("Single Search")
        mode = st.radio("Mode", ["Nearby (lat/lng + radius)", "Text search (query + optional bias)"])
        append_toggle = st.checkbox("Append to existing results", value=True)

        if mode == "Nearby (lat/lng + radius)":
            lat = st.number_input("Latitude", value=42.6977, format="%.6f")
            lng = st.number_input("Longitude", value=23.3219, format="%.6f")
            radius_m = st.slider("Radius (meters)", 100, 50000, 3000)
            keyword = st.text_input("Keyword (optional)", value="restaurant")
            place_type = st.text_input("Place type (optional)", value="")
        else:
            query = st.text_input("Query", value="restaurants near Sredets, Sofia")
            bias_lat = st.number_input("Bias Latitude (optional)", value=0.0, format="%.6f")
            bias_lng = st.number_input("Bias Longitude (optional)", value=0.0, format="%.6f")
            radius_bias_m = st.slider("Bias Radius (meters, optional)", 0, 50000, 0, help="Leave 0 to omit bias.")

        run_btn = st.button("Run single search")

    with c2:
        st.subheader("Batch: City + Subregions")
        st.caption("Runs queries like '<keyword> near <Subregion>, <City>' for each subregion.")
        bm_city = st.text_input("City", value="Sofia")
        bm_subregions_txt = st.text_area(
            "Subregions (one per line)",
            value="""Sredets
Krasno Selo
Vazrazhdane
Oborishte
Serdika
Poduene
Slatina
Lyulin
Mladost
Lozenets
Kremikovtsi
Nadezhda
Ilinden
Vrabnitsa
Ovcha Koupel
Studentski grad
Izgrev
Bankya
Pancharevo
Vitosha
Krasna Polyana
Iskar
Novi Iskar
Dragalevtsi"""
        )
        bm_keyword = st.text_input("Batch keyword (optional)", value="restaurant")
        run_city_subregions_btn = st.button("Run City + Subregions batch")

    # Execute single
    if run_btn:
        if not api_key:
            st.error("Please enter your Google API key.")
        else:
            with st.spinner("Searching Google Places…"):
                try:
                    if mode == "Nearby (lat/lng + radius)":
                        raw = google_places_nearby(api_key, lat, lng, int(radius_m), keyword, place_type)
                        new_df = parse_places(raw, origin_label=f"Nearby@{lat:.4f},{lng:.4f}")
                        st.session_state["last_keyword"] = keyword or "leads"
                    else:
                        if radius_bias_m and bias_lat and bias_lng:
                            raw = google_places_text_with_bias(api_key, query, bias_lat, bias_lng, int(radius_bias_m))
                        else:
                            raw = google_places_text_simple(api_key, query)
                        new_df = parse_places(raw, origin_label=f"Text:{query}")
                        guessed_kw = query.split(" near ")[0].strip() if " near " in query else query.split(",")[0].strip()
                        st.session_state["last_keyword"] = guessed_kw or "leads"
                except Exception as e:
                    st.error(f"API error: {e}")
                    new_df = pd.DataFrame()

            if new_df.empty:
                st.warning("No results found. Try adjusting your inputs.")
            else:
                if append_toggle:
                    st.session_state["results_df"] = append_and_dedupe(st.session_state["results_df"], new_df)
                else:
                    st.session_state["results_df"] = new_df
                st.success(f"Now tracking {len(st.session_state['results_df'])} unique places.")

    # Execute batch
    if run_city_subregions_btn:
        if not api_key:
            st.error("Please enter your Google API key.")
        else:
            subs = [s.strip() for s in bm_subregions_txt.splitlines() if s.strip()]
            if not bm_city or not subs:
                st.warning("Please provide a City and at least one Subregion.")
            else:
                all_new = pd.DataFrame()
                progress = st.progress(0)
                for i, sub in enumerate(subs, start=1):
                    q = f"{bm_keyword or ''} near {sub}, {bm_city}".strip()
                    try:
                        chunk = google_places_text_simple(api_key, q)
                        label = f"{bm_city} > {sub}"
                        df_chunk = parse_places(chunk, origin_label=label)
                        all_new = append_and_dedupe(all_new, df_chunk)
                    except Exception as e:
                        st.warning(f"Error searching '{q}': {e}")
                    progress.progress(int(i / max(1, len(subs)) * 100))
                    time.sleep(0.2)
                progress.empty()
                if all_new.empty:
                    st.warning("Batch finished, but no results returned. Try different keyword or subregions.")
                else:
                    st.session_state["results_df"] = append_and_dedupe(st.session_state["results_df"], all_new)
                    st.session_state["last_keyword"] = bm_keyword or "leads"
                    st.success(f"Batch complete. Now tracking {len(st.session_state['results_df'])} unique places.")

with tab_results:
    df = st.session_state["results_df"]
    if df.empty:
        st.info("Run a search or batch to see results here.")
    else:
        st.subheader("Results")

        # Summary
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Places", len(df))
        c2.metric("With Website", int(df["website"].astype(str).str.strip().ne("").sum()) if "website" in df.columns else 0)
        c3.metric("Unique Types", len(set(",".join(df.get("types", pd.Series([], dtype=str))).split(","))) if "types" in df.columns else 0)

        # Pagination
        total_rows = len(df)
        page_size = st.slider("Rows per page", 10, 200, 20, step=10)
        total_pages = max(1, (total_rows + page_size - 1) // page_size)
        if "page" not in st.session_state:
            st.session_state["page"] = 1
        st.number_input("Page", 1, total_pages, key="page")
        start = (st.session_state["page"] - 1) * page_size
        end = start + page_size

        # Editable table
        view_cols = [
            "name","address","phone","website","email","lat","lng","rating","types",
            "status","contact_name","owner","deal_value","last_contacted","notes","search_origin"
        ]
        existing_cols = [c for c in view_cols if c in df.columns]
        page_df = df.iloc[start:end].copy()
        st.markdown("#### Edit inline")
        edited = st.data_editor(page_df[existing_cols], num_rows="dynamic", use_container_width=True, key="editor")
        for col in existing_cols:
            st.session_state["results_df"].iloc[start:end, st.session_state["results_df"].columns.get_loc(col)] = edited[col].values

        # Enrichment: details
        with st.expander("Details Enrichment (address, phone, website)"):
            st.caption("Fetches Place Details for the first N rows (uses additional API calls).")
            max_rows_details = st.number_input("Max rows to enrich", 10, 2000, 200, step=10)
            if st.button("Fetch address, phone & website"):
                st.session_state["results_df"] = enrich_with_details(
                    st.secrets.get("GOOGLE_API_KEY","") or st.session_state.get("api_key",""),
                    st.session_state["results_df"], int(max_rows_details)
                )
                st.success("Details enrichment complete.")

        # Enrichment: emails
        with st.expander("Email Enrichment (crawl websites for emails)"):
            st.caption("Crawls each website (limited pages) and extracts email addresses.")
            max_rows_email = st.number_input("Max rows to process", 10, 500, 50, step=10)
            max_pages_site = st.slider("Max pages per site", 1, 10, 3, step=1)
            if st.button("Fetch emails from websites"):
                st.session_state["results_df"] = enrich_emails_via_crawl(
                    st.session_state["results_df"], int(max_rows_email), int(max_pages_site)
                )
                st.success("Email enrichment complete.")

        # Export
        st.markdown("### Export — FULL CSV (all columns)")
        export_df = full_export(st.session_state["results_df"]).copy()

        # Sheets-safe phone
        if "phone" in export_df.columns:
            def _fix_phone(v):
                if isinstance(v, str):
                    v = v.strip()
                    if v.startswith("+") or v.startswith("="):
                        return f"'{v}"
                return v
            export_df["phone"] = export_df["phone"].apply(_fix_phone)

        city = _guess_city_from_origin(export_df) or "mixed"
        keyword_for_name = st.session_state.get("last_keyword", "leads")
        stamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        fname = f"leads_{_safe_slug(city)}_{_safe_slug(keyword_for_name)}_{stamp}.csv"

        csv_bytes = export_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download FULL CSV", csv_bytes, file_name=fname, mime="text/csv")

# Footer
st.markdown("<hr>", unsafe_allow_html=True)
st.caption("Built by Home Comfort Marketing Group")