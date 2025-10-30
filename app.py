# app.py
# Local Lead Finder (Google Places)
# Nearby/Text Search + Batch (City + Subregions via Text Search) + Details Enrichment + Email Crawler
# Append & Dedupe + Pagination + FULL Export (ALL columns)
# -----------------------------------------------------------------------------------------------
# COMPLIANCE NOTE:
# - Exporting Google-sourced data is generally restricted for redistribution. This build exports ALL fields
#   for your personal/internal use. Do not redistribute or resell Google data.
#
# RUN:
#   pip install -r requirements.txt
#   streamlit run app.py

import time
import re
import requests
import pandas as pd
import streamlit as st
from urllib.parse import urlparse
from bs4 import BeautifulSoup

st.set_page_config(page_title="Local Lead Finder (Google Places)", layout="wide")

PLACES_NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
PLACES_TEXT_URL   = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

HEADERS  = {"User-Agent": "LocalLeadFinder/1.0 (+local)"}
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.IGNORECASE)

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
            "status": "",
            "contact_name": "",
            "email": "",
            "owner": "",
            "deal_value": "",
            "last_contacted": "",
            "notes": "",
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
        href = a["href"]
        if href.startswith("mailto:"):
            addr = href.split("mailto:")[1].split("?")[0]
            if addr:
                emails.add(addr.strip())
    return emails

def crawl_emails_from_site(website: str, max_pages: int = 3, timeout: int = 15):
    if not website:
        return set()
    if not urlparse(website).scheme:
        website = "http://" + website

    visited = set()
    queue = [website]
    emails = set()

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

with st.sidebar:
    st.header("API key")
    api_key = st.text_input("Google API Key", type="password")

    st.header("Single search")
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

    st.markdown("---")
    st.header("Batch mode (City + Subregions via Text Search)")
    st.caption("Runs queries like '<keyword> near <Subregion>, <City>' for each subregion, and merges results.")
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

st.title("Local Lead Finder (Google Places) — FULL Export")
st.caption("Powered by Google • FULL CSV export is for private/internal use only.")

if "results_df" not in st.session_state:
    st.session_state["results_df"] = pd.DataFrame()

if run_btn:
    if not api_key:
        st.error("Please enter your Google API key.")
    else:
        with st.spinner("Searching Google Places…"):
            try:
                if mode == "Nearby (lat/lng + radius)":
                    raw = google_places_nearby(api_key, lat, lng, int(radius_m), keyword, place_type)
                    new_df = parse_places(raw, origin_label=f"Nearby@{lat:.4f},{lng:.4f}")
                else:
                    if radius_bias_m and bias_lat and bias_lng:
                        raw = google_places_text_with_bias(api_key, query, bias_lat, bias_lng, int(radius_bias_m))
                    else:
                        raw = google_places_text_simple(api_key, query)
                    new_df = parse_places(raw, origin_label=f"Text:{query}")
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

if run_city_subregions_btn:
    if not api_key:
        st.error("Please enter your Google API key.")
    else:
        subregions = [s.strip() for s in bm_subregions_txt.splitlines() if s.strip()]
        if not bm_city or not subregions:
            st.warning("Please provide a City and at least one Subregion.")
        else:
            all_new = pd.DataFrame()
            progress = st.progress(0)
            for i, sub in enumerate(subregions, start=1):
                query_text = f"{bm_keyword or ''} near {sub}, {bm_city}".strip()
                try:
                    chunk = google_places_text_simple(api_key, query_text)
                    label = f"{bm_city} > {sub}"
                    df_chunk = parse_places(chunk, origin_label=label)
                    all_new = append_and_dedupe(all_new, df_chunk)
                except Exception as e:
                    st.warning(f"Error searching '{query_text}': {e}")
                progress.progress(int(i / max(1, len(subregions)) * 100))
                time.sleep(0.2)
            progress.empty()

            if all_new.empty:
                st.warning("Batch finished, but no results returned. Try different keyword or subregions.")
            else:
                st.session_state["results_df"] = append_and_dedupe(st.session_state["results_df"], all_new)
                st.success(f"Batch complete. Now tracking {len(st.session_state['results_df'])} unique places.")

df = st.session_state["results_df"]
if not df.empty:
    st.markdown("#### Results (in-app view)")
    total_rows = len(df)
    page_size = st.sidebar.slider("Rows per page", 10, 200, 20, step=10, key="page_size")
    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    if "page" not in st.session_state:
        st.session_state["page"] = 1
    st.sidebar.number_input("Page", 1, total_pages, key="page")

    start = (st.session_state["page"] - 1) * page_size
    end = start + page_size

    view_cols = [
        "name", "address", "phone", "website", "email", "lat", "lng", "rating", "types",
        "status", "contact_name", "owner", "deal_value", "last_contacted", "notes", "search_origin"
    ]
    existing_cols = [c for c in view_cols if c in df.columns]

    page_df = df.iloc[start:end].copy()
    edited = st.data_editor(page_df[existing_cols], num_rows="dynamic", use_container_width=True, key="editor")
    for col in existing_cols:
        st.session_state["results_df"].iloc[start:end, st.session_state["results_df"].columns.get_loc(col)] = edited[col].values

    with st.expander("Details (address, phone, website)"):
        st.caption("Fetches Place Details for the first N rows (uses additional API calls).")
        max_rows_details = st.number_input("Max rows to enrich", 10, 2000, 200, step=10)
        if st.button("Fetch address, phone & website"):
            st.session_state["results_df"] = enrich_with_details(api_key, st.session_state["results_df"], int(max_rows_details))
            st.success("Details enrichment complete.")

    with st.expander("Email enrichment (website crawler)"):
        st.caption("Crawls each website (limited pages) and extracts email addresses.")
        max_rows_email = st.number_input("Max rows to process", 10, 500, 50, step=10)
        max_pages_site = st.slider("Max pages per site", 1, 10, 3, step=1)
        if st.button("Fetch emails from websites"):
            st.session_state["results_df"] = enrich_emails_via_crawl(st.session_state["results_df"], int(max_rows_email), int(max_pages_site))
            st.success("Email enrichment complete.")

    st.markdown("### Export — FULL CSV (all columns)")
    export_df = df.copy()
    csv_bytes = export_df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download FULL CSV", csv_bytes, file_name="leads_full.csv", mime="text/csv")

    st.markdown("---")
    st.caption("Attribution: © Google — Data shown is provided by Google and used under the Maps Platform Terms.")
else:
    st.info("Add your API key and run a single search or a City + Subregions batch.")
