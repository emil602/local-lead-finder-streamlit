
# Local Lead Finder (Google Places) — FULL Export + City/Subregions Batch

Includes:
- Single searches (Nearby or Text with optional bias)
- **Batch: City + Subregions** via Text Search (queries like: "restaurants near Sredets, Sofia")
- Details enrichment (address/phone/website)
- Email crawler
- Append & dedupe across searches (by place_id) with `search_origin`
- **FULL CSV export** (ALL columns). For private/internal use only.

## Run
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

Notes:
- Places API must be enabled. Geocoding is not required for batch subregions.
- Exporting Google data is restricted for redistribution. Keep exports private.
