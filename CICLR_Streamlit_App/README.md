# CICLR Full-Text Finder (server-side)

A Streamlit app that checks OpenAlex and Europe PMC for legally accessible full-text PDFs, attempts to download them server-side, validates PDF signatures, and produces a ZIP of successful downloads and a CSV report.

## Deploy (Streamlit Community Cloud)
1. Create a new **private or public** GitHub repository. Streamlit Community Cloud deployment may require a public repository depending on your account/plan. The bundled citation data will be stored in that repository; remove it if you prefer not to publish it.
2. Upload `app.py`, `core_include.json`, and `requirements.txt` into the repository root.
3. Visit https://share.streamlit.io/ and choose **Create app**, select your repository, branch `main`, and entrypoint `app.py`.
4. Open the deployed URL, select the first 25 records, and click **Find and download PDFs**.
5. Save the resulting ZIP and CSV.

**Notes:** No guarantee of full-text availability. Publisher URLs may deny server requests (403), rate-limit, or require institutional login. Download success is counted only for validated PDFs. Uploaded Excel/CSV files are processed in memory during the session; they are not deliberately saved. Avoid putting confidential data in a public GitHub repository. Larger batches can exceed hosting time or memory limits; start with 25.
