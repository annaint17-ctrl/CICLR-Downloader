"""CICLR Full-Text Finder v3: Streamlit user interface.

This file is intentionally separate from retriever_v3.py, which implements
open-access discovery and byte-validated downloads.  The main entry point
app.py executes this file via runpy.run_path().
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="CICLR · Full-Text Finder v3", page_icon="📚", layout="wide")

try:
    import retriever_v3 as r
except ImportError as exc:
    st.error("Не найден retriever_v3.py. Он должен находиться рядом с app_v3.py в GitHub.")
    st.exception(exc)
    st.stop()


@st.cache_data(show_spinner=False)
def load_core_records():
    path = Path(__file__).resolve().parent / 'core_include.json'
    with path.open(encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, list) or not data:
        raise ValueError('core_include.json must be a non-empty list of records')
    return data


def get_field(data, *options):
    for option in options:
        val = data.get(option, None)
        if val is not None and not pd.isna(val):
            val = str(val).strip()
            if val and val.lower() not in ('nan', 'none', '<na>'):
                return val
    return ''


def read_upload(upload):
    if upload.name.lower().endswith('.csv'):
        df = pd.read_csv(upload, dtype=str, keep_default_na=False)
    else:
        df = pd.read_excel(upload, dtype=str).fillna('')
    df.columns = [str(col).strip().lower().replace('_', ' ') for col in df.columns]
    items, seen = [], set()
    for idx, (_, raw) in enumerate(df.iterrows(), start=1):
        row = raw.to_dict()
        rid = r.normalize_id(get_field(row, 'id', 'mr id', 'record id', 'master record id', 'record'))
        if not rid:
            # This app is designed for stable CICLR IDs; don't invent an MR ID.
            continue
        if rid in seen:
            continue
        seen.add(rid)
        items.append({
            'id': rid,
            'title': get_field(row, 'title', 'article title'),
            'doi': r.normalize_doi(get_field(row, 'doi')),
            'pmid': get_field(row, 'pmid', 'pubmed id'),
            'pmcid': get_field(row, 'pmcid'),
            'year': get_field(row, 'year', 'publication year'),
        })
    return items


def selected_report_rows(records, past_rows, existing_files, chosen, client, openalex_key,
                         xml_fallback, upgrade_xml, max_tries, search_repositories,
                         progress_bar, progress_status):
    """Process chosen records; keep report rows for all 340 original IDs."""
    rows_by_id = dict(past_rows)
    retrieved = {}
    for index, rec in enumerate(chosen, start=1):
        rid = rec['id']
        progress_status.write(f'Checking {index}/{len(chosen)}: {rid}')
        pdf_prior = (rid, 'pdf') in existing_files
        xml_prior = (rid, 'xml') in existing_files
        if pdf_prior:
            row = r.blank_row(rec)
            row.update({'status': 'Existing PDF (uploaded)', 'file_type': 'PDF',
                        'filename': rid + '.pdf',
                        'verification': 'Validated PDF supplied through earlier file upload',
                        'notes': 'Skipped downloading an existing PDF'})
        elif xml_prior and not upgrade_xml:
            row = r.blank_row(rec)
            row.update({'status': 'Existing XML (uploaded)', 'file_type': 'XML',
                        'filename': rid + '.xml',
                        'verification': 'Validated article full text supplied through earlier upload',
                        'notes': 'Skipped PDF upgrade at user request'})
        else:
            # For an existing XML, only try to recover PDF; no need to redownload XML.
            report, files = r.retrieve_record(
                client, rec, openalex_key=openalex_key,
                try_xml=(xml_fallback and not xml_prior),
                max_pdf_tries=max_tries, use_html=search_repositories)
            if 'pdf' in files:
                retrieved[(rid, 'pdf')] = files['pdf']
                row = report
            elif xml_prior:
                row = report
                row.update({'status': 'Existing XML (uploaded)', 'file_type': 'XML',
                            'filename': rid + '.xml',
                            'verification': 'Existing uploaded XML validated; PDF upgrade unsuccessful',
                            'notes': ' | '.join(filter(None, [report.get('notes', ''),
                                    'Existing XML retained; PDF not downloaded']))})
            elif 'xml' in files:
                retrieved[(rid, 'xml')] = files['xml']
                row = report
            else:
                row = report
        rows_by_id[rid] = row
        progress_bar.progress(index / len(chosen))
    return [rows_by_id[rec['id']] for rec in records], retrieved


st.title('CICLR · Full-Text Finder')
st.caption('v3 · Server-side retrieval · Official PMC archives + open-access repositories · Original MR IDs')
st.info('This app finds and downloads legally available full texts. It cannot bypass paywalls or log into a university library. Only received and validated files count as downloads.')

source = st.radio('Article source', ('Preloaded Core Include (340)', 'Upload Excel or CSV'), horizontal=True)
if source.startswith('Preloaded'):
    try:
        records = load_core_records()
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        st.error('Cannot load core_include.json. Check the app folder in GitHub.')
        st.exception(exc)
        st.stop()
else:
    uploaded_list = st.file_uploader('Upload a CICLR Excel or CSV with MR ID and DOI or PMID', type=['csv', 'xlsx'])
    records = []
    if uploaded_list is not None:
        try:
            records = read_upload(uploaded_list)
            if not records:
                st.warning('No MR IDs were identified. Check the ID column name and values.')
        except Exception as exc:
            st.error('Could not read this file: ' + str(exc))

if not records:
    st.stop()

st.write(f'**{len(records)} records loaded**')
start_col, size_col = st.columns(2)
start = int(start_col.number_input('Starting record (1-based)', min_value=1,
                                  max_value=len(records), value=1, step=1))
batch_size = int(size_col.number_input('Batch size', min_value=1,
                                      max_value=min(100, len(records)),
                                      value=min(25, len(records)), step=1))
chosen = records[start - 1:start - 1 + batch_size]
st.caption(f'Selected {len(chosen)} records: **{chosen[0]["id"]}** through **{chosen[-1]["id"]}**. Starting record is the position in the list, not the MR ID.')

with st.expander('Earlier ZIP / PDFs / XMLs (recommended for retries)', expanded=True):
    earlier_uploads = st.file_uploader(
        'Upload your previously saved ZIP, PDFs, or XMLs to skip files you already have',
        type=['zip', 'pdf', 'xml'], accept_multiple_files=True,
        help='Upload CICLR_Pilot_13_Existing_Files.zip here. Old files will not be re-downloaded.')
    previous_report = st.file_uploader(
        'Optional: upload an earlier cumulative tracker CSV to preserve previous batch statuses',
        type=['csv'], key='earlier_tracker')
    st.caption('Earlier ZIPs are read only for matching MR files; they are not uploaded to research APIs.')

xml_fallback = st.checkbox('Save full-text XML if PDF cannot be obtained', value=True)
upgrade_xml = st.checkbox('Try to upgrade existing XML-only articles to original PDF', value=True)
with st.expander('Advanced retrieval settings'):
    max_tries = int(st.slider('Maximum PDF links to try per article', 1, 12, 7))
    search_repositories = st.checkbox('Search open-access repository pages for PDFs', value=True)
    openalex_key = st.text_input('Optional OpenAlex API key (never put it in GitHub)', type='password', value='')

if st.button('Find and download full texts for selected records', type='primary'):
    with st.spinner('Checking earlier files and open-access sources. This may take several minutes...'):
        try:
            previously_uploaded = [(file.name, file.getvalue()) for file in earlier_uploads or []]
            existing, warnings = r.load_existing_files(previously_uploaded, records)
            for warning in warnings[:10]:
                st.warning(warning)
            past = r.parse_report_csv(previous_report.getvalue(), records) if previous_report else {
                rec['id']: r.blank_row(rec) for rec in records
            }
            progress = st.progress(0)
            status = st.empty()
            client = r.HTTPClient()
            report_rows, downloaded = selected_report_rows(
                records, past, existing, chosen, client, openalex_key, xml_fallback,
                upgrade_xml, max_tries, search_repositories, progress, status)
            chosen_rows = {row['id']: row for row in report_rows if row['id'] in {a['id'] for a in chosen}}
            n_pdf = sum(row['status'] in (r.STATUS_PDF, 'Existing PDF (uploaded)') for row in chosen_rows.values())
            n_xml = sum(row['status'] in (r.STATUS_XML, 'Existing XML (uploaded)') for row in chosen_rows.values())
            n_missing = len(chosen) - n_pdf - n_xml
            summary = (f'Finished: {n_pdf} PDF + {n_xml} full-text XML articles accounted for '
                       f'out of {len(chosen)} ({n_missing} still requiring full text). '
                       f'New downloads: {len(downloaded)}.')
            st.session_state['ciclr_v3_run'] = {
                'rows': report_rows,
                'selected_ids': [rec['id'] for rec in chosen],
                'zip': r.results_archive(downloaded, report_rows, [name for name, _ in previously_uploaded]),
                'tracker': r.csv_bytes(report_rows),
                'queue': r.library_queue(report_rows)[0],
                'summary': summary,
            }
            status.success(summary)
        except Exception as exc:
            st.error('Retrieval encountered an error. Your earlier article files remain unchanged.')
            st.exception(exc)

if 'ciclr_v3_run' in st.session_state:
    result = st.session_state['ciclr_v3_run']
    st.subheader('Retrieval results')
    st.success(result['summary'])
    selected_ids = set(result['selected_ids'])
    df = pd.DataFrame([row for row in result['rows'] if row.get('id') in selected_ids])
    display_columns = ['id', 'title', 'status', 'filename', 'verification', 'failed_attempts', 'article_links']
    st.dataframe(df[[col for col in display_columns if col in df.columns]],
                 use_container_width=True, hide_index=True)
    st.caption('The ZIP contains **new files from this run only**. Keep your previously downloaded ZIPs as well.')
    a, b, c = st.columns(3)
    a.download_button('Download NEW files ZIP', result['zip'],
                      file_name='CICLR_v3_New_Files.zip', mime='application/zip')
    b.download_button('Download CUMULATIVE tracker CSV', result['tracker'],
                      file_name='CICLR_v3_Cumulative_Tracker.csv', mime='text/csv')
    c.download_button('Download library-access queue CSV', result['queue'],
                      file_name='CICLR_v3_Library_Queue.csv', mime='text/csv')

st.caption('Academic research use · Verify article eligibility and figures/supplements before evidence extraction.')
