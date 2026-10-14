from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

import retriever_v4 as r

st.set_page_config(page_title='CICLR · Full-Text Finder v4', page_icon='📚', layout='wide')


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
    for _, raw in df.iterrows():
        row = raw.to_dict()
        rid = r.normalize_id(get_field(row, 'id', 'mr id', 'record id', 'master record id', 'record'))
        if not rid or rid in seen:
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


def process_selected(records, past_rows, existing_files, chosen, client, openalex_key,
                     contact_email, xml_fallback, upgrade_xml, max_tries,
                     search_repositories, progress_bar, progress_status):
    rows_by_id = dict(past_rows)
    retrieved = {}
    upgraded_ids = []

    for index, rec in enumerate(chosen, start=1):
        rid = rec['id']
        progress_status.write(f'Checking {index}/{len(chosen)}: {rid}')
        pdf_prior = (rid, 'pdf') in existing_files
        xml_prior = (rid, 'xml') in existing_files

        if pdf_prior:
            row = r.blank_row(rec)
            row.update({
                'status': 'Existing PDF (uploaded)', 'file_type': 'PDF',
                'filename': rid + '.pdf',
                'verification': 'Validated PDF supplied through earlier file upload',
                'notes': 'Existing PDF retained; network retrieval skipped'
            })
        elif xml_prior and not upgrade_xml:
            row = r.blank_row(rec)
            row.update({
                'status': 'Existing XML (uploaded)', 'file_type': 'XML',
                'filename': rid + '.xml',
                'verification': 'Validated article full text supplied through earlier upload',
                'notes': 'Existing XML retained; PDF upgrade disabled'
            })
        else:
            # Existing XML: retry PDF only. New/missing record: PDF, then XML fallback.
            report, files = r.retrieve_record(
                client, rec,
                openalex_key=openalex_key,
                try_xml=(xml_fallback and not xml_prior),
                max_pdf_tries=max_tries,
                use_html=search_repositories,
                contact_email=contact_email,
            )
            if 'pdf' in files:
                retrieved[(rid, 'pdf')] = files['pdf']
                row = report
                if xml_prior:
                    upgraded_ids.append(rid)
                    row['notes'] = ' | '.join(filter(None, [row.get('notes', ''),
                        'Existing XML upgraded to PDF in this run']))
            elif xml_prior:
                row = report
                row.update({
                    'status': 'Existing XML (uploaded)', 'file_type': 'XML',
                    'filename': rid + '.xml',
                    'verification': 'Existing uploaded XML validated; PDF upgrade unsuccessful',
                    'notes': ' | '.join(filter(None, [report.get('notes', ''),
                        'Existing XML retained; PDF not downloaded']))
                })
            elif 'xml' in files:
                retrieved[(rid, 'xml')] = files['xml']
                row = report
            else:
                row = report

        rows_by_id[rid] = row
        progress_bar.progress(index / len(chosen))

    return [rows_by_id[rec['id']] for rec in records], retrieved, upgraded_ids


def prior_success_ids(past_rows, selected_ids):
    success_markers = {
        r.STATUS_PDF, r.STATUS_XML, 'Existing PDF (uploaded)', 'Existing XML (uploaded)',
        'PDF accounted for (uploaded or downloaded)',
        'Full-text XML accounted for (PDF not yet obtained)'
    }
    return {rid for rid in selected_ids if past_rows.get(rid, {}).get('status') in success_markers}


st.title('CICLR · Full-Text Finder')
st.caption('v4 · Better PMC AWS retrieval · OpenAlex/Crossref/Unpaywall fallbacks · self-contained batch ZIPs · original MR IDs')
st.info(
    'This app retrieves legally available full texts only. It cannot bypass paywalls or university logins. '
    'A record counts as obtained only when a PDF or full-text XML file is actually present and validated.'
)

source = st.radio('Article source', ('Preloaded Core Include (340)', 'Upload Excel or CSV'), horizontal=True)
if source.startswith('Preloaded'):
    try:
        records = load_core_records()
    except Exception as exc:
        st.error('Cannot load core_include.json. Check the app folder in GitHub.')
        st.exception(exc)
        st.stop()
else:
    uploaded_list = st.file_uploader('Upload CICLR Excel/CSV with MR ID and DOI or PMID', type=['csv', 'xlsx'])
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
st.caption(
    f'Selected {len(chosen)} records: **{chosen[0]["id"]}** through **{chosen[-1]["id"]}**. '
    'Starting record is the position in the Core Include list, not the MR number.'
)

with st.expander('Earlier full-text files / tracker — strongly recommended for retries', expanded=True):
    st.warning(
        'For a retry, upload your earlier ZIP/PDF/XML files here. A tracker alone cannot prove that you still possess the file. '
        'v4 will merge earlier files with new retrievals and produce one self-contained ZIP for the selected batch.'
    )
    earlier_uploads = st.file_uploader(
        'Upload earlier ZIP(s), PDFs, or XMLs', type=['zip', 'pdf', 'xml'],
        accept_multiple_files=True,
        help='For the first 25, upload CICLR_Pilot_13_Existing_Files.zip or any newer batch ZIP you saved.'
    )
    previous_report = st.file_uploader(
        'Optional: upload the latest cumulative tracker CSV', type=['csv'], key='earlier_tracker')

xml_fallback = st.checkbox('Save full-text XML if PDF cannot be obtained', value=True)
upgrade_xml = st.checkbox('Try to upgrade existing XML-only articles to PDF', value=True)

with st.expander('Advanced retrieval settings'):
    max_tries = int(st.slider('Maximum PDF links to try per article', 1, 15, 10))
    search_repositories = st.checkbox('Inspect open-access repository landing pages for PDF links', value=True)
    openalex_key = st.text_input(
        'Optional OpenAlex API key', type='password', value='',
        help='Free key; useful if anonymous OpenAlex access is limited. Do not put the key in GitHub.')
    contact_email = st.text_input(
        'Optional contact email for Unpaywall/Crossref', value='',
        help='Unpaywall requires an email parameter. This value is used only during this run and is not saved in GitHub.')

if st.button('Find and download full texts for selected records', type='primary'):
    with st.spinner('Checking earlier files and legal open-access sources. This may take several minutes...'):
        try:
            previously_uploaded = [(file.name, file.getvalue()) for file in earlier_uploads or []]
            existing, warnings = r.load_existing_files(previously_uploaded, records)
            for warning in warnings[:15]:
                st.warning(warning)

            past = r.parse_report_csv(previous_report.getvalue(), records) if previous_report else {
                rec['id']: r.blank_row(rec) for rec in records
            }

            selected_ids = [rec['id'] for rec in chosen]
            prior_reported = prior_success_ids(past, set(selected_ids))
            prior_files = {rid for rid, _ in existing if rid in set(selected_ids)}
            stale = sorted(prior_reported - prior_files)
            if stale:
                st.warning(
                    f'The uploaded tracker reports {len(stale)} previously obtained article(s) in this batch, '
                    'but their actual PDF/XML files were not uploaded. They will be retried and are not counted as holdings unless re-obtained.'
                )

            progress = st.progress(0)
            status = st.empty()
            client = r.HTTPClient()
            report_rows, downloaded, upgraded_ids = process_selected(
                records, past, existing, chosen, client, openalex_key, contact_email,
                xml_fallback, upgrade_xml, max_tries, search_repositories, progress, status)

            # Actual holdings win over stale network statuses.
            all_holdings = dict(existing)
            all_holdings.update(downloaded)
            report_rows = r.reconcile_rows_with_files(report_rows, all_holdings)

            batch_zip, best_selected = r.selected_batch_archive(
                existing, downloaded, report_rows, selected_ids)
            n_pdf = sum(ext == 'pdf' for _, ext in best_selected)
            n_xml = sum(ext == 'xml' for _, ext in best_selected)
            n_missing = len(chosen) - n_pdf - n_xml
            new_article_ids = {rid for rid, _ in downloaded} - {rid for rid, _ in existing}

            summary = (
                f'Finished: {n_pdf} PDF + {n_xml} full-text XML = {n_pdf+n_xml}/{len(chosen)} articles '
                f'with actual full-text files ({n_missing} still missing). '
                f'New full-text article(s) this run: {len(new_article_ids)}; XML→PDF upgrades: {len(upgraded_ids)}.'
            )

            st.session_state['ciclr_v4_run'] = {
                'rows': report_rows,
                'selected_ids': selected_ids,
                'batch_zip': batch_zip,
                'new_zip': r.results_archive(downloaded, report_rows, [name for name, _ in previously_uploaded]),
                'tracker': r.csv_bytes(report_rows),
                'queue': r.library_queue(report_rows)[0],
                'summary': summary,
                'upgraded': upgraded_ids,
            }
            status.success(summary)
        except Exception as exc:
            st.error('Retrieval encountered an error. Your earlier files remain unchanged.')
            st.exception(exc)

if 'ciclr_v4_run' in st.session_state:
    result = st.session_state['ciclr_v4_run']
    st.subheader('Retrieval results')
    st.success(result['summary'])
    if result.get('upgraded'):
        st.info('Upgraded from XML to PDF: ' + ', '.join(result['upgraded']))

    selected_ids = set(result['selected_ids'])
    df = pd.DataFrame([row for row in result['rows'] if row.get('id') in selected_ids])
    display_columns = [
        'id', 'title', 'status', 'filename', 'successful_pdf_url',
        'verification', 'failed_attempts', 'notes', 'article_links'
    ]
    st.dataframe(df[[col for col in display_columns if col in df.columns]],
                 use_container_width=True, hide_index=True)

    st.markdown('**Recommended:** download the COMPLETE selected-batch ZIP. It contains prior uploaded full texts plus new files, with PDF preferred over XML for each MR ID.')
    a, b = st.columns(2)
    a.download_button(
        'Download COMPLETE selected-batch full texts ZIP', result['batch_zip'],
        file_name='CICLR_v4_Selected_Batch_FullTexts.zip', mime='application/zip')
    b.download_button(
        'Download NEW files only ZIP', result['new_zip'],
        file_name='CICLR_v4_New_Files_Only.zip', mime='application/zip')

    c, d = st.columns(2)
    c.download_button(
        'Download CUMULATIVE tracker CSV', result['tracker'],
        file_name='CICLR_v4_Cumulative_Tracker.csv', mime='text/csv')
    d.download_button(
        'Download library-access queue CSV', result['queue'],
        file_name='CICLR_v4_Library_Queue.csv', mime='text/csv')

st.caption('Academic research use · Full-text retrieval does not replace eligibility screening or checking figures/supplements when needed.')
