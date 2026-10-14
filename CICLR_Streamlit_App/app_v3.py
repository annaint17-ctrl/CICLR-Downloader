"""CICLR research full-text retrieval helpers.

Uses official and public OA discovery/download endpoints. No paywall avoidance.
S3 structure is the NIH/NLM PMC Article Datasets structure introduced in 2026.
See: https://pmc.ncbi.nlm.nih.gov/tools/pmcaws/
"""
from __future__ import annotations

import csv
import io
import ipaddress
import json
import re
import socket
import time
import zipfile
from dataclasses import dataclass, field
from html.parser import HTMLParser
from urllib.parse import quote, urlencode, urljoin, urlparse
from xml.etree import ElementTree as ET

import requests

API_AGENT = 'CICLR-Academic-Fulltext-Finder/3.0 (non-commercial research; OA-only)'
PMC_BUCKET = 'https://pmc-oa-opendata.s3.amazonaws.com'
EPMC_API = 'https://www.ebi.ac.uk/europepmc/webservices/rest'
OPENALEX_API = 'https://api.openalex.org'
MAX_PDF_BYTES = 35 * 1024 * 1024
MAX_XML_BYTES = 14 * 1024 * 1024
MAX_HTML_BYTES = 1_500_000
MAX_ARCHIVE_BYTES = 180 * 1024 * 1024
MAX_ARCHIVE_ENTRIES = 1000
RETRYABLE = {429, 500, 502, 503, 504}
STATUS_PDF = 'PDF downloaded and validated'
STATUS_XML = 'Full-text XML retrieved (PDF not yet obtained)'
STATUS_LANDING = 'Article links located; no full-text file obtained'
STATUS_BLOCKED = 'PDF location found; automated retrieval failed'
STATUS_NOT_FOUND = 'No legal open-access full text found automatically'
STATUS_NOT_CHECKED = 'Not checked'


def normalize_doi(value):
    val = str(value or '').strip()
    if val.lower() in ('nan', 'none', '<na>'):
        return ''
    val = re.sub(r'^(?:https?://(?:dx\.)?doi\.org/|doi\s*:\s*)', '', val, flags=re.I)
    return val.strip().rstrip('.,')


def normalize_id(value):
    match = re.search(r'MR[-_ ]?(\d{1,6})', str(value), re.I)
    return f'MR-{int(match.group(1)):04d}' if match else ''


def normalize_pmcid(value):
    match = re.search(r'PMC\s*(\d+)', str(value), re.I)
    return 'PMC' + match.group(1) if match else ''


def valid_public_url(url):
    """Allow HTTPS public hosts only; no embedded credentials/nonstandard ports."""
    try:
        p = urlparse(url)
        if p.scheme != 'https' or not p.hostname or p.username or p.password or p.port not in (None, 443):
            return False
        for row in socket.getaddrinfo(p.hostname, 443, type=socket.SOCK_STREAM):
            if not ipaddress.ip_address(row[4][0]).is_global:
                return False
        return True
    except (ValueError, OSError, TypeError):
        return False


@dataclass
class DownloadCandidate:
    url: str
    provider: str
    provenance: str = ''


@dataclass
class Lookup:
    candidates: list[DownloadCandidate] = field(default_factory=list)
    pages: list[str] = field(default_factory=list)
    pmcid: str = ''
    license: str = ''
    notes: list[str] = field(default_factory=list)
    source: str = ''
    aws_xml_urls: list[str] = field(default_factory=list)

    def add_pdf(self, url, provider, provenance=''):
        if isinstance(url, str) and url.startswith('https://') and url not in (c.url for c in self.candidates):
            self.candidates.append(DownloadCandidate(url, provider, provenance))

    def add_page(self, url):
        if isinstance(url, str) and url.startswith('https://') and url not in self.pages:
            self.pages.append(url)


class HTTPClient:
    """Safe, bounded fetches, polite backoff, no automatic unverified redirects."""
    def __init__(self, session=None, timeout=(5, 16)):
        self.session = session or requests.Session()
        self.timeout = timeout
        self.last_error = ''

    def request(self, url, params=None, accept='*/*', limit=MAX_XML_BYTES, retries=1):
        self.last_error = ''
        if params:
            url = url + ('&' if '?' in url else '?') + urlencode(params)
        current = url
        for redirect in range(6):
            if not valid_public_url(current):
                self.last_error = 'Blocked unsafe/non-HTTPS URL'
                return None
            for trial in range(retries + 1):
                response = None
                try:
                    response = self.session.get(
                        current,
                        headers={'User-Agent': API_AGENT, 'Accept': accept},
                        timeout=self.timeout, stream=True, allow_redirects=False,
                    )
                    if response.status_code in (301, 302, 303, 307, 308):
                        dest = urljoin(current, response.headers.get('Location', ''))
                        response.close()
                        current = dest
                        break
                    if response.status_code in RETRYABLE and trial < retries:
                        response.close()
                        time.sleep(min(3, 0.65 * (2 ** trial)))
                        continue
                    if response.status_code != 200:
                        self.last_error = 'HTTP ' + str(response.status_code)
                        response.close()
                        return None
                    out = bytearray()
                    for part in response.iter_content(chunk_size=65536):
                        if part:
                            out.extend(part)
                        if len(out) > limit:
                            self.last_error = 'File too large'
                            response.close()
                            return None
                    response.close()
                    return bytes(out)
                except requests.RequestException as e:
                    self.last_error = type(e).__name__
                    if response is not None:
                        response.close()
                    if trial < retries:
                        time.sleep(0.5)
                        continue
                    return None
            else:
                return None
            # Loop breaks only for a validated redirect target (checked next pass).
        self.last_error = 'Too many redirects'
        return None

    def json(self, url, params=None):
        raw = self.request(url, params=params, accept='application/json', limit=4_000_000)
        if raw is None:
            return None
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else None
        except (ValueError, UnicodeError):
            self.last_error = 'Invalid JSON'
            return None


def validate_pdf(data, record=None, trusted_metadata=False):
    """Basic structural PDF validation + optional first page title/DOI when pypdf installed.

    Identity cannot be established solely by PDF magic. Trusted source metadata
    is separately crosschecked by DOI in the retrieval pipeline.
    """
    if not data or len(data) < 1024 or not data.startswith(b'%PDF-'):
        return False, 'Not a valid PDF header or PDF too small'
    if b'%%EOF' not in data[-8192:]:
        return False, 'No PDF EOF marker (truncated download suspected)'
    try:
        from pypdf import PdfReader  # optional validation if installed
        reader = PdfReader(io.BytesIO(data), strict=False)
        if len(reader.pages) < 1:
            return False, 'PDF has no pages'
        if record:
            first = ' '.join((reader.pages[i].extract_text() or '') for i in range(min(2, len(reader.pages))))
            if len(first.strip()) > 80:
                doi = normalize_doi(record.get('doi', '')).lower()
                flat = re.sub(r'\s+', '', first).lower()
                title = str(record.get('title', '')).lower()
                # Loose keyword overlap can falsely match two papers about the same topic.
                # Require an exact DOI or a long, near-contiguous distinctive title span.
                title_words = re.findall(r'[a-z0-9]+', title)
                text_words = re.findall(r'[a-z0-9]+', first.lower())
                title_compact = ''.join(title_words)
                page_compact = ''.join(text_words)
                title_match = len(title_words) >= 5 and len(title_compact) >= 35 and title_compact in page_compact
                if doi and doi in flat:
                    return True, 'PDF parsed; DOI matched'
                if title_match:
                    return True, 'PDF parsed; exact normalized title matched'
                return (True, 'PDF parsed; identity not independently matched (trusted source DOI metadata)') if trusted_metadata else (False, 'PDF article title/DOI did not match')
        return True, 'PDF parsed; identity confirmation requires source metadata'
    except ImportError:
        # No additional package is required by the deployed app. Many article
        # PDFs carry their DOI uncompressed in their XMP metadata/header.
        head = data[:500_000].decode('latin-1', errors='replace').lower()
        doi = normalize_doi((record or {}).get('doi', '')).lower()
        if doi and doi in re.sub(r'\s+', '', head):
            return True, 'PDF structure and embedded DOI matched (lightweight check)'
        sources = re.findall(r'<dc:source>[^<]*?(10\.\d{4,9}/[^<\s]+)', head)
        if doi and sources and not any(doi == v.rstrip('.,') for v in sources):
            return False, 'Embedded publisher DOI does not match selected article'
        return True, ('PDF structure validated; article identity checked using official source metadata'
                      if trusted_metadata else 'PDF structure validated; article identity needs manual confirmation')
    except Exception as exc:
        return False, 'PDF parsing failed: ' + type(exc).__name__


def xml_summary(data, record=None):
    if not data or len(data) > MAX_XML_BYTES:
        return None
    try:
        root = ET.fromstring(data)
        if root.tag.split('}')[-1] != 'article':
            return None
        article_meta = root.find('.//article-meta')
        body = root.find('body')
        if article_meta is None or body is None or len(''.join(body.itertext())) < 1500:
            return None
        ids = {}
        for elem in article_meta.findall('article-id'):
            ids[elem.attrib.get('pub-id-type', '').lower()] = (elem.text or '').strip()
        title_el = article_meta.find('.//article-title')
        title = ' '.join(title_el.itertext()).strip() if title_el is not None else ''
        doi = normalize_doi(ids.get('doi', ''))
        if record and record.get('doi') and doi and doi.lower() != normalize_doi(record['doi']).lower():
            return None
        if record and record.get('pmid') and ids.get('pmid') and str(record['pmid']).strip() != ids['pmid']:
            return None
        pdf_names = []
        for elem in root.iter():
            if elem.tag.split('}')[-1] == 'self-uri' and elem.attrib.get('content-type') == 'pmc-pdf':
                href = elem.attrib.get('{http://www.w3.org/1999/xlink}href', '')
                if re.fullmatch(r'[a-zA-Z0-9_.-]+\.pdf', href):
                    pdf_names.append(href)
        return {'pmcid': normalize_pmcid(ids.get('pmcid', '')), 'pmid': ids.get('pmid', ''),
                'doi': doi, 'title': title, 'pdf_names': list(dict.fromkeys(pdf_names)),
                'body_chars': len(''.join(body.itertext()))}
    except (ET.ParseError, ValueError):
        return None


def europe_pmc_record(client, record):
    pmid = str(record.get('pmid', '') or '').strip()
    doi = normalize_doi(record.get('doi'))
    queries = []
    if pmid.isdigit():
        queries.append('EXT_ID:' + pmid + ' AND SRC:MED')
    if doi:
        queries.append('DOI:"' + doi + '"')
    for query in queries:
        response = client.json(EPMC_API + '/search', {'query': query, 'format': 'json', 'resultType': 'core', 'pageSize': '2'})
        if not response:
            continue
        for item in (response.get('resultList') or {}).get('result', []):
            returned_doi = normalize_doi(item.get('doi', ''))
            returned_pmid = str(item.get('pmid', '')).strip()
            if doi and returned_doi and doi.lower() != returned_doi.lower():
                continue
            if pmid and returned_pmid and pmid != returned_pmid:
                continue
            return item
    return None


def openalex_record(client, doi, key=''):
    if not doi:
        return None
    url = OPENALEX_API + '/works/https://doi.org/' + quote(doi, safe='/')
    params = {'api_key': key.strip()} if key.strip() else None
    return client.json(url, params=params)


def pmc_aws_info(client, pmcid, expected_doi=''):
    """PMC Cloud version-aware lookup; does NOT use retired PMC OA API."""
    pmcid = normalize_pmcid(pmcid)
    if not pmcid:
        return [], []
    raw = client.request(PMC_BUCKET + '/', {
        'list-type': '2', 'prefix': pmcid + '.', 'delimiter': '/', 'max-keys': '15'},
        accept='application/xml', limit=300_000)
    versions = []
    if raw:
        try:
            root = ET.fromstring(raw)
            for elem in root.iter():
                if elem.tag.split('}')[-1] == 'Prefix' and elem.text:
                    v = elem.text.strip('/').split('/')[-1]
                    if re.fullmatch(re.escape(pmcid) + r'\.\d+', v):
                        versions.append(v)
        except ET.ParseError:
            pass
    metadata_urls = [PMC_BUCKET + '/metadata/' + v + '.json' for v in sorted(set(versions), reverse=True)]
    # If listing is blocked, probe common metadata versions; never invent a PDF path.
    if not metadata_urls:
        metadata_urls = [PMC_BUCKET + '/metadata/' + pmcid + f'.{v}.json' for v in (1, 2, 3)]
    found = []
    info_notes = []
    for meta_url in metadata_urls[:5]:
        item = client.json(meta_url)
        if not item:
            continue
        if normalize_pmcid(item.get('pmcid')) != pmcid:
            info_notes.append('Cloud metadata PMCID mismatch')
            continue
        retrieved_doi = normalize_doi(item.get('doi'))
        if expected_doi and retrieved_doi and expected_doi.lower() != retrieved_doi.lower():
            info_notes.append('Cloud metadata DOI mismatch; ignored')
            continue
        found.append(item)
    return found, info_notes


class _MetaParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.pdf_links = []
    def handle_starttag(self, tag, attrs):
        d = dict(attrs)
        if tag == 'meta':
            name = (d.get('name') or d.get('property') or '').lower()
            if name in {'citation_pdf_url', 'dc.identifier', 'citation_fulltext_pdf_url'}:
                val = d.get('content', '')
                if '.pdf' in val.lower() or name != 'dc.identifier':
                    self.pdf_links.append(val)
        elif tag == 'a':
            href = d.get('href', '')
            if '.pdf' in href.lower() and not href.lower().startswith('javascript:'):
                self.pdf_links.append(href)


def repository_pdf_links(client, landing_url):
    """Inspect only OA landing pages identified by OpenAlex for PDF metadata."""
    data = client.request(landing_url, accept='text/html', limit=MAX_HTML_BYTES, retries=0)
    if not data or b'<html' not in data[:4000].lower():
        return []
    parser = _MetaParser()
    try:
        parser.feed(data.decode('utf-8', errors='replace'))
    except Exception:
        return []
    urls = []
    for candidate in parser.pdf_links:
        u = urljoin(landing_url, candidate)
        if u.startswith('https://') and u not in urls:
            urls.append(u)
    return urls[:6]


def discover_sources(client, record, openalex_key='', use_html=True):
    doi = normalize_doi(record.get('doi'))
    lookup = Lookup()
    lookup.pmcid = normalize_pmcid(record.get('pmcid', ''))
    ep = europe_pmc_record(client, record) if not lookup.pmcid else None
    if ep:
        pmcid = normalize_pmcid(ep.get('pmcid', ''))
        if pmcid:
            lookup.pmcid = pmcid
            lookup.add_page('https://pmc.ncbi.nlm.nih.gov/articles/' + pmcid + '/')
            lookup.add_page('https://europepmc.org/articles/' + pmcid)
        for link in ((ep.get('fullTextUrlList') or {}).get('fullTextUrl') or []):
            url = link.get('url', '')
            if 'pdf' in (link.get('documentStyle', '') or '').lower() or '.pdf' in url.lower():
                lookup.add_pdf(url, 'Europe PMC full-text metadata')
            else:
                lookup.add_page(url)
        lookup.source = 'Europe PMC'
    elif not lookup.pmcid:
        lookup.notes.append('Europe PMC: no matching record or service unavailable')

    oa = openalex_record(client, doi, openalex_key)
    oa_landing_pages = []
    if oa:
        oa_doi = normalize_doi(oa.get('doi'))
        if doi and oa_doi and oa_doi.lower() != doi.lower():
            lookup.notes.append('OpenAlex DOI mismatch; ignored')
        else:
            for loc in [oa.get('best_oa_location')] + (oa.get('locations') or []):
                if not isinstance(loc, dict):
                    continue
                # May include several non-OA locations; these are not automated targets.
                if not loc.get('is_oa'):
                    continue
                pdf = loc.get('pdf_url')
                page = loc.get('landing_page_url')
                source_info = loc.get('source') or {}
                source_name = (source_info.get('display_name') or 'OpenAlex OA location') if isinstance(source_info, dict) else 'OpenAlex OA location'
                lookup.add_pdf(pdf, source_name, 'OpenAlex OA location')
                lookup.add_page(page)
                is_repo = (source_info.get('type') in ('repository', 'preprint-server')
                           if isinstance(source_info, dict) else False)
                if page and (not pdf or is_repo):
                    oa_landing_pages.append(page)
            pmcid = normalize_pmcid((oa.get('ids') or {}).get('pmcid', ''))
            if pmcid and not lookup.pmcid:
                lookup.pmcid = pmcid
            lookup.source += ', OpenAlex'
    else:
        lookup.notes.append('OpenAlex: no metadata, rate limit, or API unavailable')

    if not lookup.pmcid:
        lookup.pmcid = normalize_pmcid(record.get('pmcid', ''))
    if lookup.pmcid:
        lookup.add_page('https://pmc.ncbi.nlm.nih.gov/articles/' + lookup.pmcid + '/')
        metadata, aws_notes = pmc_aws_info(client, lookup.pmcid, doi)
        lookup.notes += aws_notes
        if not metadata:
            lookup.notes.append('PMC Cloud metadata not retrieved: possibly not in licensed dataset, temporarily inaccessible, or rate-limited')
        elif not any(meta.get('pdf_url') for meta in metadata):
            lookup.notes.append('PMC Cloud record located but no licensed PDF URL provided')
        # AWS is the official primary PDF source when article metadata permits it.
        for meta in metadata:
            if not lookup.license:
                lookup.license = str(meta.get('license_code') or '')
            if meta.get('pdf_url'):
                candidate = meta['pdf_url']
                if urlparse(candidate).hostname == 'pmc-oa-opendata.s3.amazonaws.com':
                    lookup.add_pdf(candidate, 'PMC official AWS dataset', 'PMCID and DOI matched against NLM metadata')
            is_oa = meta.get('is_pmc_openaccess') in (True, 1, 'yes', 'true', 'True')
            if is_oa and meta.get('xml_url'):
                lookup.aws_xml_urls.append(meta['xml_url'])
        # XML self-uri identifies the *exact* PMC PDF filename where one exists.
        # This is a public link fallback, not an attempt to bypass a 403/paywall.
        for original_name in (record.get('xml_pdf_names') or [])[:3]:
            if re.fullmatch(r'[A-Za-z0-9_.-]+\.pdf', original_name):
                lookup.add_pdf('https://pmc.ncbi.nlm.nih.gov/articles/' + lookup.pmcid +
                               '/pdf/' + quote(original_name), 'PMC XML-linked PDF')
        lookup.add_pdf('https://europepmc.org/articles/' + lookup.pmcid + '?page=1&pdf=render', 'Europe PMC PDF page')

    if use_html:
        for page in list(dict.fromkeys(oa_landing_pages))[:3]:
            if len(lookup.candidates) >= 7:
                break
            for discovered in repository_pdf_links(client, page):
                lookup.add_pdf(discovered, 'Open-access landing-page PDF')
    # Common publisher-specific OA targets, only for records with OA evidence.
    if lookup.pmcid or any(c.provenance == 'OpenAlex OA location' for c in lookup.candidates):
        if doi.startswith(('10.1007/', '10.1186/')):
            lookup.add_pdf('https://link.springer.com/content/pdf/' + doi + '.pdf', 'Springer OA candidate')
        if doi.startswith('10.1371/journal.pone.'):
            lookup.add_pdf('https://journals.plos.org/plosone/article/file?id=' + quote(doi) + '&type=printable', 'PLOS OA candidate')
    if doi:
        lookup.add_page('https://doi.org/' + quote(doi, safe='/'))
    pmid = str(record.get('pmid', '') or '').strip()
    if pmid.isdigit():
        lookup.add_page('https://pubmed.ncbi.nlm.nih.gov/' + pmid + '/')

    # Prioritize official NLM AWS links over publisher/repository links; deduplicate.
    lookup.candidates.sort(key=lambda c: (0 if c.provider == 'PMC official AWS dataset' else
                                          1 if c.provenance == 'OpenAlex OA location' else
                                          2 if c.provider == 'Open-access landing-page PDF' else 3))
    return lookup


def retrieve_fulltext_xml(client, lookup, record):
    urls = []
    if lookup.pmcid:
        urls.append(EPMC_API + '/' + lookup.pmcid + '/fullTextXML')
    urls.extend(lookup.aws_xml_urls)
    for url in dict.fromkeys(urls):
        raw = client.request(url, accept='application/xml', limit=MAX_XML_BYTES, retries=0)
        summary = xml_summary(raw, record)
        if summary:
            return raw, url, summary
    return None, '', None


def retrieve_record(client, record, openalex_key='', try_xml=True, max_pdf_tries=7, use_html=True):
    """One record. Returns (report dict, {pdf: bytes, xml: bytes})."""
    rec_id = normalize_id(record.get('id', '')) or str(record.get('id', ''))
    lookup = discover_sources(client, record, openalex_key, use_html)
    files = {}
    errors = []
    source = ''
    verified = ''
    tried = 0
    for candidate in lookup.candidates[:max_pdf_tries]:
        tried += 1
        raw = client.request(candidate.url, accept='application/pdf,application/octet-stream,*/*', limit=MAX_PDF_BYTES, retries=0)
        if raw is None:
            errors.append(candidate.provider + ': ' + client.last_error)
            continue
        ok, detail = validate_pdf(raw, record, trusted_metadata=('NLM metadata' in candidate.provenance))
        if not ok:
            errors.append(candidate.provider + ': ' + detail)
            continue
        files['pdf'] = raw
        source = candidate.url
        verified = detail
        break

    full_xml_source = ''
    xml_summary_obj = None
    if 'pdf' not in files and try_xml:
        raw, full_xml_source, xml_summary_obj = retrieve_fulltext_xml(client, lookup, record)
        if raw:
            files['xml'] = raw
            if xml_summary_obj.get('pmcid') and not lookup.pmcid:
                lookup.pmcid = xml_summary_obj['pmcid']
    if 'pdf' in files:
        outcome = STATUS_PDF
    elif 'xml' in files:
        outcome = STATUS_XML
    elif lookup.candidates:
        outcome = STATUS_BLOCKED
    elif lookup.pages:
        outcome = STATUS_LANDING
    else:
        outcome = STATUS_NOT_FOUND
    if not lookup.pages and not lookup.candidates and not (record.get('doi') or record.get('pmid')):
        outcome = 'No DOI or PMID supplied'
    report = {
        'id': rec_id, 'title': str(record.get('title', '')), 'year': str(record.get('year', '')),
        'doi': normalize_doi(record.get('doi')), 'pmid': str(record.get('pmid', '') or ''),
        'pmcid': lookup.pmcid, 'status': outcome, 'file_type': 'PDF' if 'pdf' in files else 'XML' if 'xml' in files else '',
        'filename': (rec_id + '.pdf') if 'pdf' in files else (rec_id + '.xml') if 'xml' in files else '',
        'successful_pdf_url': source, 'successful_xml_url': full_xml_source,
        'verification': verified if 'pdf' in files else ('XML body and article DOI/PMID validated' if 'xml' in files else ''),
        'license': lookup.license,
        'pdf_candidates': ' | '.join(c.url for c in lookup.candidates),
        'article_links': ' | '.join(lookup.pages),
        'failed_attempts': ' ; '.join(errors[:15]),
        'notes': ' | '.join(lookup.notes),
        'pdf_links_tested': tried,
    }
    return report, files


REPORT_COLUMNS = [
    'id', 'title', 'year', 'doi', 'pmid', 'pmcid', 'status', 'file_type', 'filename',
    'successful_pdf_url', 'successful_xml_url', 'verification', 'license',
    'pdf_candidates', 'article_links', 'failed_attempts', 'notes', 'pdf_links_tested'
]


def blank_row(record):
    result = {c: '' for c in REPORT_COLUMNS}
    result.update({'id': record.get('id', ''), 'title': record.get('title', ''),
                   'year': record.get('year', ''), 'doi': normalize_doi(record.get('doi')),
                   'pmid': record.get('pmid', ''), 'pmcid': record.get('pmcid', ''),
                   'status': STATUS_NOT_CHECKED})
    return result


def parse_report_csv(content, records):
    """Merge only recognized IDs. Historical success statuses are not assumed verified."""
    existing = {rec['id']: blank_row(rec) for rec in records}
    try:
        parsed = csv.DictReader(io.StringIO(content.decode('utf-8-sig', errors='replace')))
        if not parsed.fieldnames or 'id' not in parsed.fieldnames:
            return existing
        for row in parsed:
            rid = normalize_id(row.get('id', ''))
            if rid not in existing:
                continue
            actual_doi = normalize_doi(existing[rid]['doi'])
            row_doi = normalize_doi(row.get('doi', ''))
            if actual_doi and row_doi and actual_doi.lower() != row_doi.lower():
                continue
            base = existing[rid]
            for col in REPORT_COLUMNS:
                if col in row and row[col] is not None:
                    base[col] = row[col]
            base['id'] = rid
    except (ValueError, UnicodeError, csv.Error):
        pass
    return existing


def csv_bytes(rows, fieldnames=REPORT_COLUMNS):
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode('utf-8-sig')


def _safe_archive_name(filename):
    base = str(filename).replace('\\', '/').rsplit('/', 1)[-1]
    match = re.fullmatch(r'(MR[-_]\d{1,6})\.(pdf|xml)', base, flags=re.I)
    if not match:
        return None, None
    return normalize_id(match.group(1)), match.group(2).lower()


def load_existing_files(uploaded, records):
    """Reads ZIP(s) or individual PDFs/XML, checks sizes and XML identifiers.

    uploaded: sequence of (name, bytes). Never writes uploaded files to disk.
    """
    rec_map = {rec['id']: rec for rec in records}
    found = {}
    warnings = []

    def consider(name, content):
        rid, ext = _safe_archive_name(name)
        if not rid or rid not in rec_map or not content:
            return
        if ext == 'pdf':
            if len(content) > MAX_PDF_BYTES:
                warnings.append('File too large: ' + name)
                return
            ok, note = validate_pdf(content, rec_map[rid])
            if not ok:
                warnings.append('Rejected ' + name + ': ' + note)
                return
        else:
            if len(content) > MAX_XML_BYTES or not xml_summary(content, rec_map[rid]):
                warnings.append('Rejected invalid/mismatched XML: ' + name)
                return
        # Prefer PDF to XML for the same article, preserve XML too for later reuse.
        found[(rid, ext)] = content

    for filename, content in uploaded:
        if filename.lower().endswith('.zip'):
            if len(content) > MAX_ARCHIVE_BYTES:
                warnings.append(filename + ': archive too large')
                continue
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as archive:
                    members = archive.infolist()
                    if len(members) > MAX_ARCHIVE_ENTRIES:
                        warnings.append(filename + ': archive has too many entries')
                        continue
                    extracted = 0
                    for member in members:
                        rid, ext = _safe_archive_name(member.filename)
                        if not rid or rid not in rec_map or not ext or member.is_dir():
                            continue
                        cap = MAX_PDF_BYTES if ext == 'pdf' else MAX_XML_BYTES
                        if member.file_size > cap or member.compress_size > MAX_ARCHIVE_BYTES:
                            warnings.append('Skipped oversized archive entry: ' + member.filename)
                            continue
                        extracted += member.file_size
                        if extracted > MAX_ARCHIVE_BYTES:
                            warnings.append(filename + ': stopped at archive safety limit')
                            break
                        consider(member.filename, archive.read(member))
            except (zipfile.BadZipFile, OSError, RuntimeError):
                warnings.append(filename + ': not a readable ZIP')
        else:
            consider(filename, content)
    return found, warnings


def results_archive(files, report_rows, included_names=None):
    """New-file-only ZIP, with exact MR filenames + full report, not the prior ZIP."""
    stream = io.BytesIO()
    included_names = included_names or []
    with zipfile.ZipFile(stream, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for (rid, ext), data in sorted(files.items()):
            folder = 'PDFs' if ext == 'pdf' else 'Full_Text_XML'
            archive.writestr(f'{folder}/{rid}.{ext}', data)
        archive.writestr('retrieval_report.csv', csv_bytes(report_rows))
        archive.writestr('README.txt',
            'CICLR: This ZIP contains new files downloaded in this run only.\n'
            'Previously uploaded files are not duplicated. Keep earlier ZIPs.\n'
            'The CSV tracker covers all records and retains MR identifiers.\n'
            'Full text XML is a valid structured full text source, not an original PDF.\n'
            'File inclusion does not replace full-text eligibility screening.\n'
            'Prior files considered: ' + ', '.join(included_names) + '\n')
    return stream.getvalue()


def library_queue(report_rows):
    fields = ['id', 'title', 'year', 'doi', 'doi_url', 'pubmed_url', 'pmcid',
              'status', 'article_links', 'pdf_candidates', 'failed_attempts', 'next_step',
              'marquette_library_guide', 'marquette_ill_guide']
    rows = []
    for row in report_rows:
        status = row.get('status', '')
        if status == STATUS_NOT_CHECKED or status == STATUS_PDF or status == 'Existing PDF (uploaded)':
            continue
        doi = normalize_doi(row.get('doi', ''))
        has_xml = status in (STATUS_XML, 'Existing XML (uploaded)')
        rows.append({
            'id': row.get('id', ''), 'title': row.get('title', ''), 'year': row.get('year', ''),
            'doi': doi, 'doi_url': 'https://doi.org/' + quote(doi, safe='/') if doi else '',
            'pubmed_url': 'https://pubmed.ncbi.nlm.nih.gov/' + str(row.get('pmid')) + '/' if str(row.get('pmid', '')).isdigit() else '',
            'pmcid': row.get('pmcid', ''), 'status': status,
            'article_links': row.get('article_links', ''), 'pdf_candidates': row.get('pdf_candidates', ''),
            'failed_attempts': row.get('failed_attempts', ''),
            'marquette_library_guide': 'https://www.marquette.edu/library/help/library-search-help.php',
            'marquette_ill_guide': 'https://www.marquette.edu/library/resources/ill/',
            'next_step': 'Full text XML obtained; PDF optional for screening; retry official PDF / library if required'
            if has_xml else 'Check university library full text; request through interlibrary loan if necessary',
        })
    return csv_bytes(rows, fields), len(rows)
