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

API_AGENT = 'CICLR-Academic-Fulltext-Finder/4.0 (non-commercial research; OA-only)'
PMC_BUCKET = 'https://pmc-oa-opendata.s3.amazonaws.com'
EPMC_API = 'https://www.ebi.ac.uk/europepmc/webservices/rest'
OPENALEX_API = 'https://api.openalex.org'
CROSSREF_API = 'https://api.crossref.org'
UNPAYWALL_API = 'https://api.unpaywall.org/v2'
PMC_OAI = 'https://pmc.ncbi.nlm.nih.gov/api/oai/v1/mh/'
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

def s3_to_https(url):
    """Convert the official PMC s3:// object URLs in metadata JSON to anonymous HTTPS."""
    val = str(url or '').strip()
    if not val:
        return ''
    p = urlparse(val)
    if p.scheme == 's3' and p.netloc == 'pmc-oa-opendata':
        path = p.path.lstrip('/')
        return PMC_BUCKET.rstrip('/') + '/' + path + (('?' + p.query) if p.query else '')
    if p.scheme == 'https' and p.hostname == 'pmc-oa-opendata.s3.amazonaws.com':
        return val
    return val


def looks_like_email(value):
    return bool(re.fullmatch(r'[^@\s]+@[^@\s]+\.[^@\s]+', str(value or '').strip()))


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


def openalex_record(client, doi='', pmid='', key=''):
    """Resolve a work by DOI (or PMID) using OpenAlex's documented short-form ID lookup."""
    ident = ''
    doi = normalize_doi(doi)
    if doi:
        ident = 'doi:' + doi
    elif str(pmid or '').strip().isdigit():
        ident = 'pmid:' + str(pmid).strip()
    if not ident:
        return None
    url = OPENALEX_API + '/works/' + quote(ident, safe=':/().-_')
    params = {'api_key': key.strip()} if key.strip() else None
    return client.json(url, params=params)


def crossref_record(client, doi, contact_email=''):
    doi = normalize_doi(doi)
    if not doi:
        return None
    params = {'mailto': contact_email.strip()} if looks_like_email(contact_email) else None
    data = client.json(CROSSREF_API + '/works/' + quote(doi, safe='/'), params=params)
    if not data or data.get('status') != 'ok' or not isinstance(data.get('message'), dict):
        return None
    msg = data['message']
    returned = normalize_doi(msg.get('DOI', ''))
    if returned and returned.lower() != doi.lower():
        return None
    return msg


def unpaywall_record(client, doi, contact_email=''):
    doi = normalize_doi(doi)
    if not doi or not looks_like_email(contact_email):
        return None
    data = client.json(UNPAYWALL_API + '/' + quote(doi, safe='/'), {'email': contact_email.strip()})
    if not data:
        return None
    returned = normalize_doi(data.get('doi', ''))
    if returned and returned.lower() != doi.lower():
        return None
    return data


def pmc_aws_info(client, pmcid, expected_doi=''):
    """Look up official PMC Article Dataset metadata on AWS.

    The 2026 PMC metadata JSON returns s3:// object URLs; these are converted
    to anonymous HTTPS before download.
    """
    pmcid = normalize_pmcid(pmcid)
    if not pmcid:
        return [], []
    raw = client.request(PMC_BUCKET + '/', {
        'list-type': '2', 'prefix': pmcid + '.', 'delimiter': '/', 'max-keys': '30'},
        accept='application/xml', limit=500_000)
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
    # The overwhelming majority are version 1. Probe a few versions if bucket listing failed.
    if not versions:
        versions = [pmcid + '.1', pmcid + '.2', pmcid + '.3']
    versions = sorted(set(versions), key=lambda x: int(x.rsplit('.',1)[1]), reverse=True)
    found, info_notes = [], []
    for version in versions[:6]:
        meta_url = PMC_BUCKET + '/metadata/' + version + '.json'
        item = client.json(meta_url)
        if not item:
            continue
        if normalize_pmcid(item.get('pmcid')) != pmcid:
            info_notes.append('PMC AWS metadata PMCID mismatch')
            continue
        retrieved_doi = normalize_doi(item.get('doi'))
        if expected_doi and retrieved_doi and expected_doi.lower() != retrieved_doi.lower():
            info_notes.append('PMC AWS metadata DOI mismatch; ignored')
            continue
        item = dict(item)
        for field in ('pdf_url', 'xml_url', 'text_url'):
            if item.get(field):
                item[field] = s3_to_https(item[field])
        if isinstance(item.get('media_urls'), list):
            item['media_urls'] = [s3_to_https(u) for u in item['media_urls']]
        item['_metadata_url'] = meta_url
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


def discover_sources(client, record, openalex_key='', use_html=True, contact_email=''):
    doi = normalize_doi(record.get('doi'))
    pmid = str(record.get('pmid', '') or '').strip()
    lookup = Lookup()
    lookup.pmcid = normalize_pmcid(record.get('pmcid', ''))

    # Europe PMC is useful for identifier resolution and public full-text location metadata.
    ep = europe_pmc_record(client, record)
    if ep:
        pmcid = normalize_pmcid(ep.get('pmcid', ''))
        if pmcid:
            lookup.pmcid = lookup.pmcid or pmcid
            lookup.add_page('https://pmc.ncbi.nlm.nih.gov/articles/' + pmcid + '/')
            lookup.add_page('https://europepmc.org/articles/' + pmcid)
        for link in ((ep.get('fullTextUrlList') or {}).get('fullTextUrl') or []):
            url = str(link.get('url', '') or '')
            style = str(link.get('documentStyle', '') or '').lower()
            if 'pdf' in style or '.pdf' in url.lower() or 'pdf=' in url.lower():
                lookup.add_pdf(url, 'Europe PMC full-text metadata')
            else:
                lookup.add_page(url)
        lookup.source = 'Europe PMC'
    else:
        lookup.notes.append('Europe PMC: no matching record or service unavailable')

    # OpenAlex OA locations (documented singleton lookup by DOI).
    oa = openalex_record(client, doi=doi, pmid=pmid, key=openalex_key)
    oa_landing_pages = []
    if oa:
        oa_doi = normalize_doi(oa.get('doi'))
        if doi and oa_doi and oa_doi.lower() != doi.lower():
            # Secondary DOI locations can still resolve to the same work; accept only if a location has the DOI.
            loc_dois = {normalize_doi(loc.get('doi','')).lower() for loc in (oa.get('locations') or []) if isinstance(loc, dict)}
            if doi.lower() not in loc_dois:
                lookup.notes.append('OpenAlex DOI mismatch; ignored')
                oa = None
        if oa:
            for loc in [oa.get('best_oa_location')] + (oa.get('locations') or []):
                if not isinstance(loc, dict) or not loc.get('is_oa'):
                    continue
                pdf = loc.get('pdf_url')
                page = loc.get('landing_page_url')
                source_info = loc.get('source') or {}
                source_name = (source_info.get('display_name') or 'OpenAlex OA location') if isinstance(source_info, dict) else 'OpenAlex OA location'
                lookup.add_pdf(pdf, source_name, 'OpenAlex OA location; DOI-resolved work')
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
        lookup.notes.append('OpenAlex: no metadata / service unavailable (API key optional)')

    # Unpaywall is optional because its API requires an email parameter.
    up = unpaywall_record(client, doi, contact_email)
    if up:
        for loc in [up.get('best_oa_location')] + (up.get('oa_locations') or []):
            if not isinstance(loc, dict):
                continue
            pdf = loc.get('url_for_pdf')
            page = loc.get('url_for_landing_page') or loc.get('url')
            host_type = loc.get('host_type') or 'Unpaywall OA location'
            lookup.add_pdf(pdf, 'Unpaywall ' + str(host_type), 'Unpaywall OA DOI match')
            lookup.add_page(page)
            if page and not pdf:
                oa_landing_pages.append(page)
        lookup.source += ', Unpaywall'
    elif looks_like_email(contact_email):
        lookup.notes.append('Unpaywall: no OA location or service unavailable')

    # Crossref TDM/full-text links are metadata-deposited by the DOI registrant.
    cr = crossref_record(client, doi, contact_email)
    if cr:
        for link in cr.get('link') or []:
            if not isinstance(link, dict):
                continue
            url = link.get('URL') or link.get('url')
            ctype = str(link.get('content-type') or link.get('content_type') or '').lower()
            if 'pdf' in ctype or (isinstance(url, str) and '.pdf' in url.lower()):
                lookup.add_pdf(url, 'Crossref full-text/TDM link', 'Crossref DOI metadata matched')
        resource = cr.get('resource') or {}
        if isinstance(resource, dict):
            primary = resource.get('primary') or {}
            if isinstance(primary, dict):
                lookup.add_page(primary.get('URL'))
        lookup.source += ', Crossref'

    # Official PMC AWS dataset: this is the preferred automated PMC PDF source.
    if lookup.pmcid:
        lookup.add_page('https://pmc.ncbi.nlm.nih.gov/articles/' + lookup.pmcid + '/')
        metadata, aws_notes = pmc_aws_info(client, lookup.pmcid, doi)
        lookup.notes += aws_notes
        if not metadata:
            lookup.notes.append('PMC AWS metadata not retrieved or article version not in distributable dataset')
        for meta in metadata:
            if not lookup.license:
                lookup.license = str(meta.get('license_code') or '')
            if meta.get('pdf_url'):
                lookup.add_pdf(meta['pdf_url'], 'PMC official AWS dataset',
                               'PMCID/DOI matched against official NLM metadata')
            if meta.get('xml_url'):
                lookup.aws_xml_urls.append(meta['xml_url'])
        if metadata and not any(meta.get('pdf_url') for meta in metadata):
            lookup.notes.append('PMC AWS article version found but no distributable PDF exists')

    if use_html:
        for page in list(dict.fromkeys(oa_landing_pages))[:4]:
            if len(lookup.candidates) >= 12:
                break
            for discovered in repository_pdf_links(client, page):
                lookup.add_pdf(discovered, 'Open-access repository/landing-page PDF',
                               'OA landing page supplied by OpenAlex/Unpaywall')

    # Conservative publisher-specific OA patterns only when we have OA evidence.
    has_oa_evidence = bool(lookup.pmcid or up or oa)
    if has_oa_evidence and doi:
        if doi.startswith(('10.1007/', '10.1186/')):
            lookup.add_pdf('https://link.springer.com/content/pdf/' + doi + '.pdf', 'Springer OA candidate')
        if doi.startswith('10.1371/journal.pone.'):
            lookup.add_pdf('https://journals.plos.org/plosone/article/file?id=' + quote(doi) + '&type=printable', 'PLOS OA candidate')
        if doi.startswith(('10.1111/', '10.1155/')):
            lookup.add_pdf('https://onlinelibrary.wiley.com/doi/pdfdirect/' + doi, 'Wiley/Hindawi OA candidate')

    if doi:
        lookup.add_page('https://doi.org/' + quote(doi, safe='/'))
    if pmid.isdigit():
        lookup.add_page('https://pubmed.ncbi.nlm.nih.gov/' + pmid + '/')

    # Official PMC AWS first, then OA aggregators/repositories, then publisher patterns.
    lookup.candidates.sort(key=lambda c: (
        0 if c.provider == 'PMC official AWS dataset' else
        1 if c.provider.startswith('Unpaywall') or c.provenance.startswith('OpenAlex OA') else
        2 if c.provider.startswith('Open-access repository') else
        3 if c.provider.startswith('Crossref') else 4))
    return lookup


def _extract_oai_article(raw, record):
    """Extract the JATS <article> element from a PMC OAI-PMH GetRecord response."""
    if not raw:
        return None
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return None
    article = None
    for elem in root.iter():
        if elem.tag.split('}')[-1] == 'article':
            article = elem
            break
    if article is None:
        return None
    payload = ET.tostring(article, encoding='utf-8', xml_declaration=True)
    return payload if xml_summary(payload, record) else None


def retrieve_fulltext_xml(client, lookup, record):
    # Prefer the official PMC AWS XML object when available.
    for url in dict.fromkeys(lookup.aws_xml_urls):
        raw = client.request(url, accept='application/xml', limit=MAX_XML_BYTES, retries=0)
        summary = xml_summary(raw, record)
        if summary:
            return raw, url, summary

    # Official PMC OAI-PMH full-text endpoint for reusable full text.
    if lookup.pmcid:
        numeric = re.sub(r'^PMC', '', lookup.pmcid, flags=re.I)
        oai_url = PMC_OAI
        raw = client.request(oai_url, {
            'verb': 'GetRecord',
            'identifier': 'oai:pubmedcentral.nih.gov:' + numeric,
            'metadataPrefix': 'pmc'}, accept='application/xml', limit=MAX_XML_BYTES, retries=0)
        payload = _extract_oai_article(raw, record)
        if payload:
            return payload, oai_url + '?verb=GetRecord&identifier=oai:pubmedcentral.nih.gov:' + numeric + '&metadataPrefix=pmc', xml_summary(payload, record)

    # Europe PMC remains a final XML fallback when the article is mirrored there.
    if lookup.pmcid:
        url = EPMC_API + '/' + lookup.pmcid + '/fullTextXML'
        raw = client.request(url, accept='application/xml', limit=MAX_XML_BYTES, retries=0)
        summary = xml_summary(raw, record)
        if summary:
            return raw, url, summary
    return None, '', None


def retrieve_record(client, record, openalex_key='', try_xml=True, max_pdf_tries=9, use_html=True, contact_email=''):
    """One record. Returns (report dict, {pdf: bytes, xml: bytes})."""
    rec_id = normalize_id(record.get('id', '')) or str(record.get('id', ''))
    lookup = discover_sources(client, record, openalex_key, use_html, contact_email)
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


def best_file_map(existing_files, downloaded_files, selected_ids=None):
    """Merge prior + newly retrieved files; prefer PDF over XML for each MR ID."""
    merged = dict(existing_files or {})
    merged.update(downloaded_files or {})
    selected = set(selected_ids or [])
    best = {}
    ids = {rid for rid, _ in merged}
    if selected:
        ids &= selected
    for rid in ids:
        if (rid, 'pdf') in merged:
            best[(rid, 'pdf')] = merged[(rid, 'pdf')]
        elif (rid, 'xml') in merged:
            best[(rid, 'xml')] = merged[(rid, 'xml')]
    return best


def selected_batch_archive(existing_files, downloaded_files, report_rows, selected_ids):
    """Self-contained ZIP for the selected batch, including prior files + new upgrades."""
    selected_ids = list(selected_ids)
    selected_set = set(selected_ids)
    best = best_file_map(existing_files, downloaded_files, selected_set)
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for (rid, ext), data in sorted(best.items()):
            folder = 'PDFs' if ext == 'pdf' else 'Full_Text_XML'
            archive.writestr(f'{folder}/{rid}.{ext}', data)
        rows = [r for r in report_rows if r.get('id') in selected_set]
        archive.writestr('retrieval_report_selected_batch.csv', csv_bytes(rows))
        archive.writestr('README.txt',
            'CICLR v4 selected-batch full-text archive.\n'
            'This ZIP merges uploaded prior files with new retrievals for this selected batch.\n'
            'For each MR ID, PDF is preferred over XML when both exist.\n'
            'Full-text XML is a complete structured full-text source when a distributable PDF is unavailable.\n'
            'File inclusion does not replace full-text eligibility screening.\n')
    return stream.getvalue(), best


def reconcile_rows_with_files(report_rows, file_map):
    """Make tracker status reflect actual uploaded/downloaded holdings, not stale network results."""
    out=[]
    for row in report_rows:
        row=dict(row)
        rid=row.get('id','')
        if (rid,'pdf') in file_map:
            row['status']='PDF accounted for (uploaded or downloaded)'
            row['file_type']='PDF'; row['filename']=rid+'.pdf'
            if not row.get('verification'):
                row['verification']='Validated PDF present in current holdings'
        elif (rid,'xml') in file_map:
            row['status']='Full-text XML accounted for (PDF not yet obtained)'
            row['file_type']='XML'; row['filename']=rid+'.xml'
            if not row.get('verification'):
                row['verification']='Validated full-text XML present in current holdings'
        out.append(row)
    return out

def library_queue(report_rows):
    fields = ['id', 'title', 'year', 'doi', 'doi_url', 'pubmed_url', 'pmcid',
              'status', 'article_links', 'pdf_candidates', 'failed_attempts', 'next_step',
              'marquette_library_guide', 'marquette_ill_guide']
    rows = []
    for row in report_rows:
        status = row.get('status', '')
        if status in (STATUS_NOT_CHECKED, STATUS_PDF, 'Existing PDF (uploaded)', 'PDF accounted for (uploaded or downloaded)'):
            continue
        doi = normalize_doi(row.get('doi', ''))
        has_xml = status in (STATUS_XML, 'Existing XML (uploaded)', 'Full-text XML accounted for (PDF not yet obtained)')
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


# ===== Streamlit UI =====
import json
from pathlib import Path

import pandas as pd
import streamlit as st
import sys

# Standalone build: retrieval helpers are defined above in this same file.
r = sys.modules[__name__]


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
