import csv, io, json, re, time, zipfile
from pathlib import Path
from urllib.parse import quote, urlparse
import ipaddress, socket
import requests
import pandas as pd
import streamlit as st

st.set_page_config(page_title='CICLR Full-Text Finder', page_icon='📚', layout='wide')
HEADERS={'User-Agent':'CICLRResearchFullTextFinder/1.0 (academic literature discovery; contact via deployed app owner)','Accept':'application/json'}
PDF_HEADERS={'User-Agent':'Mozilla/5.0 (compatible; CICLR academic literature downloader)','Accept':'application/pdf,*/*'}
MAX_PDF=25*1024*1024


def normalize_doi(v):
    s=str(v or '').strip()
    if s.lower() in ('nan','none'):return ''
    return re.sub(r'^(https?://(dx\.)?doi\.org/|doi:\s*)','',s,flags=re.I).strip()

def public_url(url):
    try:
        p=urlparse(url)
        if p.scheme!='https' or not p.hostname or p.username or p.password:return False
        for result in socket.getaddrinfo(p.hostname,443,type=socket.SOCK_STREAM):
            ip=ipaddress.ip_address(result[4][0])
            if not ip.is_global:return False
        return True
    except Exception:return False

def get_json(url):
    if not public_url(url):return None
    try:
        r=requests.get(url,headers=HEADERS,timeout=(6,17),allow_redirects=False)
        if r.status_code==200:return r.json()
    except (requests.RequestException,ValueError):pass
    return None

def pdf_candidates(rec):
    doi=normalize_doi(rec.get('doi','')); pmid=str(rec.get('pmid','')).strip()
    candidates=[]; pages=[]; notes=[]
    if doi:
        oa=get_json('https://api.openalex.org/works/https://doi.org/'+quote(doi,safe='/'))
        if oa:
            locations=[oa.get('best_oa_location'),oa.get('primary_location')]+(oa.get('locations') or [])
            for loc in locations:
                if not isinstance(loc,dict):continue
                u=loc.get('pdf_url'); page=loc.get('landing_page_url')
                if u and u not in candidates:candidates.append(u)
                if page and page not in pages:pages.append(page)
            if oa.get('open_access',{}).get('oa_url') and oa['open_access']['oa_url'] not in pages:pages.append(oa['open_access']['oa_url'])
        else:notes.append('OpenAlex lookup unavailable or no match')
    if pmid and pmid.lower() not in ('nan','none'):
        ep=get_json('https://www.ebi.ac.uk/europepmc/webservices/rest/search?query='+quote('EXT_ID:'+pmid+' AND SRC:MED')+'&format=json&pageSize=1')
        if ep:
            results=ep.get('resultList',{}).get('result',[])
            if results:
                pmcid=results[0].get('pmcid')
                if pmcid:
                    pages.append('https://europepmc.org/articles/'+pmcid)
                    candidates.append('https://europepmc.org/articles/'+pmcid+'?page=1&pdf=render')
    return list(dict.fromkeys(candidates)),list(dict.fromkeys(pages)),notes

def download_pdf(url):
    if not public_url(url):return None,'URL not approved (HTTPS/public host required)'
    try:
        # Redirects checked at every hop, preventing access to private addresses.
        current=url
        for _ in range(6):
            if not public_url(current):return None,'Unsafe redirect'
            r=requests.get(current,headers=PDF_HEADERS,timeout=(8,25),stream=True,allow_redirects=False)
            if r.status_code in (301,302,303,307,308):
                from urllib.parse import urljoin
                nxt=r.headers.get('Location','');r.close()
                current=urljoin(current,nxt)
                continue
            if r.status_code!=200:
                status=r.status_code;r.close();return None,'HTTP '+str(status)
            data=bytearray()
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:data.extend(chunk)
                if len(data)>MAX_PDF:r.close();return None,'PDF exceeds 25 MB limit'
            r.close()
            if bytes(data[:5])!=b'%PDF-':return None,'Response was not a PDF'
            return bytes(data),'Downloaded and PDF signature verified'
        return None,'Too many redirects'
    except requests.RequestException as e:return None,type(e).__name__

@st.cache_data
def core():return json.loads((Path(__file__).parent/'core_include.json').read_text())

st.title('CICLR · Full-Text Finder')
st.caption('Server-side open-access PDF retrieval · Preserves MR IDs · Downloads verified PDFs as a ZIP')
st.info('This app searches public research sources. It cannot bypass paywalls or institutional logins. A link is not counted as a download until a PDF is retrieved and verified.')
source=st.radio('Article source',['Preloaded Core Include (340)','Upload Excel or CSV'],horizontal=True)
if source.startswith('Preloaded'):
    records=core()
else:
    uploaded=st.file_uploader('Upload a CSV or Excel file containing an MR ID and DOI or PMID',type=['csv','xlsx'])
    records=[]
    if uploaded:
        try:
            frame=pd.read_csv(uploaded) if uploaded.name.lower().endswith('.csv') else pd.read_excel(uploaded)
            frame.columns=[str(c).strip().lower() for c in frame.columns]
            def field(row,*names):
                for n in names:
                    if n in row and pd.notna(row[n]):return str(row[n]).strip()
                return ''
            for ix,row in frame.iterrows():
                records.append({'id':field(row,'id','mr id','record id','record_id') or f'ROW-{ix+1:04d}', 'title':field(row,'title','article title'), 'doi':field(row,'doi'), 'pmid':field(row,'pmid'), 'year':field(row,'year')})
        except Exception as e:st.error('Could not read file: '+str(e))
if records:
    st.write(f'**{len(records)} records loaded**')
    c1,c2=st.columns(2)
    start=c1.number_input('Starting record (1-based)',min_value=1,max_value=len(records),value=1,step=1)
    batch=c2.number_input('Batch size',min_value=1,max_value=min(100,len(records)),value=min(25,len(records)),step=1)
    chosen=records[int(start)-1:int(start)-1+int(batch)]
    st.caption(f'Selected {len(chosen)} records: {chosen[0]["id"]} through {chosen[-1]["id"]}')
    if st.button('Find and download PDFs',type='primary'):
        rows=[]; found={};progress=st.progress(0);status=st.empty()
        for i,rec in enumerate(chosen):
            ident=rec['id'];status.write(f'Checking {i+1}/{len(chosen)}: {ident}')
            candidates,pages,notes=pdf_candidates(rec)
            result='No PDF link located';failure='';success_url=''
            if not normalize_doi(rec.get('doi')) and not str(rec.get('pmid','')).strip():result='Missing DOI and PMID'
            for url in candidates[:8]:
                data,detail=download_pdf(url)
                if data:
                    safe_id=re.sub(r'[^A-Za-z0-9_-]','_',ident)
                    name=safe_id+'.pdf'
                    if name in found:name=f'{safe_id}_{i+1}.pdf'
                    found[name]=data;result='Downloaded';success_url=url;failure='';break
                failure=(failure+'; ' if failure else '')+detail
            if result!='Downloaded' and candidates:result='PDF URL found, download failed'
            elif result!='Downloaded' and pages:result='Full-text/landing page only'
            rows.append({'id':ident,'title':rec.get('title',''),'year':rec.get('year',''),'doi':normalize_doi(rec.get('doi')),'pmid':rec.get('pmid',''),'status':result,'downloaded_pdf':(name if result=='Downloaded' else ''),'successful_pdf_url':success_url,'candidate_pdf_urls':' | '.join(candidates),'page_urls':' | '.join(pages),'failure_details':failure,'lookup_notes':' | '.join(notes)})
            progress.progress((i+1)/len(chosen))
            time.sleep(.15)
        output=io.BytesIO()
        csv_out=io.StringIO();w=csv.DictWriter(csv_out,fieldnames=rows[0].keys());w.writeheader();w.writerows(rows)
        with zipfile.ZipFile(output,'w',compression=zipfile.ZIP_DEFLATED) as z:
            for filename,data in found.items():z.writestr('PDFs/'+filename,data)
            z.writestr('retrieval_report.csv',csv_out.getvalue().encode('utf-8-sig'))
            z.writestr('README.txt',f'CICLR retrieval batch. Records checked: {len(rows)}. Verified PDFs: {len(found)}. Other records: {len(rows)-len(found)}. Review retrieval_report.csv for all statuses.\n')
        st.session_state['result_zip']=output.getvalue()
        st.session_state['result_csv']=csv_out.getvalue().encode('utf-8-sig')
        st.session_state['result_rows']=rows
        status.success(f'Finished: {len(found)} verified PDFs out of {len(rows)} records')
    if 'result_rows' in st.session_state:
        df=pd.DataFrame(st.session_state['result_rows'])
        st.subheader('Retrieval results')
        st.dataframe(df[['id','title','status','failure_details','candidate_pdf_urls','page_urls']],hide_index=True,use_container_width=True)
        st.download_button('Download ZIP (PDFs + report)',st.session_state['result_zip'],file_name='CICLR_PDFs_and_Report.zip',mime='application/zip',type='primary')
        st.download_button('Download CSV report',st.session_state['result_csv'],file_name='CICLR_Retrieval_Report.csv',mime='text/csv')
