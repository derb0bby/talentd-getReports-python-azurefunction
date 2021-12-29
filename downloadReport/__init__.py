import datetime
import logging
import re
from io import BytesIO
import requests
from azure.storage.blob import ContainerClient
from bs4 import BeautifulSoup
import time
import azure.functions as func

utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

def get_csrf_token(session) -> str:
    LOGIN_PAGE_URL = 'https://app.talentdesk.io/login'
    with session as s:
        r = s.get(LOGIN_PAGE_URL)
        soup = BeautifulSoup(r.content, 'html.parser')
        text = soup.find_all(text=re.compile("__CSRF_TOKEN__ = \'(\S+)\'"))
        parse_str = str(text[0])
        match = re.search(r"__CSRF_TOKEN__ = \'(\S+)\'", parse_str)
        csrf_token = match.group(1)
    return csrf_token


def login(session):
    login_header = {
        'authority': 'app.talentdesk.io',
        'x-xsrf-token': get_csrf_token(session),
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
        'content-type': 'application/json;charset=UTF-8',
        'accept': 'application/json, text/plain, */*',
        'origin': 'https://app.talentdesk.io',
        'referer': 'https://app.talentdesk.io/login',
    }

    payload = '{"password":"Fepa4657!","email":"mauricio.franco@atrain.com"}'
    LOGIN_POST_URL = 'https://app.talentdesk.io/api/accounts/login'

    with session as s:
        response = s.post(LOGIN_POST_URL, headers=login_header,
                          data=payload, allow_redirects=True)
        logging.info('Login Response code:' + str(response.status_code))
        return response


def logout(session):
    header = {
        'authority': 'app.talentdesk.io',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
        'content-type': 'application/json;charset=UTF-8',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'referer': 'https://app.talentdesk.io/atrain-Germany/projects',
    }
    LOGOUT_URL = 'https://app.talentdesk.io/accounts/logout'

    with session as s:
        response = s.get(LOGOUT_URL, headers=header)
        logging.info('Logout Response code:' + str(response.status_code))
        return response


def downloadReport(session, FILE_URL: str) -> BytesIO:

    ADLS2_CONN_STR = "DefaultEndpointsProtocol=https;AccountName=atrainlakeprodeu;AccountKey=XARMjD/WGyRgzqoVg4/Fr765BgcnhatONLwQmZHjxVCy239Pe0WAIkmxEHrupggE+VSAkDhBa5u62rTnQVKS0g==;EndpointSuffix=core.windows.net"
    header = {
        'authority': 'app.talentdesk.io',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
        'content-type': 'application/json;charset=UTF-8',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'referer': 'https://app.talentdesk.io/atrain-Germany/projects',
    }

    with session as s:
        data = BytesIO()
        response = s.get(FILE_URL, headers=header)
        data = response.content

        if response.ok:

            file_name = response.headers.get(
                "Content-Disposition").split("filename=")[1].replace('"', '')

            if file_name is None:
                file_name = f'no_filename_found_{time.strftime("%Y%m%d-%H%M%S")}.csv'

            elif re.search('reports/12/', FILE_URL) is not None:
                file_name = file_name.replace('.csv', '_custom.csv')

            # Upload file to data lake storage
            blob_block = ContainerClient.from_connection_string(
                conn_str=ADLS2_CONN_STR, container_name='raw/talentdesk/reports')
            blob_block.upload_blob(
                file_name, data, overwrite=True, encoding='utf-8')
        else:
            response.raise_for_status()


def monthList(StartDate: str = None) -> list:
    """
    Params:
        StatDate: Set a custom start date for full/reload scenarios.
        If no value given, the StartDate will always be the previous month.

    Returns:
        List: Returns a list fo date strings e.g. ["2021-01-01", "2021-02-01"...]
    """
    todaysDate = datetime.date.today()

    if StartDate is None:  # Set previous month as Start Date
        beginDate = todaysDate.replace(day=1) - datetime.timedelta(days=1)

    dates_between = [beginDate.strftime(
        '%Y-%m-01'), todaysDate.strftime('%Y-%m-%d')]
    start, end = [datetime.datetime.strptime(
        _, "%Y-%m-%d") for _ in dates_between]

    def total_months(dt): return dt.month + 12 * dt.year
    mlist = []
    for tot_m in range(total_months(start)-1, total_months(end)):
        y, m = divmod(tot_m, 12)
        mlist.append(datetime.datetime(y, m+1, 1).strftime("%Y-%m-%d"))
    return mlist


def main(mytimer: func.TimerRequest) -> None:
    

    # List of downloadable reports  
    REPORT_IDS = [1, 2, 10, 20, 21, 40, 12]  # /32/csv/2021-12-01/2021-12-29

    session = requests.session()
    login_response = login(session)

    if login_response.ok:
        dates = monthList()
        for date in dates: # For each month, download all reports
            for id in REPORT_IDS:
                url = f'https://app.talentdesk.io/api/atrain-Germany/analytics/reports/{id}/csv/{date}'
                try:
                    downloadReport(session, FILE_URL=url)
                    time.sleep(0.5)
                except requests.exceptions.HTTPError as e:
                    logging.warning('Download error %s', utc_timestamp)
                    continue
        logout(session)
    else:
        login_response.raise_for_status()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
