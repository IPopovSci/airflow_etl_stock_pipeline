from __future__ import annotations
from airflow.providers.http.hooks.http import HttpHook
from dotenv import load_dotenv
import json
import requests
from requests.auth import AuthBase
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter
import pendulum
load_dotenv()

class NewsDataHook(HttpHook,AuthBase):
    conn_name_attr = "newsapi_conn_id"
    default_conn_name = "NEWSAPI"
    conn_type = "NewsAPI"
    hook_name = "News API"



    def __init__(self, newsapi_conn_id: str = 'NEWSAPI') -> None:
        super().__init__()
        self.http_conn_id = newsapi_conn_id
        self.apikey = None

    def run(
        self,
        endpoint: str | None = None,
        data: dict = None,
        headers: dict = None,
        extra_options: dict = None,
        **request_kwargs
    ):
        r"""
        Performs the request

        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :param data: payload to be uploaded or request parameters
        :param headers: additional headers to be passed through as a dictionary
        :param extra_options: additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        :param request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        extra_options = extra_options or {}

        session = self.get_conn(headers)
        data['apikey']=session.auth.password

        url = self.url_from_endpoint(endpoint)

        if self.tcp_keep_alive:
            keep_alive_adapter = TCPKeepAliveAdapter(
                idle=self.keep_alive_idle, count=self.keep_alive_count, interval=self.keep_alive_interval
            )
            session.mount(url, keep_alive_adapter)
        if self.method == "GET":
            # GET uses params
            req = requests.Request(self.method, url, params=data, headers=headers, **request_kwargs)
        elif self.method == "HEAD":
            # HEAD doesn't use params
            req = requests.Request(self.method, url, headers=headers, **request_kwargs)
        else:
            # Others use data
            req = requests.Request(self.method, url, data=data, headers=headers, **request_kwargs)

        prepped_request = session.prepare_request(req)
        self.log.debug("Sending '%s' to url: %s", self.method, url)
        return self.run_and_check(session, prepped_request, extra_options)


    def get_data(self,symbol,start_date \
                 ,end_date):
        self.method='GET'
        body = {'q':symbol,'from':start_date,\
                'to':end_date} #Make it so start date by default is 29 days ago for free plan
        try:
            message = self.run(endpoint='everything',data=body).json()
            json_data=json.dumps(message)
            return json_data
        except Exception as e:
            #print('e args are, ',e.args,'e status code is, ', e.status_code, ' e traceback: ', e.with_traceback)
            if 'Upgrade Required' in str(e.args):
                print("Your API key can't query data from this long ago! Trying w/ last month interval instead")
                start_date = pendulum.now().subtract(days=28).isoformat()
                body = {'q': symbol, 'from': start_date, \
                        'to': end_date}
                message = self.run(endpoint='everything', data=body).json()
                json_data = json.dumps(message)
                return json_data



