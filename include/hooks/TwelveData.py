from __future__ import annotations
from airflow.providers.http.hooks.http import HttpHook
from dotenv import load_dotenv
import os
import json
import requests
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter

load_dotenv()

class TwelveDataHook(HttpHook):
    conn_name_attr = "twelvedata_conn_id"
    default_conn_name = "TWELVEDATA"
    conn_type = "TwelveData"
    hook_name = "TwelveData API"

    def __init__(self, twelvedata_conn_id: str = 'TWELVEDATA') -> None:
        super().__init__()
        self.http_conn_id = twelvedata_conn_id

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

    def get_data(self,interval=os.environ['interval'],timezone=os.environ['timezone'],symbol=os.environ['symbol'],start_date=os.environ['start_date'] \
                 ,end_date=os.environ['end_date'],dp=2,format='JSON'):
        self.method='GET'
        body = {'interval':interval,'timezone':timezone,'symbol':symbol,'start_date':start_date,'end_date':end_date,'dp':dp,'format':format}
        json_data=json.dumps(self.run(endpoint='time_series',data=body).json())
        return json_data