from airflow.models.baseoperator import BaseOperator
from include.hooks.NewsApi import NewsDataHook
from include.hooks.TwelveData import TwelveDataHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
from psycopg2.sql import Identifier,SQL
from pendulum import from_format
from dotenv import load_dotenv
load_dotenv()

class GetDataAPI(BaseOperator):
    template_fields = ['start_date_api']
    def __init__(self, db: str,data_type: str, symbol: str, postgres_conn_id: str, start_date_api=from_format(os.environ['start_date'], 'YYYY-MM-DD HH:mm:ss').isoformat(), end_date=from_format(os.environ['end_date'], 'YYYY-MM-DD HH:mm:ss').isoformat(), **kwargs) -> None:
        super().__init__(**kwargs)
        self.db =  db
        self.symbol = symbol
        self.postgres_conn_id =postgres_conn_id
        self.start_date_api = start_date_api
        self.end_date_api = end_date
        self.data_type= data_type
    def execute(self, context):
        if self.data_type=='news':
            self.api_conn_id = 'NEWSAPI'
            hook=NewsDataHook(newsapi_conn_id=self.api_conn_id)
        elif self.data_type=='stocks':
            self.api_conn_id = 'TWELVEDATA'
            hook=TwelveDataHook(twelvedata_conn_id=self.api_conn_id)
        else:
            print("Un-avaliable connection ID. Currently supported: NEWSAPI, TWELVEDATA")

        hook_postgr=PostgresHook(postgres_conn_id=self.postgres_conn_id,database='localstorage')
        conn=hook_postgr.get_conn()
        cursor=conn.cursor()

        data=hook.get_data(symbol=self.symbol, start_date=self.start_date_api, end_date=self.end_date_api)

        sql_insert = SQL("""INSERT INTO {table} (ticker,info) VALUES (%(symbol)s,%(data)s) ON CONFLICT (ticker) DO UPDATE SET info=excluded.info;""").format(table=Identifier(self.data_type))
        cursor.execute(sql_insert,vars={'symbol':self.symbol,'data':data})
        conn.commit()
        return None
