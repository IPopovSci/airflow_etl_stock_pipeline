from airflow.models.baseoperator import BaseOperator
from include.hooks.Cloudant import CloudantHook
import json
import os
from psycopg2.sql import Identifier,SQL
from dotenv import load_dotenv
from airflow.providers.postgres.hooks.postgres import PostgresHook
load_dotenv()

class CreateDatabaseOperator(BaseOperator):
    def __init__(self, db: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.db = db

    def execute(self, context):
        hook = CloudantHook()
        message = hook.create_db(db=self.db)
        return None

class CreateDocumentOperator(BaseOperator): #data_type Stock or News
    template_fields = ['rev']
    def __init__(self, db: str, data_type: str, symbol: str,postgres_conn_id: str, rev: str = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.db =  db
        self.data_type = data_type
        self.symbol = symbol
        self.rev = rev
        self.postgres_conn_id=postgres_conn_id
    def execute(self,context):
        hook = CloudantHook()

        hook_postgr=PostgresHook(postgres_conn_id=self.postgres_conn_id,database='localstorage')
        conn=hook_postgr.get_conn()
        cursor=conn.cursor()

        sql_select = SQL("select info from {} where ticker=%s").format(Identifier(self.data_type))
        cursor.execute(sql_select,[str(self.symbol).upper()])

        self.data = cursor.fetchall()[0][0]

        message = hook.create_document(db=self.db,data=self.data,symbol=self.symbol,rev=self.rev)
        return None

class CreateDateTimeViewOperator(BaseOperator):
    def __init__(self, db: str, symbol: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.db =  db
        self.symbol = symbol
    def execute(self,context):
        hook = CloudantHook()
        message = hook.create_datetime_view(db=self.db,symbol=self.symbol)
        return None

class GetLastUpdateDateOperator(BaseOperator):
    def __init__(self, db: str, symbol: str,data_type: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.data_type = data_type
        self.db =  db
        self.symbol = symbol
        self.do_xcom_push = True

    def execute(self,context):
        hook = CloudantHook()
        message = hook.post_view(db=self.db,symbol=self.symbol)

        try:
            last_update = message['rows'][0]['value'][0]
        except: #Probably no file, so set a default date
            last_update = os.environ['start_date']

        try:
            rev = hook.get_document(db=self.db,symbol=self.symbol)['_rev']
        except: #Either document doesn't exist, or some problem occured
            rev = None

        self.xcom_push(context,f'last_rev_{self.symbol}_{self.data_type}',rev)
        self.xcom_push(context,f'last_update_{self.symbol}_{self.data_type}',last_update)

        return None

class GetDocumentOperator(BaseOperator): #data_type stock or News
    template_fields = ['rev']
    def __init__(self, db: str, data_type: str, symbol: str,postgres_conn_id: str, rev: str = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.db =  db
        self.data_type = data_type
        self.symbol = symbol
        self.rev = rev
        self.postgres_conn_id = postgres_conn_id


    def execute(self,context):
        print(self.rev)
        hook = CloudantHook()
        old_data=json.dumps(hook.get_document(db=self.db,symbol=self.symbol))

        hook_postgr=PostgresHook(postgres_conn_id=self.postgres_conn_id,database='localstorage')
        conn=hook_postgr.get_conn()
        cursor=conn.cursor()

        sql_insert = SQL("INSERT INTO {table} (ticker,info) VALUES (%(oldtick)s,%(data)s) ON CONFLICT (ticker) DO UPDATE SET info=excluded.info;").format(table=Identifier(self.data_type)) #TODO: Fix this,not propoer
        cursor.execute(sql_insert,vars={'oldtick':self.symbol + '_old','data':old_data})
        conn.commit()

        return None