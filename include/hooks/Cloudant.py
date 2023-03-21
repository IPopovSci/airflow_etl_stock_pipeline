from ibm_cloud_sdk_core import ApiException
from ibmcloudant.cloudant_v1 import Document, CloudantV1, DesignDocument, DesignDocumentViewsMapReduce, \
    SearchIndexDefinition
from airflow.hooks.base import BaseHook
import os
from dotenv import load_dotenv
load_dotenv()

class CloudantHook(BaseHook):
    conn_name_attr = "cloudant_conn_id"
    default_conn_name = "CLOUDANT"
    conn_type = "cloudant"
    hook_name = "Cloudant API"

    def __init__(self, cloudant_conn_id: str = 'CLOUDANT') -> None:
        super().__init__()
        self.cloudant_conn_id = cloudant_conn_id

    def get_conn(self):
        conn = self.get_connection(conn_id=self.cloudant_conn_id)
        # This is needed for api interface to work
        os.environ['CLOUDANT_URL'] = conn.login
        os.environ['CLOUDANT_APIKEY'] = conn.password

        cloudant_session = CloudantV1.new_instance()
        return cloudant_session

    def create_db(self, db: str) -> str:
        try:
            self.get_conn().put_database(
                db=db
            ).get_result()
            return f'Database {db} created Successfully'
        except ApiException as ae:
            if ae.code == 412:
                raise (ApiException(message=f'Cannot create "{db}" database, ' +
                                            'it already exists', code=412))
            if ae.code == 400:
                raise (ApiException(message="illegal database name", code=400))
            else:
                raise (ApiException(message=f"Error Occured", code=ae.code))

    def create_document(self, db, data, symbol, rev) -> str:
        try:
            document = Document.from_dict(data)
            self.get_conn().put_document(
                db=db,
                doc_id=symbol,
                document=document,
                rev=rev
            ).get_result()
            return print(f"Created document for ticker {symbol} successfully")
        except ApiException as ae:
            if ae.code == 409:
                raise (ApiException(message=f'Cannot create "{db}" document, ' +
                                            'it already exists', code=409))
            else:
                raise (ApiException(message=f"Error Occured", code=ae.code))

    def create_datetime_view(self, db: str, symbol: str) -> str:
        try:
            datetime_view_map_reduce = DesignDocumentViewsMapReduce(
                map="function(doc) {emit('Dates',doc.values.map(function(datetime) {return datetime.datetime}))}"
            )
            doc_name_index = SearchIndexDefinition(
                index='function (doc) { index("Ticker", doc._id)}') #Do we need an index?

            design_document = DesignDocument(
                views={f'Get{symbol}Dates': datetime_view_map_reduce},
                indexes={'Ticker Name': doc_name_index}
            )
            response = self.get_conn().put_design_document(
                db=db,
                design_document=design_document,
                ddoc=f"Get{symbol}DatesByName"
            ).get_result()
            return response

        except ApiException as ae:
            raise (ApiException(message=f"Error Occured", code=ae.code))

    def post_view(self,db: str,symbol: str) -> str:
        try:
            response = self.get_conn().post_view(
              db=db,
              ddoc=f"Get{symbol}DatesByName",
              view=f'Get{symbol}Dates',key='Dates',update=True,descending=True
            ).get_result()
            return response
        except ApiException as ae:
            raise(ApiException(message=f"Error Occured",code=ae.code))

    def get_document(self,db: str,symbol: str) -> str:
        try:
            response = self.get_conn().get_document(
                db=db,
                doc_id=symbol,attachments=True
            ).get_result()

            return response
        except ApiException as ae:
            raise(ApiException(message=f"Error Occured",code=ae.code))
