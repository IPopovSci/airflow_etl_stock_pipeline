from include.operators.CloudantOperators import CreateDatabaseOperator, CreateDocumentOperator, CreateDateTimeViewOperator
from include.operators.DataOperators import GetDataAPI
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task_group
from include.custom_dags.dag_structure.setting_import import symbol_list

def initialize_pipeline():
    group_news=[]
    group_stocks=[]

    for ticker in symbol_list:
        @task_group(group_id=f'populate_{ticker}_news')
        def tg_init_cloud_db_news(symbol=ticker):
            data_type = 'news'

            db = str(symbol + '_' + data_type).lower()

            create_cloud_db = CreateDatabaseOperator(db=db,
                                                   task_id=f"CreateDB_{symbol}_{data_type}")

            get_data = GetDataAPI(data_type=data_type, task_id=f"GetAPIData_{db}", db=db,
                                 symbol=symbol)

            transform_data = SQLExecuteQueryOperator(sql='transform_news.sql', parameters={'symbol': symbol},
                                                    task_id=f"TransformData_{symbol}_{data_type}", autocommit=True)

            create_doc = CreateDocumentOperator(task_id=f"CreateDoc_{symbol}_{data_type}",
                                               data_type=data_type,
                                               db=db, symbol=symbol)

            create_view = CreateDateTimeViewOperator(task_id=f"CreateDateTimeView_{symbol}_{data_type}",
                                                    db=db, symbol=symbol)

            create_cloud_db >> get_data >> transform_data >> create_doc >> create_view
        group_news.append(tg_init_cloud_db_news())

        @task_group(group_id=f'populate_{ticker}_stocks')
        def tg_init_cloud_db_stocks(symbol=ticker):
            data_type = 'stocks'

            db = str(symbol + '_' + data_type).lower()

            create_cloud_db = CreateDatabaseOperator(db=db,
                                                   task_id=f"CreateDB_{symbol}_{data_type}")

            get_data = GetDataAPI(data_type=data_type, task_id=f"GetAPIData_{db}", db=db,
                                 symbol=symbol)

            create_doc = CreateDocumentOperator(task_id=f"CreateDoc_{symbol}_{data_type}",
                                               data_type=data_type,
                                               db=db, symbol=symbol)

            create_view = CreateDateTimeViewOperator(task_id=f"CreateDateTimeView_{symbol}_{data_type}",
                                                    db=db, symbol=symbol)

            create_cloud_db >> get_data >> create_doc >> create_view
        group_stocks.append(tg_init_cloud_db_stocks())

    [group_stocks,group_news]