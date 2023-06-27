from operators.CloudantOperators import CreateDocumentOperator, \
    GetLastUpdateDateOperator, GetDocumentOperator
from operators.DataOperators import GetDataAPI
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum
from airflow.decorators import task_group
from custom_dags.dag_structure.setting_import import symbol_list


def update_info():
    group_stocks = []
    group_news = []
    for ticker in symbol_list:
        @task_group(group_id=f'update_{ticker}_news')
        def tg_get_transform_data_news(symbol=ticker):
            data_type = 'news'
            db = str(symbol + '_' + data_type).lower()
            get_last_date_and_rev = GetLastUpdateDateOperator(task_id=f"GetRev_{symbol}_{data_type}", db=db,
                                                          symbol=symbol, data_type=data_type)
            get_old_cloud_data = GetDocumentOperator(
                rev=f"{{{{ti.xcom_pull(task_ids='update_{ticker}_news.GetRev_{symbol}_{data_type}',key='last_rev_{symbol}_{data_type}')}}}}",
                task_id=f"GetOldDoc_{symbol}_{data_type}",
                data_type=data_type,
                db=db,
                symbol=symbol)
            get_new_data = GetDataAPI(
                start_date_api=f"{{{{ti.xcom_pull(task_ids='update_{ticker}_news.GetRev_{symbol}_{data_type}',key='last_update_{symbol}_{data_type}')}}}}",
                data_type=data_type, end_date=pendulum.now('UTC') \
                , task_id=f"GetNewAPIData_{symbol}_{data_type}", db=db, symbol=symbol)
            transform_data = SQLExecuteQueryOperator(sql='transform_news.sql', parameters={'symbol': symbol},
                                                    task_id=f"TransformNewData_{symbol}_{data_type}", autocommit=True)
            # TODO:Sql doesn't like BRK-B stock due to dash. Check how Twelvedata handles the ticker, might need to switch the variable to not have dash in SQL requests.
            merge_update = SQLExecuteQueryOperator(sql='merge_update.sql',
                                                  params={'view_name': str(symbol + '_' + data_type).lower(),
                                                          'table': data_type},
                                                  task_id=f"UpdateAndMerge_{symbol}_{data_type}", autocommit=True,
                                                  parameters={'ticker': symbol,
                                                              'ticker_old': str(symbol + '_old')})

            create_doc = CreateDocumentOperator(
                rev=f"{{{{ti.xcom_pull(task_ids='update_{ticker}_news.GetRev_{symbol}_{data_type}',key='last_rev_{symbol}_{data_type}')}}}}",
                data_type=data_type, task_id=f"CreateUpdatedDoc_{symbol}_{data_type}", db=db, symbol=symbol)

            drop_view = SQLExecuteQueryOperator(sql='delete_view.sql',
                                                  params={'view_name': str(symbol + '_' + data_type).lower(),
                                                          'table': data_type},
                                                  task_id=f"drop_view_{symbol}_{data_type}", autocommit=True)

            get_last_date_and_rev >> [get_old_cloud_data , get_new_data] >> transform_data >> merge_update >> create_doc >> drop_view

        group_news.append(tg_get_transform_data_news())
        @task_group(group_id=f'update_{ticker}_stocks')
        def tg_get_transform_data_stocks(symbol=ticker):
            data_type = 'stocks'
            db = str(symbol + '_' + data_type).lower()
            get_last_date_and_rev = GetLastUpdateDateOperator(task_id=f"GetRev_{symbol}_{data_type}", db=db,
                                                          symbol=symbol, data_type=data_type)
            get_old_cloud_data = GetDocumentOperator(
                rev=f"{{{{ti.xcom_pull(task_ids='update_{ticker}_stocks.GetRev_{symbol}_{data_type}',key='last_rev_{symbol}_{data_type}')}}}}",
                task_id=f"GetOldDoc_{symbol}_{data_type}",
                data_type=data_type, db=db, symbol=symbol)
            get_new_data = GetDataAPI(
                start_date_api=f"{{{{ti.xcom_pull(task_ids='update_{ticker}_stocks.GetRev_{symbol}_{data_type}',key='last_update_{symbol}_{data_type}')}}}}",
                data_type=data_type, end_date=pendulum.now('UTC') \
                , task_id=f"GetNewAPIData_{symbol}_{data_type}", db=db, symbol=symbol)
            #TODO:Sql doesn't like BRK-B stock due to dash. Check how Twelvedata handles the ticker, might need to switch the variable to not have dash in SQL requests.
            merge_update = SQLExecuteQueryOperator(sql='merge_update.sql',
                                                  params={'view_name': str(symbol + '_' + data_type).lower(),
                                                          'table': data_type},
                                                  task_id=f"UpdateAndMerge_{symbol}_{data_type}", autocommit=True,
                                                  parameters={'ticker': symbol,
                                                              'ticker_old': str(symbol + '_old')})

            create_doc = CreateDocumentOperator(
                rev=f"{{{{ti.xcom_pull(task_ids='update_{ticker}_stocks.GetRev_{symbol}_{data_type}',key='last_rev_{symbol}_{data_type}')}}}}",
                data_type=data_type, task_id=f"CreateUpdatedDoc_{symbol}_{data_type}", db=db, symbol=symbol)

            drop_view = SQLExecuteQueryOperator(sql='delete_view.sql',
                                                  params={'view_name': str(symbol + '_' + data_type).lower(),
                                                          'table': data_type},
                                                  task_id=f"drop_view_{symbol}_{data_type}", autocommit=True)

            get_last_date_and_rev >> [get_old_cloud_data, get_new_data] >> merge_update >> create_doc >> drop_view
        group_stocks.append(tg_get_transform_data_stocks())

    [group_news,group_stocks]
