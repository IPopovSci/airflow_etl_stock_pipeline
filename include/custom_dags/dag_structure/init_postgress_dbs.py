from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator



def initialize_postgress():

    create_post_db = SQLExecuteQueryOperator(sql='create_database_local.sql', task_id='CreateLocalDB', autocommit=True,database=None)
    create_table_news = SQLExecuteQueryOperator(sql='create_table.sql',
                                           params={"table": 'news'}, task_id=f'create_table_news')
    create_table_stocks = SQLExecuteQueryOperator(sql='create_table.sql',
                                           params={"table": 'stocks'}, task_id=f'create_table_stocks')

    create_post_db>>[create_table_news,create_table_stocks]