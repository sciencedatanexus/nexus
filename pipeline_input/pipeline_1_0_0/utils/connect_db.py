# coding=utf-8

# =============================================================================
# """
# .. module:: input_pipeline.utils.connect_db.py
# .. moduleauthor:: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# .. version:: 1.0
#
# :Copyright: Jean-Francois Desvignes for Science Data Nexus, 2024
# :Contact: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# :Updated: 19/08/2024
# """
# =============================================================================

# Import the local functions module (file)
from sqlalchemy import create_engine, text
from requests.utils import quote


def create_connection_to_postgresql(configuration, driver='sql', configfile="D:\\MyData\config.ini"):
    # connection function to the database using sqlalchemy
    try:
        db_config = read_db_config(configuration, filename=configfile)
        if driver == 'snowflake':
            engine_db = create_engine(
                'snowflake://{user}:{password}@{account_identifier}/?warehouse={warehouse}'.format(
                    user=db_config['user'],
                    password=quote(db_config['password']),
                    account_identifier=db_config['account_identifier'],
                    warehouse='IP'
                )
            )
        else:
            engine_db = create_engine(
                'postgresql+psycopg2://{}:{}@{}/{}'.format(quote(db_config['user']), quote(db_config['password']),
                                                           quote(db_config['host']), quote(db_config['database'])),
                echo=False)
        connection_db = engine_db.connect()
    finally:
        return connection_db


def exec_sql_file(file_name, connection_db):
    with open(file_name) as file:
        query = text(file.read()).execution_options(autocommit=True)
        connection_db.execute(query)


def exec_sql(query_sql, connection_db):
    query_sql = text(query_sql).execution_options(autocommit=True)
    connection_db.execute(query_sql)

