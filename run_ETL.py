import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from create_cluster import datawarehouse


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    MyDWH = datawarehouse()
    MyDWH.describe_redshift_cluster()
    MyDWH.get_cluster_endpoint_and_role_arn()
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        MyDWH.DWH_ENDPOINT, MyDWH.DWH_DB, MyDWH.DWH_DB_USER, MyDWH.DWH_DB_PASSWORD, MyDWH.DWH_PORT))
    cur = conn.cursor()
    print('Connected')
    print('Loading staging tables')
    load_staging_tables(cur, conn)
    print('Staging tables loaded')
    print('Inserting tables')
    insert_tables(cur, conn)
    print('Tables inserted')
    conn.close()
    print('Connection closed')

if __name__ == "__main__":
    main()