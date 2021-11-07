import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from create_cluster import datawarehouse


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
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
    print('Dropping tables')
    drop_tables(cur, conn)
    print('Tables dropped')
    print('Creating tables')
    create_tables(cur, conn)
    print('Tables created')
    conn.close()
    print('Connection closed')


if __name__ == "__main__":
    main()