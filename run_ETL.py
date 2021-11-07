import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, check_tables_queries
from create_cluster import datawarehouse


def load_staging_tables(cur, conn):
    """Loops throgh all queries to load data from AWS S3 into redshift cluster"""
    for query in copy_table_queries:
        print('Running ' + query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Loops through all queries to insert data from staging tables into OLAP tables"""
    for query in insert_table_queries:
        print('Running ' + query)
        cur.execute(query)
        conn.commit()

def check_tables(cur, conn):
    """Loops through all queries to check if tables were inserted correctly"""
    for query in check_tables_queries:
        print('Running ' + query)
        cur.execute(query)
        results = cur.fetchall()
        for row in results:
            print("   ", row)


def main():
    """Connects to previously created redshift cluster, loads staging tables from AWS S3 and inserts data into tables"""
    
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
    print('Query tables')
    check_tables(cur, conn)
    print('Tables checked')
    conn.close()
    print('Connection closed')

if __name__ == "__main__":
    main()