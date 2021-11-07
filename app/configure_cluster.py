from control_datawarehouse import datawarehouse
import psycopg2

def main():
    """Initializes datawarehous object, saves endpoint+roleARN to config file and opens ports of VPC for connections"""

    MyDWH = datawarehouse()
    MyDWH.describe_redshift_cluster()
    MyDWH.get_cluster_endpoint_and_role_arn()
    MyDWH.open_ports()

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        MyDWH.DWH_ENDPOINT, MyDWH.DWH_DB, MyDWH.DWH_DB_USER, MyDWH.DWH_DB_PASSWORD, MyDWH.DWH_PORT))
    cur = conn.cursor()
    print('Connected')
    conn.close()
    print('Connection closed')

if __name__ == "__main__":
    main()