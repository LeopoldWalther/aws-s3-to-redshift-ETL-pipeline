from control_datawarehouse import datawarehouse
import psycopg2

def main():
    """Initializes datwarehouse object and deletes the redshift cluster and IAM role"""

    MyDWH = datawarehouse()
    MyDWH.delete_redshift_cluster()
    MyDWH.delete_IAM()

if __name__ == "__main__":
    main()