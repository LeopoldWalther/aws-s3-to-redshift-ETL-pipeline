from create_cluster import datawarehouse

def main():
    """Initializes datawarehouse object and checks status of redshift cluster"""

    MyDWH = datawarehouse()
    MyDWH.describe_redshift_cluster()

if __name__ == "__main__":
    main()