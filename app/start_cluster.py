from control_datawarehouse import datawarehouse

def main():
    """Initializes the datawarehouse object, creates an IAM role and start a redshift cluster"""

    MyDWH = datawarehouse()
    MyDWH.create_iam_role()
    MyDWH.create_redshift_cluster()
    print('Please wait until cluster in status available, check status with describe_cluster.py')

if __name__ == "__main__":
    main()