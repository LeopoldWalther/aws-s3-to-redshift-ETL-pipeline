import configparser
import pandas as pd
import boto3
import json
from botocore.exceptions import ClientError
import psycopg2


class datawarehouse(object):
    """A class to setup an AWS Data Warehouse."""

    def __init__(self):
        """Initialize the Data Warehouse, parse config file and create clients."""

        # Load DWH Params from a file
        config = configparser.ConfigParser()
        config.read_file(open('dwh.cfg'))

        self.KEY                    = config.get('AWS','KEY')
        self.SECRET                 = config.get('AWS','SECRET')
        self.DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
        self.DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
        self.DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
        self.DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
        self.DWH_DB                 = config.get("DWH","DWH_DB")
        self.DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
        self.DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
        self.DWH_PORT               = config.get("DWH","DWH_PORT")
        self.DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

        ## Create clients for EC2, S3, IAM, and Redshift
        self.ec2 = boto3.resource('ec2',
                            region_name='us-west-2',
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET
                        )

        self.s3 = boto3.resource('s3',
                        region_name='us-west-2',
                        aws_access_key_id=self.KEY,
                        aws_secret_access_key=self.SECRET
                        ) 

        self.iam = boto3.client('iam',
                        region_name='us-west-2',
                        aws_access_key_id=self.KEY,
                        aws_secret_access_key=self.SECRET
                        )

        self.redshift = boto3.client('redshift',
                            region_name='us-west-2',
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET
                            )


    def create_iam_role(self):
        """Create the IAM role to allow the Redshift cluster to calls AWS services."""
        # 1.1 Create the IAM role
        try:
            print("1.1 Creating a new IAM Role") 
            dwhRole = self.iam.create_role(
                Path='/',
                RoleName=self.DWH_IAM_ROLE_NAME,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
            )    
        except Exception as e:
            print(e)
            
        print("1.2 Attaching Policy")
        self.iam.attach_role_policy(RoleName=self.DWH_IAM_ROLE_NAME,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']

        print("1.3 Get the IAM role ARN")
        self.roleArn = self.iam.get_role(RoleName=self.DWH_IAM_ROLE_NAME)['Role']['Arn']


    def create_redshift_cluster(self):
        """Create the redshift cluster using the client and configuration information"""
        # Step 2: Create Redshift Cluster
        try:
            response = self.redshift.create_cluster(        

                #HW
                ClusterType=self.DWH_CLUSTER_TYPE,
                NodeType=self.DWH_NODE_TYPE,
                NumberOfNodes=int(self.DWH_NUM_NODES),

                #Identifiers & Credentials
                DBName=self.DWH_DB,
                ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
                MasterUsername=self.DWH_DB_USER,
                MasterUserPassword=self.DWH_DB_PASSWORD,
                
                #Roles (for s3 access)
                IamRoles=[self.roleArn]  
            )
        except Exception as e:
            print(e)
    

    def get_cluster_endpoint_and_role_arn(self):
        """Method to query the Data Warhouse endpoint and IAM roles."""

        def prettyRedshiftProps(props):
            pd.set_option('display.max_colwidth', None)
            keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
            x = [(k, v) for k,v in props.items() if k in keysToShow]
            return pd.DataFrame(data=x, columns=["Key", "Value"])

        # 2.1 *Describe* the cluster to see its status
        myClusterProps = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        prettyRedshiftProps(myClusterProps)

        # 2.2 Take note of the cluster endpoint and role ARN
        self.DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        self.DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']


    def open_ports(self):
        """Open the ports of the virtual privat cloud for incoming and outgoing traffic."""

        # STEP 3: Open an incoming  TCP port to access the cluster ednpoint
        try:
            vpc = self.ec2.Vpc(id=self.myClusterProps['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            print(defaultSg)
            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(self.DWH_PORT),
                ToPort=int(self.DWH_PORT)
            )
        except Exception as e:
            print(e)


def main():
    """Main function to create Data Warehouse and check if connection works properly."""
    MyDatawarehouse = datawarehouse()
    datawarehouse.create_iam_role()
    datawarehouse.create_redshift_cluster()
    datawarehouse.get_cluster_endpoint_and_role_arn()
    datawarehouse.open_ports()

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected')
    conn.close()



if __name__ == "__main__":
    main()