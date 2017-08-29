import logging
import boto3
import psycopg2
from . import KMS_encrypt_decrypt

HOST = 'drinks-db.ce6hlg7jz41w.us-east-1.rds.amazonaws.com'
DATABASE = 'drinks_db'
USER = 'drinks_user'
PORT = '8200'


class PostgresProcessor:

    def __init__(self, entity):
        self.__client = boto3.client('athena')
        self.__entity = entity

    def __get_password_from_dynamo(self):
        dynamodb = boto3.resource("dynamodb", region_name='us-east-1',
                                  endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
        table = dynamodb.Table('credentials')
        response = table.get_item(
            Key={
                'name': 'rds-drinks_db-drinks_user',
            }
        )
        if 'Item' in response:
            if 'encrypted_password' in response['Item']:
                a = response['Item']['encrypted_password']
                return response['Item']['encrypted_password']
            else:
                return "No such attribute : encrypted_password"
        else:
            return "No key found"

    def __connect(self):
        credential = self.__get_password_from_dynamo()
        decrypted = KMS_encrypt_decrypt.KMSEncryptDecrypt.decrypt_data(credential.value)
        decrypted = decrypted.decode("utf-8")
        conn = psycopg2.connect(host=HOST, database=DATABASE, port=PORT, user=USER, password=decrypted)
        # conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def execute_sql(self):
        try:
            conn = self.__connect()
            # print("Processing {}".format(self.__entity.sql_statement()))
            cur = conn.cursor()
            cur.execute(self.__entity.sql_statement())
            conn.commit()
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return rows
        except Exception as error:
            print(error)

    @staticmethod
    def __my_logging_handler(event):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.info('DEBUG:{}'.format(event))
