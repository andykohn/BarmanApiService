import logging
import boto3
#import psycopg2
from . import KMS_encrypt_decrypt
import records

HOST = 'drinks-db.ce6hlg7jz41w.us-east-1.rds.amazonaws.com'
DATABASE = 'drinks_db'
USER = 'drinks_user'
PORT = '8200'


class PostgresProcessor:

    def __init__(self, sql):
        self.sql = sql

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
        self.__my_logging_handler('DEBUG-1-1-1')
        decrypted_pass = KMS_encrypt_decrypt.KMSEncryptDecrypt.decrypt_data(credential.value)
        self.__my_logging_handler('DEBUG-1-1-2')
        decrypted_pass = decrypted_pass.decode("utf-8")
        #conn = psycopg2.connect(host=HOST, database=DATABASE, port=PORT, user=USER, password=decrypted_pass)
        url = 'postgresql://{}:{}/{}?user={}&password={}'.format(HOST, PORT, DATABASE, USER, decrypted_pass)
        self.__my_logging_handler('DEBUG-1-1-3')
        db = records.Database(url)
        self.__my_logging_handler('DEBUG-1-1-4')
        # conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        #return conn, db
        return db

    def execute_sql(self):
        try:
            #conn, db = self.__connect()
            # print("Processing {}".format(self.__entity.sql_statement()))
            #cur = conn.cursor()
            #cur.execute(self.__entity.sql_statement())
            #conn.commit()
            #rows = cur.fetchall()
            #cur.close()
            #conn.close()
            self.__my_logging_handler('DEBUG-1-1')
            db = self.__connect()
            self.__my_logging_handler('DEBUG-1-2')
            # records imp
            rec_rows = db.query(self.sql)
            self.__my_logging_handler('DEBUG-1-3')
            return rec_rows
        except Exception as error:
            raise

    @staticmethod
    def __my_logging_handler(event):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.info('DEBUG:{}'.format(event))
