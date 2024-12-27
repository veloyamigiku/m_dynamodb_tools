import argparse
import boto3
from dotenv import load_dotenv
import os

from utils import create_table
from utils import init_credential
from utils import item_generator

def main(args):

  load_dotenv(verbose=True)

  aws_access_key_id = os.environ.get('_AWS_ACCESS_KEY_ID')
  aws_secret_access_key = os.environ.get('_AWS_SECRET_ACCESS_KEY')
  serial_number = os.environ.get('SERIAL_NUMBER')
  region_name = os.environ.get('REGION_NAME')

  temp_credential = init_credential(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    serial_number=serial_number
  )

  dynamodb_resource = boto3.resource(
    'dynamodb',
    aws_access_key_id=temp_credential['temp_access_key_id'],
    aws_secret_access_key=temp_credential['temp_secret_access_key'],
    aws_session_token=temp_credential['session_token'],
    region_name=region_name
  )

  # テーブルを作成する。
  """
  create_table(
    resource=dynamodb_resource,
    table_name=args.table_name
  )
  """

  for item in item_generator(args.item_csv_path):
    print(item)
  print('kokomade')
  

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--table_name', type=str, default=None)
  parser.add_argument('--item_csv_path', type=str, default=None)
  args = parser.parse_args()
  main(args=args)
