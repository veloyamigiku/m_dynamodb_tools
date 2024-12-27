import boto3
from datetime import datetime
from datetime import timezone
import os
import pickle
from tqdm import tqdm

def get_sts_client(
    aws_access_key_id: str,
    aws_secret_access_key: str
):

  sts_client = boto3.client(
      'sts',
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key
  )

  return sts_client


def get_session_token(
    sts_client,
    serial_number: str,
    token_code: str
):
  response = sts_client.get_session_token(
      DurationSeconds=900,
      SerialNumber=serial_number,
      TokenCode=token_code
  )
  credentials = response['Credentials']
  return credentials


def init_credential(
    aws_access_key_id: str,
    aws_secret_access_key: str,
    serial_number: str,
    temp_credential_path: str = './.credential.pkl'
):
  temp_credential = None
  if os.path.exists(temp_credential_path):
    with open(temp_credential_path, mode='rb') as f:
      temp_credential = pickle.load(f)

    now_dt = datetime.now(timezone.utc)
    if temp_credential['expiration'] < now_dt:
      temp_credential = None

  if temp_credential is None:
    temp_credential = get_temp_credential(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        serial_number=serial_number)
  with open(temp_credential_path, mode='wb') as fw:
    pickle.dump(temp_credential, fw)

  return temp_credential


def get_temp_credential(
    aws_access_key_id: str,
    aws_secret_access_key: str,
    serial_number: str,
):

  aws_access_key_id = os.environ.get('_AWS_ACCESS_KEY_ID')
  aws_secret_access_key = os.environ.get('_AWS_SECRET_ACCESS_KEY')
  sts_client = get_sts_client(
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key
  )

  serial_number = os.environ.get('SERIAL_NUMBER')
  print('token_code:', end='')
  token_code = input()
  credentials = get_session_token(
      sts_client=sts_client,
      serial_number=serial_number,
      token_code=token_code
  )
  temp_access_key_id = credentials['AccessKeyId']
  temp_secret_access_key = credentials['SecretAccessKey']
  session_token = credentials['SessionToken']
  expiration = credentials['Expiration']

  return {
      'temp_access_key_id': temp_access_key_id,
      'temp_secret_access_key': temp_secret_access_key,
      'session_token': session_token,
      'expiration': expiration
  }


def create_table(
        resource,
        table_name: str):
  table = resource.create_table(
      TableName=table_name,
      BillingMode='PAY_PER_REQUEST',
      KeySchema=[
          {
              'AttributeName': 'id',
              'KeyType': 'HASH'
          },
          {
              'AttributeName': 'datatype',
              'KeyType': 'RANGE'
          }
      ],
      AttributeDefinitions=[
          {
              'AttributeName': 'id',
              'AttributeType': 'S'
          },
          {
              'AttributeName': 'datatype',
              'AttributeType': 'S'
          },
          {
              'AttributeName': 'data_str',
              'AttributeType': 'S'
          },
          {
              'AttributeName': 'data_num',
              'AttributeType': 'N'
          },
      ],
      GlobalSecondaryIndexes=[
        {
          'IndexName': 'GSI-1',
          'KeySchema': [
            {
              'AttributeName': 'datatype',
              'KeyType': 'HASH'
            },
            {
              'AttributeName': 'data_str',
              'KeyType': 'RANGE'
            }
          ],
          'Projection': {
            'ProjectionType': 'KEYS_ONLY'
          }
        },
        {
          'IndexName': 'GSI-2',
          'KeySchema': [
            {
              'AttributeName': 'datatype',
              'KeyType': 'HASH'
            },
            {
              'AttributeName': 'data_num',
              'KeyType': 'RANGE'
            }
          ],
          'Projection': {
            'ProjectionType': 'KEYS_ONLY'
          }
        },
      ]
  )

  table.wait_until_exists()

  print('table.item_count: ', table.item_count)


def get_table(
    resource,
    table_name: str
):
  table = resource.Table(table_name)
  print(table.creation_date_time)
  return table

def delete_table(
        resource,
        table_name: str):

  table = resource.Table(table_name)

  print(table.creation_date_time)
  print('delete table?[yes/no]:', end='')
  yes_no = input()
  if yes_no == 'yes':
    table.delete()

def item_generator(
  item_csv_path: str
):
  item_size = 0
  with open(item_csv_path, mode='r', encoding='utf-8-sig') as f:
    f.readline()
    for _ in f:
      item_size = item_size + 1
  
  with tqdm(total=item_size) as pbar, \
    open(item_csv_path, mode='r', encoding='utf-8-sig') as f:
    header_names = f.readline().strip().split(',')
    header_name_id_map = {header_name: idx for idx, header_name in enumerate(header_names)}
    for line in f:
      pbar.update(1)
      line_parts = line.strip().split(',')
      id = line_parts[header_name_id_map['id']]
      datatype = line_parts[header_name_id_map['datatype']]
      data_col_name = line_parts[header_name_id_map['data_col_name']]
      data = line_parts[header_name_id_map['data']]
      if data_col_name == 'data_str':
        data = str(data)
      elif data_col_name == 'data_num':
        data = float(data)
      yield {
        'id': id,
        'datatype': datatype,
        data_col_name: data,
      }
