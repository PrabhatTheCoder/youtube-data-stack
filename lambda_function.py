import awswrangler as wr
import pandas as pd
import urllib.parse
import os

os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name'] 
os_input_write_data_operation = os.environ['write_data_operation'] 


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("bucket", bucket)
    print("key", key)
    
    try:
        df_raw = wr.s3.read_json(f's3://{bucket}/{key}')
        
        df_step_1 = pd.json_normalize(df_raw['items'])

        print("Raw DataFrame:", df_raw)
        print("Raw items key:", df_raw.get('items'))


        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_cleansed_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation 
        )

        return {
            'statusCode': 200,
            'body': 'Successfully processed and written to S3',
            'details': wr_response
        }

    except Exception as e:
        print("Exception occurred:", str(e))
        raise RuntimeError(
            f"Failed to process object {key} from bucket {bucket}: {str(e)}"
        ) from e

