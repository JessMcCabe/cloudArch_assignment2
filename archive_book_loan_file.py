
# This function is triggered after an SNS notification is sent
# The purpose of this is to archive the book_loan file to another s3 bucket
import  urllib, boto3 
from datetime import datetime
# Connect to S3 and DynamoDB
s3 = boto3.resource('s3')
bucket_name="library-system-11-19-2022"
date_time= datetime.now()
archive_bucket="archive/book_loan_" + date_time.strftime("%c") + ".txt"
fileName="library-system-11-19-2022/book_loan.txt"
file="book_loan.txt"
# This handler is run every time the Lambda function is triggered
def lambda_handler(event, context):

  
  # Find the s3 bucket ARN 
  

    
  try:
      # Copy object A as object B
    s3.Object(bucket_name, archive_bucket).copy_from(
    CopySource=fileName)
    # Delete the former object A
    s3.Object(bucket_name, file).delete()
  except Exception as e:
      print(e)
      print('Error getting object  from bucket ')
      raise e
      
    # Finished!
  return 'Success'