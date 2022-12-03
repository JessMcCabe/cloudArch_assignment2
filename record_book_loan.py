
# This function is triggered by an object being created in an Amazon S3 bucket.
# The file is downloaded and each line is inserted into a DynamoDB table.
# The book is loaned to a customer so a record is added to the books_on_loan table
import json, urllib, boto3, csv
# Connect to S3 and DynamoDB
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
# Connect to the DynamoDB tables
booksOnLoanTable = dynamodb.Table('books_on_loan');
# This handler is run every time the Lambda function is triggered
def lambda_handler(event, context):

  # Show the incoming event in the debug log
  print("Event received by Lambda function: " + json.dumps(event, indent=2))
  # Find the s3 bucket ARN 
  for record in event['Records']:

    #pull the body out & json load it
    jsonmaybe = record['body']
    jsonmaybe = json.loads(jsonmaybe)
    print('print jsonmaybe')
    print(jsonmaybe)
    bucket_name = jsonmaybe["Records"][0]["s3"]["bucket"]["name"]
    print('printing bucket name:')
    print(bucket_name)
    key = urllib.parse.unquote_plus(jsonmaybe['Records'][0]['s3']['object']['key'])
    localFilename = '/tmp/loan.txt'
    # Download the file from S3 to the local filesystem
    try:
      s3.meta.client.download_file(bucket_name, key, localFilename)
    except Exception as e:
      print(e)
      print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket_name))
      raise e
      # Read the Inventory CSV file
    with open(localFilename) as csvfile:
     reader = csv.DictReader(csvfile, delimiter=',')
     # Read each row in the file
     rowCount = 0
     for row in reader:
       rowCount += 1
      # Show the row in the debug log
       print(row['ISBN'], row['cust_id'], row['return_date'])
       try:
        # Insert Store, Item and Count into the Inventory table
         booksOnLoanTable.put_item(
            Item={
              'ISBN':  row['ISBN'],
              'cust_id':   row['cust_id'],
             'return_date':  row['return_date']})
       except Exception as e:
          print(e)
          print("Unable to insert data into DynamoDB table".format(e))
    # Finished!
     return "%d counts inserted" % rowCount