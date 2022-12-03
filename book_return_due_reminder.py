s3
# This function should be triggered by a daily event created in eventbridge
from datetime import datetime
import json, boto3
# This handler is run every time the Lambda function is triggered

dynamodb = boto3.resource('dynamodb')
# Connect to the DynamoDB tables
booksOnLoanTable = dynamodb.Table('books_on_loan');
customerTable = dynamodb.Table('customer');
def lambda_handler(event, context):
  # Show the incoming event in the debug log
  #print("Event received by Lambda function: " + json.dumps(event, indent=2))
  
  try:
    for item in booksOnLoanTable.scan()['Items']:
      print (item)
      if item['return_date'] == datetime.today().strftime('%Y-%m-%d'):
        print('printing from IF')
        print(item)
        print('printing cust id')
        customerId = item['cust_id']
        print(customerId)

        for item in customerTable.scan()['Items']:
          print('Item in customer')
          if int(item['id']) == int(customerId):
            print('matches customer')
            email=item['email']
            print('customer who is due a return is:')
            print(email)

        
          
        
        #print('printing email:')
        #print(email)
        message = 'Book due today'
        print(message)  
        print('Date time is:')
        print( datetime.today().strftime('%Y-%m-%d'))
        #print(item)
      
      # Connect to SNS
        sns = boto3.client('sns')
        alertTopic = 'book_return_due'
        snsTopicArn = [t['TopicArn'] for t in sns.list_topics()['Topics']
                        if t['TopicArn'].lower().endswith(':' + alertTopic.lower())][0]  
      # Send message to SNS
        sns.publish(
        TopicArn=snsTopicArn,
        Message=message,
        Subject='Book Return due today',
        MessageStructure='raw'
      )
  except Exception as e:
  # Finished!
    return 'Successfully processed  function'