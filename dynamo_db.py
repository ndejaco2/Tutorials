import calendar
import random
import string
import time

import boto3

from product import Product


class DynamoDBProduct(Product):
    """
    Save product data into dynamo db

    :param table_name: name of table, set to :code:`"Products"`
    :type table_name: str
    :param client: aws client, set as dynamodb
    :type client: boto3.client


    .. todo::
        should we have a table class instead?
    """
    def __init__(self, *args):
        Product.__init__(self, *args)
        self.table_name = "Products"
        self.client = boto3.client(service_name='dynamodb')

    def putItem(self):
        """Save table in dynamo db?. Print response"""
        try:
            response = self.client.put_item(TableName=self.table_name, Item=self.getItem(),
                                            ReturnConsumedCapacity='TOTAL')
            print(response)
            return True
        except Exception as e:
            print(e)
            return False

    def createTable(self):
        """Create table"""
        try:
            response = self.client.create_table(AttributeDefinitions=Product.getKeyAttributeDefs(),
                                                TableName=self.table_name,
                                                KeySchema=Product.getKeySchema(), BillingMode='PAY_PER_REQUEST')
            responseDict = dict(response)
            print("Successfully created table " + responseDict['TableDescription']['TableName'])
            return True
        except Exception as e:
            print(e)
            return False

    def deleteTable(self):
        """Delete table"""
        try:
            response = self.client.delete_table(TableName=self.table_name)
        except Exception as e:
            print(e)

    def ensureTableDeleted(self):
        while True:
            try:
                response = self.client.describe_table(TableName=self.table_name)
                time.sleep(2)
            except Exception as e:
                break

    def ensureTableActive(self):
        while True:
            response = self.client.describe_table(TableName=self.table_name)
            responseDict = dict(response)
            if responseDict['Table']['TableStatus'] == 'ACTIVE':
                break
            time.sleep(1)


def dynamoDbTutorial():
    """Tutorial workflow for dynamo db"""
    print("Starting tutorial")
    # Generate random asin and price using current unix timestamp for tutorial
    asin = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(10))
    timestamp = str(calendar.timegm(time.gmtime()))
    price = str(round(random.uniform(0, 100), 2))
    products_db = DynamoDBProduct(asin, timestamp, price)

    products_db.deleteTable()
    products_db.ensureTableDeleted()
    if not products_db.createTable():
        print("Error while creating table, please consult the log")
        return

    products_db.ensureTableActive()

    if not products_db.putItem():
        print("Error while inserting an item into the table, please consult the log")
        return

def queryTutorial():
    # for i in range(0, 100):
    #     asin = ''.join(
    #         random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(10))
    #     timestamp = str(calendar.timegm(time.gmtime()))
    #     price = str(round(random.uniform(0, 100), 2))
    #     products_db = DynamoDBProduct(asin, timestamp, price)
    #     product_db2 = DynamoDBProduct(asin, str(calendar.timegm(time.gmtime()) + 5), price)
    #     if not products_db.putItem():
    #         print("Error while inserting an item into the table, please consult the log")
    #         return
    #     if not product_db2.putItem():
    #         print("Error while inserting an item into the table, please consult the log")
    #         return

    tableName = 'Products'
    #select = 'ALL_ATTRIBUTES'
    # expressionAttributeValues = {':v3' : {'S' : 'Dv2PNdwQ7v'} }
    #
    # prod = DynamoDBProduct('Dv2PNdwQ7v', '1587933252', '178')
    # prod.putItem()
    #
    # response = boto3.client(service_name='dynamodb').query(
    #     TableName=tableName,
    #     Select=select,
    #     KeyConditionExpression='ASIN = :v3',
    #     ExpressionAttributeValues = expressionAttributeValues)

    # prod = DynamoDBProduct('Test', str(calendar.timegm(time.gmtime())), '120')
    # prod.putItem()
    # prod = DynamoDBProduct('Test', str(calendar.timegm(time.gmtime())+5), '189')
    # prod.putItem()


    # This is query challenge to print asin info for skus which have increased in price. Need to figure out more efficient way to do this
    response = boto3.client(service_name='dynamodb').scan(
        TableName=tableName,
        ProjectionExpression='ASIN')

    asinSet = set()
    for i, item in enumerate(response['Items']):
        asin = item['ASIN']['S']
        if asin not in asinSet:
            asinSet.add(asin)

            expressionAttributeValues = {':v1': {'S': asin}}
            response = boto3.client(service_name='dynamodb').query(
                TableName=tableName,
                Select=select,
                Limit=2,
                ScanIndexForward=False,
                ExpressionAttributeValues=expressionAttributeValues,
                KeyConditionExpression='ASIN = :v1')

            if len(response['Items']) >= 2:
                latest = response['Items'][0]
                previous = response['Items'][1]
                latestPrice = latest['Price']['N']
                previousPrice = previous['Price']['N']
                if latestPrice > previousPrice:
                    print(latest['ASIN']['S'], latest['Price']['N'], latest['timeStamp']['N'])


if __name__ == "__main__":
    #dynamoDbTutorial()
    queryTutorial()
