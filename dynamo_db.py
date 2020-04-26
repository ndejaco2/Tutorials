import calendar
import random
import string
import time

import boto3

from product import Product


class DynamoDBProduct(Product):
    def __init__(self, *args):
        Product.__init__(self, *args)
        self.table_name = "Products"
        self.client = boto3.client(service_name='dynamodb')

    def putItem(self):
        try:
            response = self.client.put_item(TableName=self.table_name, Item=self.getItem(),
                                            ReturnConsumedCapacity='TOTAL')
            print(response)
            return True
        except Exception as e:
            print(e)
            return False

    def createTable(self):
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


if __name__ == "__main__":
    dynamoDbTutorial()
