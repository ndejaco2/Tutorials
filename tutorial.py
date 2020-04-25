import boto3
import time
import random
import string
import calendar

client = boto3.client('dynamodb')

class Product:
    def __init__(self, asin, timestamp, price):
        self.asin = asin
        self.timestamp = timestamp
        self.price = price

    def getItem(self):
        return {'ASIN' : { 'S' : self.asin}, 'TimeStamp' : {'N' : self.timestamp}, 'Price' : {'N' : self.price}}

    @staticmethod
    def getAttributeDefs():
        return [{'AttributeName' : 'ASIN', 'AttributeType' : 'S'}, {'AttributeName' : 'TimeStamp', 'AttributeType' : 'N'}]

    @staticmethod
    def getKeySchema():
        return [{'AttributeName' : 'ASIN', 'KeyType' : 'HASH'}, {'AttributeName' : 'TimeStamp', 'KeyType' : 'RANGE'}]


def putItem(tableName, product):
    try:
        response = client.put_item(TableName=tableName, Item=product.getItem(), ReturnConsumedCapacity='TOTAL')
        print(response)
        return True
    except Exception as e:
        print(e)
        return False

def createTable(tableName):
    try:
        response = client.create_table(AttributeDefinitions=Product.getAttributeDefs(), TableName=tableName, KeySchema=Product.getKeySchema(), BillingMode='PAY_PER_REQUEST')
        responseDict = dict(response)
        print("Successfully created table " + responseDict['TableDescription']['TableName'])
        return True
    except Exception as e:
        print(e)
        return False

def deleteTable(tableName):
    try:
        response = client.delete_table(TableName=tableName)
    except Exception as e:
        print(e)

def ensureTableDeleted(tableName):
    while True:
        try:
            response = client.describe_table(TableName=tableName)
            time.sleep(2)
        except Exception as e:
            break

def ensureTableActive(tableName):
    while True:
        try:
            response = client.describe_table(TableName=tableName)
            responseDict = dict(response)
            if responseDict['Table']['TableStatus'] == 'ACTIVE':
                break
            time.sleep(1)
        except Exception as e:
            break

def dynamoDbTutorial():
    print("Starting tutorial")
    tableName = 'Products'
    deleteTable(tableName)
    ensureTableDeleted(tableName)
    if not createTable(tableName):
        print("Error while creating table, please consult the log")
        return

    ensureTableActive(tableName)

    # Generate random asin and price using current unix timestamp for tutorial
    asin = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(10))
    timestamp = str(calendar.timegm(time.gmtime()))
    price = str(round(random.uniform(0, 100), 2))
    newProduct = Product(asin, timestamp, price)
    if not putItem(tableName, newProduct):
        print("Error while inserting an item into the table, please consult the log")
        return

dynamoDbTutorial()
