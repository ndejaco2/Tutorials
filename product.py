class Product:
    def __init__(self, asin: str, timestamp: str, price: str):
        self.asin = asin
        self.timestamp = timestamp
        self.price = price

    def getItem(self):
        return {
            'ASIN': {'S': self.asin},
            'TimeStamp': {'N': self.timestamp},
            'Price': {'N': self.price}
        }

    @staticmethod
    def getKeyAttributeDefs():
        return [
            {'AttributeName': 'ASIN', 'AttributeType': 'S'},
            {'AttributeName': 'TimeStamp', 'AttributeType': 'N'}
        ]

    @staticmethod
    def getKeySchema():
        return [
            {'AttributeName': 'ASIN', 'KeyType': 'HASH'},
            {'AttributeName': 'TimeStamp', 'KeyType': 'RANGE'}
        ]
