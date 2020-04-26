class Product:
    """
    :param asin: amazon SKU id number
    :type asin: str
    :param timestamp: calendar time for which product price is found
    :type timestamp: str
    :param price: price of item in US $
    :type price: str
    """
    def __init__(self, asin: str, timestamp: str, price: str):
        self.asin = asin
        self.timestamp = timestamp
        self.price = price

    def getItem(self) -> dict:
        """Get data in table

        :returns: dict[dict]
        """
        return {
            'ASIN': {'S': self.asin},
            'timeStamp': {'N': self.timestamp},
            'Price': {'N': self.price}
        }

    @staticmethod
    def getKeyAttributeDefs() -> list:
        """

        :return: list[dict]
        """
        return [
            {'AttributeName': 'ASIN', 'AttributeType': 'S'},
            {'AttributeName': 'timeStamp', 'AttributeType': 'N'}
        ]

    @staticmethod
    def getKeySchema() -> list:
        """

        :return: list[dict]
        """
        return [
            {'AttributeName': 'ASIN', 'KeyType': 'HASH'},
            {'AttributeName': 'timeStamp', 'KeyType': 'RANGE'}
        ]
