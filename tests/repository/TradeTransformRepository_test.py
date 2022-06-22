import unittest

from cache.holder.RedisCacheHolder import RedisCacheHolder
from cache.provider.RedisCacheProviderWithHash import RedisCacheProviderWithHash

from tradetransformrepo.TradeTransform import TradeTransform
from tradetransformrepo.repository.TradeTransformRepository import TradeTransformRepository


class TradeTransformRepositoryTestCase(unittest.TestCase):

    def setUp(self) -> None:
        options = {
            'REDIS_SERVER_ADDRESS': '192.168.1.90',
            'REDIS_SERVER_PORT': 6379,
            'TRADE_TRANSFORMATIONS_KEY': 'test:mv:transformation:trade'
        }
        self.cache = RedisCacheHolder(options, held_type=RedisCacheProviderWithHash)
        self.repository = TradeTransformRepository(options)

    def tearDown(self):
        self.cache.delete('test:mv:transformation:trade')

    def test_should_store_and_retrieve_trade_transform(self):
        trade_transform = TradeTransform('BTC/OTC', {
            'instrument': 'BTCOTC'
        })
        self.repository.create(trade_transform)
        stored_trade_transformations = self.repository.retrieve()
        self.assertEqual(trade_transform, stored_trade_transformations[0])

    def test_should_store_and_retrieve_multiple_trade_transformations(self):
        trade_transform_1 = TradeTransform('BTC/OTC', {
            'instrument': 'BTCOTC'
        })
        trade_transform_2 = TradeTransform('ETH/OTC', {
            'instrument': 'ETHOTC'
        })
        trade_transform_3 = TradeTransform('GBP/OTC')
        trade_transformations = [trade_transform_1, trade_transform_2, trade_transform_3]
        self.repository.store_all(trade_transformations)
        stored_trade_transformations = self.repository.retrieve()
        self.assertEqual(trade_transformations, stored_trade_transformations)


if __name__ == '__main__':
    unittest.main()
