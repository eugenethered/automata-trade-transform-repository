from utility.json_utility import as_data

from tradetransformrepo.TradeTransform import TradeTransform


def deserialize_trade_transform(trade_transform) -> TradeTransform:
    trade = as_data(trade_transform, 'trade')
    transform = as_data(trade_transform, 'transform')
    deserialized = TradeTransform(trade, transform)
    return deserialized
