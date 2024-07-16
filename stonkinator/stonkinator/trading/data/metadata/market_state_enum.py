from enum import Enum


class MarketState(Enum):

    ENTRY = 'entry'
    BUY = 'entry'
    ACTIVE = 'active'
    NULL = 'null'
    HOLD = 'active'
    EXIT = 'exit'
    SELL = 'exit'
