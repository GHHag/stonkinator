from TETrading.signal_events.signals.trading_system_signals import TradingSystemSignals


class ActivePositions(TradingSystemSignals):
    """
    Handles data for active positions.
    """

    def __init__(self):
        super().__init__()
