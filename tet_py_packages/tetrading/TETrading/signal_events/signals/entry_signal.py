from TETrading.signal_events.signals.trading_system_signals import TradingSystemSignals


class EntrySignals(TradingSystemSignals):
    """
    Handles data for entry signals.
    """

    def __init__(self):
        super().__init__()
