from TETrading.signal_events.signals.trading_system_signals import TradingSystemSignals


class ExitSignals(TradingSystemSignals):
    """
    Handles data for exit signals.
    """

    def __init__(self):
        super().__init__()
