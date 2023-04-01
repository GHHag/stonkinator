from tet_trading_systems.trading_system_state_handler.order_execution.i_order_execution_handler import IOrderExecutionHandler


class AvanzaOrderExecutionHandler(IOrderExecutionHandler):
    
    def __init__(self):
        pass

    def _queue_orders(self):
        pass

    def place_orders(self):
        self._queue_orders()
