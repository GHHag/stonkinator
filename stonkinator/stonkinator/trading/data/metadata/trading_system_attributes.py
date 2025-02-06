class classproperty(property):
    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()


class TradingSystemAttributes:

    __LONG = 'long'
    __SHORT = 'short'
    __MARKET_STATE = 'market_state'
    __SIGNAL_INDEX = 'signal_index'
    __SIGNAL_DT = 'signal_dt'
    __SYMBOL = 'symbol'
    __DIRECTION = 'direction'
    __ORDER = 'order'
    __PERIODS_IN_POSITION = 'periods_in_position'
    __CURRENT_POSITION = 'current_position'
    __UNREALISED_RETURN = 'unrealised_return'
    __ID = '_id'
    __SYSTEM_ID = 'system_id'
    __NAME = 'name'
    __SYSTEM_NAME = 'system_name'
    __METRICS = 'metrics'
    __NUMBER_OF_PERIODS = 'num_of_periods'
    __POSITION_LIST = 'position_list'
    __ML_MODEL = 'model'
    __INSTRUMENT = 'instrument'
    __ENTRY_PERIOD_LOOKBACK = 'entry_period_lookback'
    __EXIT_PERIOD_LOOKBACK = 'exit_period_lookback'
    __REQ_PERIOD_ITERS = 'req_period_iters'
    __PRED_COL = 'pred'

    @classproperty
    def LONG(cls):
        return cls.__LONG

    @classproperty
    def SHORT(cls):
        return cls.__SHORT

    @classproperty
    def MARKET_STATE(cls):
        return cls.__MARKET_STATE

    @classproperty
    def SIGNAL_INDEX(cls):
        return cls.__SIGNAL_INDEX
    
    @classproperty
    def SIGNAL_DT(cls):
        return cls.__SIGNAL_DT

    @classproperty
    def SYMBOL(cls):
        return cls.__SYMBOL

    @classproperty
    def DIRECTION(cls):
        return cls.__DIRECTION

    @classproperty
    def ORDER(cls):
        return cls.__ORDER

    @classproperty
    def PERIODS_IN_POSITION(cls):
        return cls.__PERIODS_IN_POSITION

    @classproperty
    def CURRENT_POSITION(cls):
        return cls.__CURRENT_POSITION

    @classproperty
    def UNREALISED_RETURN(cls):
        return cls.__UNREALISED_RETURN

    @classproperty
    def ID(cls):
        return cls.__ID

    @classproperty
    def SYSTEM_ID(cls):
        return cls.__SYSTEM_ID

    @classproperty
    def NAME(cls):
        return cls.__NAME

    @classproperty
    def SYSTEM_NAME(cls):
        return cls.__SYSTEM_NAME

    @classproperty
    def METRICS(cls):
        return cls.__METRICS

    @classproperty
    def NUMBER_OF_PERIODS(cls):
        return cls.__NUMBER_OF_PERIODS

    @classproperty
    def POSITION_LIST(cls):
        return cls.__POSITION_LIST

    @classproperty
    def ML_MODEL(cls):
        return cls.__ML_MODEL

    @classproperty
    def INSTRUMENT(cls):
        return cls.__INSTRUMENT 

    @classproperty
    def ENTRY_PERIOD_LOOKBACK(cls):
        return cls.__ENTRY_PERIOD_LOOKBACK

    @classproperty
    def EXIT_PERIOD_LOOKBACK(cls):
        return cls.__EXIT_PERIOD_LOOKBACK

    @classproperty
    def REQ_PERIOD_ITERS(cls):
        return cls.__REQ_PERIOD_ITERS

    @classproperty
    def PRED_COL(cls):
        return cls.__PRED_COL