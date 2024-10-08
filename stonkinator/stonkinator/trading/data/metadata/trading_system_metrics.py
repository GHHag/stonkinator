from trading.data.metadata.trading_system_attributes import classproperty


class TradingSystemMetrics:

    __SYMBOL = 'symbol'
    __NUM_OF_POSITIONS = 'number_of_positions'
    __START_CAPITAL = 'start_capital'
    __FINAL_CAPITAL = 'final_capital'
    __TOTAL_GROSS_PROFIT = 'total_gross_profit'
    __AVG_POS_NET_PROFIT = 'avg_pos_net_profit'
    __PCT_WINS = '%_wins'
    __PROFIT_FACTOR = 'profit_factor'
    __EXPECTANCY = 'expectancy'
    __SHARPE_RATIO = 'sharpe_ratio'
    __RATE_OF_RETURN = 'rate_of_return'
    __MEAN_PROFIT_LOSS = 'mean_p/l'
    __MEDIAN_PROFIT_LOSS = 'median_p/l'
    __STD_OF_PROFIT_LOSS = 'std_of_p/l'
    __MEAN_RETURN = 'mean_return'
    __MEDIAN_RETURN = 'median_return'
    __STD_OF_RETURNS = 'std_of_returns'
    __AVG_MAE = 'avg_mae'
    __MIN_MAE = 'min_mae'
    __AVG_MFE = 'avg_mfe'
    __MAX_MFE = 'max_mfe'
    __MAX_DRAWDOWN = 'max_drawdown_(%)'
    __ROMAD = 'romad'
    __CAGR = 'cagr_(%)'
    __AVG_PERIODS_IN_POSITIONS = 'avg_periods_in_positions'
    __AVG_PERIODS_IN_WINNING_POSITIONS = 'avg_periods_in_winning_positions'
    __AVG_PERIODS_IN_LOSING_POSITIONS = 'avg_periods_in_losing_positions'

    @classproperty
    def SYMBOL(cls):
        return cls.__SYMBOL

    @classproperty
    def NUM_OF_POSITIONS(cls):
        return cls.__NUM_OF_POSITIONS

    @classproperty
    def START_CAPITAL(cls):
        return cls.__START_CAPITAL

    @classproperty
    def FINAL_CAPITAL(cls):
        return cls.__FINAL_CAPITAL

    @classproperty
    def TOTAL_GROSS_PROFIT(cls):
        return cls.__TOTAL_GROSS_PROFIT

    @classproperty
    def AVG_POS_NET_PROFIT(cls):
        return cls.__AVG_POS_NET_PROFIT

    @classproperty
    def PCT_WINS(cls):
        return cls.__PCT_WINS

    @classproperty
    def PROFIT_FACTOR(cls):
        return cls.__PROFIT_FACTOR

    @classproperty
    def EXPECTANCY(cls):
        return cls.__EXPECTANCY

    @classproperty
    def SHARPE_RATIO(cls):
        return cls.__SHARPE_RATIO

    @classproperty
    def RATE_OF_RETURN(cls):
        return cls.__RATE_OF_RETURN

    @classproperty
    def MEAN_PROFIT_LOSS(cls):
        return cls.__MEAN_PROFIT_LOSS

    @classproperty
    def MEDIAN_PROFIT_LOSS(cls):
        return cls.__MEDIAN_PROFIT_LOSS

    @classproperty
    def STD_OF_PROFIT_LOSS(cls):
        return cls.__STD_OF_PROFIT_LOSS

    @classproperty
    def MEAN_RETURN(cls):
        return cls.__MEAN_RETURN

    @classproperty
    def MEDIAN_RETURN(cls):
        return cls.__MEDIAN_RETURN

    @classproperty
    def STD_OF_RETURNS(cls):
        return cls.__STD_OF_RETURNS

    @classproperty
    def AVG_MAE(cls):
        return cls.__AVG_MAE

    @classproperty
    def MIN_MAE(cls):
        return cls.__MIN_MAE

    @classproperty
    def AVG_MFE(cls):
        return cls.__AVG_MFE

    @classproperty
    def MAX_MFE(cls):
        return cls.__MAX_MFE

    @classproperty
    def MAX_DRAWDOWN(cls):
        return cls.__MAX_DRAWDOWN

    @classproperty
    def ROMAD(cls):
        return cls.__ROMAD

    @classproperty
    def CAGR(cls):
        return cls.__CAGR

    @classproperty
    def AVG_PERIODS_IN_POSITIONS(cls):
        return cls.__AVG_PERIODS_IN_POSITIONS

    @classproperty
    def AVG_PERIODS_IN_WINNING_POSITIONS(cls):
        return cls.__AVG_PERIODS_IN_WINNING_POSITIONS

    @classproperty
    def AVG_PERIODS_IN_LOSING_POSITIONS(cls):
        return cls.__AVG_PERIODS_IN_LOSING_POSITIONS

    @classproperty
    def cls_attrs(cls):
        return [
            v for v in cls.__dict__.values() if isinstance(v, str) and \
            v not in ['__main__', 'trading.data.metadata.trading_system_metrics']
        ]

    @classproperty
    def system_evaluation_fields(cls):
        return (
            cls.__SYMBOL, cls.__SHARPE_RATIO, cls.__EXPECTANCY,
            cls.__PROFIT_FACTOR, cls.__CAGR, cls.__PCT_WINS,
            cls.__MEAN_RETURN, cls.__MAX_DRAWDOWN, cls.__ROMAD
        )
