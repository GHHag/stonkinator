from operator import itemgetter

from tet_trading_systems.trading_system_state_handler.portfolio.i_target_portfolio_creator import ITargetPortfolioCreator


class MetricRankingPortfolioCreator(ITargetPortfolioCreator):

    def __init__(self, ranking_metric='sharpe_ratio'):
        self.__ranking_metric = ranking_metric

    def _process_signals(
        self, entry_signals, target_positions, exit_signals
    ):
        entry_signals_sorted = sorted(
            (
                entry_signal for entry_signal in entry_signals \
                if entry_signal[self.__ranking_metric] is not None
            ),
            key=itemgetter(self.__ranking_metric),
            reverse=True
        )
        positions_to_exit = [
            x for x in target_positions 
            if x['symbol'] in [i['symbol'] for i in exit_signals] # går denna operation att göra med .filter() istaellet?
        ]
        return entry_signals_sorted, target_positions, positions_to_exit

    def _make_selection(
        self, system_metrics, portfolio_max_positions, 
        entry_signals, target_positions, exit_signals
    ):
        safe_f = 1.0 if system_metrics['metrics']['safe-f'] > 1.0 else system_metrics['metrics']['safe-f']
        free_pos_slots = int(portfolio_max_positions * safe_f) - \
            (len(target_positions) - len(exit_signals))
        if free_pos_slots > 0:
            return [
                x for x in entry_signals[:free_pos_slots] 
                if x['symbol'] not in [i['symbol'] for i in target_positions] and
                x['sharpe_ratio'] > -10#0.5#1.0 # går denna operation att göra med .filter() istaellet?
            ]
        else:
            return [] 

    def _create_target_portfolio(
        self, system_metrics, portfolio_max_positions,
        entry_signals, target_positions
    ):
        #target_portfolio = []
        # logik för att hantera capital, weights safe-f med mera
        import pprint
        print('\nsystem_metrics:')#, system_metrics)
        pprint.pprint(system_metrics)
        print('\ntarget_positions:')#, target_positions)
        pprint.pprint(target_positions)
        print('\nentry_signals:')#, entry_signals)
        pprint.pprint(entry_signals)

        """ if len(target_positions) > 0:            
            target_portfolio += target_positions
        if len(entry_signals) > 0:
            target_portfolio += entry_signals
        if len(target_portfolio) > 0:
            pass
        return target_portfolio """
        #return target_positions + entry_signals
        return [target_pos['_id'] for target_pos in target_positions] + \
            [entry_signal['_id'] for entry_signal in entry_signals]

    def __call__(
        self, system_metrics, portfolio_max_positions,
        entry_signals, target_positions, exit_signals, *args, 
        **kwargs
    ):
        processed_entry_signals, processed_target_positions, processed_exit_signals = self._process_signals(
            entry_signals, target_positions, exit_signals
        )
        selected_entry_signals = self._make_selection(
            system_metrics, portfolio_max_positions,
            processed_entry_signals, processed_target_positions, processed_exit_signals
        )
        return self._create_target_portfolio(
            system_metrics, portfolio_max_positions, 
            selected_entry_signals, processed_target_positions
        )
