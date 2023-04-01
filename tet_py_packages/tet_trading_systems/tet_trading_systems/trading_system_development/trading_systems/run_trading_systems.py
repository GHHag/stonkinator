from tet_doc_db.doc_database_meta_classes.tet_systems_doc_db import ITetSystemsDocumentDatabase

from TETrading.data.metadata.market_state_enum import MarketState
from TETrading.trading_system.trading_system import TradingSystem


def run_trading_system(
    data_dict, system_name, entry_func, exit_func,
    entry_args, exit_args, *args, 
    capital=10000, capital_fraction=1.0, avg_yearly_periods=251,  
    market_state_null_default=False, run_monte_carlo_sims=False, num_of_sims=2500,
    print_dataframe=False, plot_fig=False, plot_positions=False, write_to_file_path=None, 
    systems_db: ITetSystemsDocumentDatabase=None, 
    client_db: ITetSystemsDocumentDatabase=None, 
    insert_into_db=False,
    pos_list_slice_years_est=2,
    save_best_estimate_trades_path=None,
    **kwargs
):
    ts = TradingSystem(system_name, data_dict, entry_func, exit_func)
    ts(
        capital=capital,
        capital_fraction=capital_fraction,
        avg_yearly_periods=avg_yearly_periods,
        market_state_null_default=market_state_null_default,
        plot_performance_summary=plot_fig,
        save_summary_plot_to_path=None, 
        system_analysis_to_csv_path=None, 
        plot_returns_distribution=plot_fig,
        save_returns_distribution_plot_to_path=None, 
        run_monte_carlo_sims=run_monte_carlo_sims,
        num_of_monte_carlo_sims=num_of_sims,
        monte_carlo_data_amount=0.65,
        plot_monte_carlo=plot_fig,
        print_monte_carlo_df=print_dataframe,
        monte_carlo_analysis_to_csv_path=None, 
        commission_pct_cost=0.0025,
        entry_args=entry_args,
        exit_args=exit_args,
        fixed_position_size=True,
        generate_signals=True,
        plot_positions=plot_positions,
        save_position_figs_path=None,
        write_signals_to_file_path=write_to_file_path,
        insert_data_to_db_bool=insert_into_db,
        signal_handler_db_insert_funcs=
        {
            MarketState.ENTRY.value: client_db.insert_market_state_data,
            MarketState.ACTIVE.value: client_db.insert_market_state_data,
            MarketState.EXIT.value: client_db.insert_market_state_data
        },
        single_symbol_pos_list_db_insert_func=systems_db.insert_single_symbol_position_list,
        json_format_single_symbol_pos_list_db_insert_func=client_db.insert_single_symbol_position_list,
        full_pos_list_db_insert_func=systems_db.insert_position_list,
        json_format_full_pos_list_db_insert_func=client_db.insert_position_list,
        pos_list_slice_years_est=pos_list_slice_years_est
    )
