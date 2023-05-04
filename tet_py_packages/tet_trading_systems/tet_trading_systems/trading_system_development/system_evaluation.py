import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from matplotlib.ticker import MaxNLocator


def evaluate_systems(csv_files_dir, sort_on_ticker=True, write_to_csv=False, csv_file_path=''):
    systems_df_list = []
    systems_df = pd.DataFrame()

    for file in os.listdir(csv_files_dir):
        if file.endswith('.csv'):
            df = pd.read_csv(file)
            df['SN'] = file[:-4]
            systems_df_list.append(df)
            systems_df = systems_df.append(df, ignore_index=True)
        else:
            continue

    if not sort_on_ticker:
        strat_summary_df = pd.DataFrame(columns=['Strategy', 'Total # positions',
                                                 'Median gross profit', 'Mean gross profit', 'Gross profit SD',
                                                 'Median profit factor', 'Mean profit factor', 'Profit factor SD',
                                                 'Median max drawdown (%)', 'Mean max drawdown (%)', 'Max drawdown SD'])
        for df in systems_df_list:
            print(df[['Ticker', 'Number of positions', 'Total gross profit', '% wins', 'Profit factor', 'Expectancy',
                      'Max drawdown (%)', 'CAGR (%)', 'SN']].to_string())
            print('Total number of positions: ', df['Number of positions'].sum())
            print('Median gross profit:', df['Total gross profit'].median())
            print('Mean gross profit:', df['Total gross profit'].mean())
            print('Median profit factor:', df['Profit factor'].median())
            print('Mean profit factor:', df['Profit factor'].mean())
            print('Median max drawdown (%):', df['Max drawdown (%)'].median())
            print('Mean max drawdown (%): ', df['Max drawdown (%)'].mean())
            print()
            df_row = {
                'Strategy': df['SN'].iloc[0],
                'Total # positions': df['Number of positions'].sum(),
                'Median gross profit': df['Total gross profit'].median(),
                'Mean gross profit': df['Total gross profit'].mean(),
                'Gross profit SD': df['Total gross profit'].std(),
                'Median profit factor': df['Profit factor'].median(),
                'Mean profit factor': df['Profit factor'].mean(),
                'Profit factor SD': df['Profit factor'].std(),
                'Median max drawdown (%)': df['Max drawdown (%)'].median(),
                'Mean max drawdown (%)': df['Max drawdown (%)'].mean(),
                'Max drawdown SD': df['Max drawdown (%)'].std()
            }
            strat_summary_df = strat_summary_df.append(df_row, ignore_index=True)

        pd.set_option("display.max_rows", None, "display.max_columns", None)
        print(strat_summary_df.sort_values('Strategy', ascending=False).to_string())

        if write_to_csv:
            strat_summary_df.to_csv(csv_file_path, mode='a')

    if sort_on_ticker:
        for ticker in systems_df_list[0]['Ticker']:
            x = 0
            for df in systems_df_list:
                print(df[['Ticker', 'Number of trades', 'Total gross profit', '% wins', 'Profit factor', 'Expectancy',
                      'Max drawdown (%)', 'CAGR (%)', 'SN']][df['Ticker'] == ticker].to_string())
                x += 1
            print()


def evaluate_monte_carlo_simulations(csv_file_path, plot=True):
    mc_sims_df = pd.read_csv(csv_file_path)

    pd.set_option("display.max_rows", None, "display.max_columns", None)
    print(mc_sims_df.to_string())

    plt.style.use('seaborn')
    fig, axs = plt.subplots(nrows=2, ncols=4, figsize=(14, 6))
    fig.tight_layout()

    axs[0, 0].hist(mc_sims_df['Median gross profit'], edgecolor='black', linewidth=1.2, orientation='horizontal', bins=10)
    axs[0, 0].yaxis.set_major_locator(MaxNLocator(integer=True))
    axs[0, 0].set_title('Median gross profit')
    axs[0, 0].set_xlabel('')
    axs[0, 0].set_ylabel('Profit')

    axs[0, 1].hist(mc_sims_df['Mean % wins'], edgecolor='black', linewidth=1.2, orientation='horizontal', bins=20)
    axs[0, 1].yaxis.set_major_locator(MaxNLocator(integer=True))
    axs[0, 1].set_title('Mean % wins')
    axs[0, 1].set_xlabel('')
    axs[0, 1].set_ylabel('% wins')

    axs[0, 2].hist(mc_sims_df['Median profit factor'], edgecolor='black', linewidth=1.2, orientation='horizontal',
                   bins=20)
    axs[0, 2].yaxis.set_major_locator(MaxNLocator(integer=True))
    axs[0, 2].set_title('Median profit factor')
    axs[0, 2].set_xlabel('')
    axs[0, 2].set_ylabel('Profit factor')

    axs[0, 3].hist(mc_sims_df['Median expectancy'], edgecolor='black', linewidth=1.2, orientation='horizontal', bins=20)
    axs[0, 3].yaxis.set_major_locator(MaxNLocator(integer=True))
    axs[0, 3].set_title('Median expectancy')
    axs[0, 3].set_xlabel('')
    axs[0, 3].set_ylabel('Expectancy')

    axs[1, 0].hist(mc_sims_df['Median max drawdown (%)'], edgecolor='black', linewidth=1.2, orientation='horizontal',
                   bins=30)
    axs[1, 0].yaxis.set_major_locator(MaxNLocator(integer=True))
    axs[1, 0].set_title('Median max drawdown')
    axs[1, 0].set_xlabel('')
    axs[1, 0].set_ylabel('Drawdown')

    axs[1, 1].hist(mc_sims_df['Median CAGR (%)'], edgecolor='black', linewidth=1.2, orientation='horizontal', bins=30)
    axs[1, 1].yaxis.set_major_locator(MaxNLocator(integer=True))
    axs[1, 1].set_title('Median CAGR (%)')
    axs[1, 1].set_xlabel('')
    axs[1, 1].set_ylabel('CAGR')

    axs[1, 2].hist(mc_sims_df['CAR25'], edgecolor='black', linewidth=1.2, orientation='horizontal', bins=30)
    axs[1, 2].yaxis.set_major_locator(MaxNLocator(integer=True))
    axs[1, 2].set_title('CAR25')
    axs[1, 2].set_xlabel('')
    axs[1, 2].set_ylabel('CAR')

    axs[1, 3].hist(mc_sims_df['CAR75'], edgecolor='black', linewidth=1.2, orientation='horizontal', bins=30)
    axs[1, 3].yaxis.set_major_locator(MaxNLocator(integer=True))
    axs[1, 3].set_title('CAR75')
    axs[1, 3].set_xlabel('')
    axs[1, 3].set_ylabel('CAR')

    if plot is True:
        plt.tight_layout()
        plt.show()


def evaluate_compare_monte_carlo_simulations(csv_files_dir, sort_on_ticker=True, metric='unknown', plot=True):
    pass


if __name__ == '__main__':
    os.chdir(r'')
    file_directory = os.getcwd()

    # BACKTESTS
    write_csv = False
    csv_path = r''

    evaluate_systems(file_directory, sort_on_ticker=False, write_to_csv=write_csv, csv_file_path=csv_path)

    # MONTE CARLO SIMULATIONS
    #evaluate_monte_carlo_simulations(r'D:\Trading\Backtest runs\Mean reversion stocks\Out of sample monte carlo sims\mean_reversion_function_rsi_divergence_entry_n_period_rw_rsi_target_trail_atr_exit.csv',
    #                                 plot=True)
