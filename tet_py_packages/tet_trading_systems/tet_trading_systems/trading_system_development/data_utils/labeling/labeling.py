import pandas as pd


def base_labeler(df: pd.DataFrame, column: str, predicate: callable, label_name: str):
    df[label_name] = predicate(df[column])

def multi_period_labeler(
    df: pd.DataFrame, column1: str, column2: str, 
    predicate1: callable, predicate2: callable,
    entry_label: str='entry', hold_label: str='hold', exit_label: str='exit'
):
    df[entry_label] = predicate1(df[column1])
    df[exit_label] = predicate2(df[entry_label], df[column2])
    df[hold_label] = False

    hold_label_state = False
    for i, _ in enumerate(df.itertuples()):
        if (
            df[hold_label].iloc[i-1] and
            df[exit_label].iloc[i] and
            hold_label_state
        ):
            hold_label_state = False
        elif (
            not df[hold_label].iloc[i] and 
            df[entry_label].iloc[i] and 
            not hold_label_state 
        ):
            hold_label_state = True
        
        df.loc[i, hold_label] = hold_label_state

    df[hold_label] = df[hold_label].shift(1)


if __name__ == '__main__':
    df = pd.DataFrame(
        data={'1': [
            100, 150, 130, 80, 340, 
            3000, 100, 120, 900, 650, 
            300, 140, 140, 14000, 110,
            110, 120, 130, 140, 149,
            155, 900, 1000, 1000, 3001,
            100, 100, 1, 2, 0
        ]}
    )

    base_labeler(df, '1', lambda i: i > 140, 'bauta')

    multi_period_labeler(
        df, '1', '1', lambda i: i > 140, lambda j, k: k >= 3000
    )
    print(df.tail(60).to_string())