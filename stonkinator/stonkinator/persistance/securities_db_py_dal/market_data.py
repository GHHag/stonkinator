def get_stock_indices_symbols_list():
    ticker = [
        'GSPC', 'DJI', 'IXIC', 'RUT', 'FTSE', 'GDAXI', 'STOXX50E', 'FCHI',
        'N225', 'HSI', 'OMX', 'OMXSPI', 'OMXC25', 'OBX', 'OMXH25', 'FN25'
    ]
    return ['^' + t for t in ticker]


def get_futures_symbols_list():
    return [
        'ZN=F',
        'GC=F', 'SI=F', 'HG=F', 'PL=F', 'PA=F', 
        'CL=F', 'NG=F', 
        'ZC=F', 'CC=F', 'KC=F', 'KE=F', 'SB=F'
    ]
