from setuptools import setup, find_packages

VERSION = '0.1.0'
DESCRIPTION = 'Trading system development functionality'
LONG_DESCRIPTION = DESCRIPTION

setup(
    name='tet_trading_systems',
    version='0.1.0',
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author='GHHag',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['pandas==1.5.2', 'numpy', 'matplotlib', 'scikit-learn']#, 'TETrading', 'securities_pg_db', 'tet_doc_db'] 
)
