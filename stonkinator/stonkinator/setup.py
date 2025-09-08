from setuptools import setup, find_packages


VERSION = '0.1.0'
DESCRIPTION = 'stonkinator'
LONG_DESCRIPTION = DESCRIPTION

setup(
    name='stonkinator',
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author='GHHag',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'pandas==2.2.0',
        'pyarrow==20.0.0',
        'numpy==1.26.4',
        'matplotlib==3.8.2',
        'mplfinance==0.12.10b0',
        'scikit-learn==1.4.0',
        'yahooquery==2.3.7',
        'grpcio-tools==1.70.0'
    ]
)
