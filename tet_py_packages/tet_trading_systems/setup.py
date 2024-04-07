from setuptools import setup, find_packages


VERSION = '0.1.0'
DESCRIPTION = 'Trading system development package'
LONG_DESCRIPTION = DESCRIPTION

setup(
    name='tet_trading_systems',
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author='GHHag',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'pandas==2.2.0', 
        'numpy==1.26.4', 
        'matplotlib==3.8.2', 
        'scikit-learn==1.4.0',
        'sterunets @ git+https://github.com/GHHag/sterunets.git'
    ]
)
