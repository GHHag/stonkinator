from setuptools import setup, find_packages


VERSION = '0.1.0'
DESCRIPTION = 'Data layer'
LONG_DESCRIPTION = DESCRIPTION

setup(
    name='securities_pg_db',
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author='GHHag',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'yahooquery==2.3.7', 
        'pandas==2.2.0', 
        'requests==2.31.0'
    ]
)
