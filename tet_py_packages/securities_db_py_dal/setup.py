from setuptools import setup, find_packages


VERSION = '0.1.0'
DESCRIPTION = 'Data access layer'
LONG_DESCRIPTION = DESCRIPTION

setup(
    name='securities_pg_db',
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author='GHHag',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['yahooquery', 'pandas==1.5.2']#, 'instruments_mongo_db']
)
