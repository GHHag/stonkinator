from setuptools import setup, find_packages


VERSION = '0.1.0'
DESCRIPTION = 'A package with database functionality for systems \
               created and managed with TETrading.'

setup(
    name='tet_doc_db',
    version=VERSION,
    author='GHHag',
    description=DESCRIPTION,
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'pymongo==4.6.1', 
        'firebase_admin==6.4.0'
    ]
)
