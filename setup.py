from setuptools import setup, find_packages

setup(
    name = 'dypr',
    version = '1.0.0',
    description = '(Data-Piper) a data pipeline convenience module for Python.',
    author = 'James Buchanan',
    author_email = 'buchananja.github@pm.me',
    license = 'MIT',
    url = 'https://github.com/buchananja/pyped',
    install_requires = [
        'pandas',
        'pyarrow'
    ],
    packages = find_packages(exclude = 'testing*')
)