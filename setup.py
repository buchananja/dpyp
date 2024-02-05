from setuptools import setup, find_packages

setup(
    name = 'pyped',
    version = '1.0.0',
    description = 'A data pipeline convenience module for Python.',
    author = 'James Buchanan',
    author_email = 'buchananja.github@pm.me',
    license = 'MIT',
    url = 'https://github.com/buchananja/pyped',
    install_requires = [
        'pandas',
        'sqlite3'
    ],
    packages = find_packages(exclude = 'testing*')
)