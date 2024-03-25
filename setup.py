from setuptools import setup, find_packages

setup(
    name = 'dpypr',
    version = '1.0.0',
    description = '(data-piper) a data pipeline convenience tool for Python',
    author = 'James Buchanan',
    author_email = 'buchananja.github@pm.me',
    license = 'MIT',
    url = 'https://github.com/buchananja/dpypr',
    install_requires = [
        'pandas',
        'pyarrow'
    ],
    packages = find_packages(exclude = [
        'tests', 
        'build',
        'logo'
    ])
)