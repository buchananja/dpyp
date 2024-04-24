from setuptools import setup, find_packages

setup(
    name = 'dpyp',
    version = '1.0.0',
    description = 'A convenience tool for small-scale data pipelines in Python',
    author = 'James Buchanan',
    author_email = 'buchananja.github@pm.me',
    license = 'MIT',
    url = 'https://github.com/buchananja/dpyp',
    install_requires = ['pandas', 'pyarrow'],
    keywords = ['pandas', 'data', 'pipeline', 'cleaning', 'processing'],
    packages = find_packages(exclude = ['tests', 'build', 'logo'])
)