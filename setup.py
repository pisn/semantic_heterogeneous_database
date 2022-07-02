import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='semantic_heterogeneous_database',
    version='1.0.0',
    author='Pedro Ivo Siqueira Nepomuceno',
    author_email='pedro.siqueira@ime.usp.br',
    description='Prototype of a semantic evolution compatible database using MongoDB',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/pisn/semantic_heterogeneous_database',    
    license='MIT',
    packages=['semantic_heterogeneous_database'],
    install_requires=['importlib_metadata','pandas','pymongo'],
)