from setuptools import setup

with open("README.md", 'r') as f:
    long_description = f.read()

setup(
    name="pyutils",
    version="1.0",
    description="Miscellaneous utility modules",
    long_description=long_description,
    packages=["pyutils"],
    install_requires=[]
)
