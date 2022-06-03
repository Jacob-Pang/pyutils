from glob import glob
from setuptools import setup, find_packages
from os.path import basename, splitext

with open("README.md", 'r') as f:
    long_description = f.read()

print(find_packages("src"))
setup(
    name="pyutils",
    version="1.0",
    description="Utility modules for python projects",
    long_description=long_description,
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[
        splitext(basename(path))[0] for path in glob('src/*.py')
    ],
    include_package_data=True,
    install_requires=[
        "requests", "PyGithub", "pandas"
    ]
)
