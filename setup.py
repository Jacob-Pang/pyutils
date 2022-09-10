import numpy as np

from glob import glob
from setuptools import Extension
from setuptools import setup, find_packages
from os.path import basename, join, splitext

with open("README.md", 'r') as readme:
    long_description = readme.read()

# Cython modules
ext_modules = [
    Extension("cyutils.vector_as_numpy", sources=[
            join("src", "pyutils", "cyutils", "vector_as_numpy.pyd"),
            join("src", "pyutils", "cyutils", "vector_as_numpy.pyx")
        ])
]

# Set to Python3
for ext_module in ext_modules:
    ext_module.cython_directives = {"language_level": "3"}


setup(
    name="pyutils",
    version="1.0",
    description="General utility modules for python projects",
    long_description=long_description,
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[
        splitext(basename(path))[0] for path in glob('src/*.py')
    ],
    include_package_data=True,
    install_requires=[
        "cloudpickle",
        "cython",
        "pandas",
        "PyGithub",
        "requests",
        "rpa",
        "selenium==4.2"
    ],
    include_dirs=[np.get_include()],
    ext_modules=ext_modules
)
