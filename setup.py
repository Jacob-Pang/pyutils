import os
import numpy as np

from glob import glob
from setuptools import setup, find_packages
from os.path import basename, splitext
from Cython.Build import cythonize
from src.pyutils.cython_templating import make_cython_templates

with open("README.md", 'r') as readme:
    long_description = readme.read()

setup(
    name="pyutils",
    version="1.0",
    description="General utility modules for python projects",
    long_description=long_description,
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[
        splitext(basename(path))[0] for path in glob("src/*.py")
    ],
    include_package_data=True,
    install_requires=[
        "cloudpickle",
        "pandas",
        "PyGithub",
        "requests",
        "rpa",
        "selenium==4.2"
    ],
    ext_modules=cythonize(
        make_cython_templates(os.path.join("src", "pyutils", "array_queue", "templates.json"))
    ),
    include_dirs=[
        np.get_include()
    ]
)
