import numpy as np

from glob import glob
from setuptools import Extension
from setuptools import setup, find_packages
from os.path import basename, join, splitext
from Cython.Build import cythonize

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
    ext_modules=cythonize([
            Extension("cyutils.vector_as_numpy", sources=[join("src", "pyutils",
                    "cyutils", "vector_as_numpy.pyx")])
        ],
        compiler_directives = {"language_level": "3"},
    ),
    include_dirs=[
        np.get_include(),
        join("src", "pyutils", "cyutils", "vector_as_numpy.pxd")
    ]
)
