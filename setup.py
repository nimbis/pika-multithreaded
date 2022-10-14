#!/usr/bin/env python
import os
from setuptools import setup, find_packages

# these internal requirements move often, good thing to do is this:
# https://github.com/pypa/pip/search?q=PipSession
# https://github.com/pypa/pip/search?q=parse_requirements
from pip._internal.network.session import PipSession
from pip._internal.req import parse_requirements

#
# parse requirements.in
#

requires = []  # for package names

requirements = parse_requirements(
    'requirements.in', session=PipSession()
)

for item in requirements:
    if item.requirement:
        requires.append(str(item.requirement))  # always the package name

#
# find_packages exclude, comma-separated
#

exclude_packages = os.getenv('EXCLUDE_PACKAGES', '').split(',')

#
# get long description from README
#
with open("README.md", "r") as file:
    long_description = file.read()

#
# setup script
#

setup(
    name='pika-multithreaded',
    version='1.0.1',
    author='Nimbis Services',
    author_email='info@nimbisservics.com',
    description='A project that enables multithreading support for the Pika package',
    long_description=long_description,
    url="https://github.com/nimbis/pika-multithreaded",
    packages=find_packages(exclude=exclude_packages),
    python_requires='>=3.6',
    include_package_data=True,
    install_requires=requires
)
