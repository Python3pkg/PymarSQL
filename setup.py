#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

if sys.version_info[:2] < (2, 7):
    sys.exit("ERROR: Python >= 2.7 required")

if sys.version_info[0] > 2:
    sys.exit("ERROR: Python < 3.0 required")

from distutils.core import setup

setup(
    name='PymarSQL',
    version="0.1.1",
    description="Pymar addition to easily scale your operations with tables of relational data bases.",
    url="https://github.com/alexgorin/PymarSQL",
    author="Alexander Gorin",
    author_email='saniagorin@gmail.com',
    license='MIT',
    packages=[
        'pymar.plugins',
        'pymar.plugins.datasources',
    ],
    install_requires=[
          'pymar',
          'sqlalchemy'
    ],
    keywords=[
        'python', 'scale', 'distribute', 'map', 'reduce', 'mongo'
    ]
)
