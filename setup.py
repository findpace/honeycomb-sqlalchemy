#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup


setup(
    name="honeycomb-sqlalchemy",
    version="1.0.0",
    description="SQLAlchemy Instrumentation for Honeycomb",
    author="Pace",
    author_email="tech@pacerevenue.com",
    url="http://github.com/findpace/honeycomb-sqlalchemy",
    py_modules=["honeycomb_sqlalchemy"],
    install_requires=["honeycomb-beeline", "sqlalchemy"],
    extras_require={
        "dev": ["coverage==5.5", "pytest==6.2.2", "psycopg2-binary==2.8.6"]
    },
    zip_safe=True,
    license="Apache License, Version 2.0",
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
    ],
)
