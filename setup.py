#!/usr/bin/env python
from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="flask-uwsgi-ws",
    version="0.1.0",
    description="Flask WebSocket extension for uWSGI",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Nidal Alhariri",
    author_email="leve09@gmail.com",
    url="https://github.com/level09/flask-uwsgi-ws",
    packages=find_packages(),
    install_requires=[
        "flask>=2.0.0",
        "uwsgi>=2.0.0",
    ],
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Web Environment",
        "Framework :: Flask",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
    ],
    keywords="flask uwsgi websocket websockets ws async",
)
