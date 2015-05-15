#!/usr/bin/env python

from setuptools import setup

setup(
	name="py-yarn",
	version='0.1.0',
	packages=['yarn'],
	author='Avishai Ish-Shalom',
	author_email='avishai@fewbytes.com',
	license='Apache V2',
	keywords='yarn hadoop',
	description='A Hadoop Yarn API client',
	install_requires=['snakebite']
	)