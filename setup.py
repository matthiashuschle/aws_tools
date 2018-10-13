#!/usr/bin/env python

from distutils.core import setup

setup(name='AWSGlacierManager',
      version='0.1.0',
      description='Provides backup, encryption, recovery to/from AWS Glacier',
      author='Matthias Huschle',
      author_email='matthiashuschle@gmail.com',
      packages=['aws_glacier_manager'],
      requires=[
          'cryptography',
          'pynacl'
      ]
     )