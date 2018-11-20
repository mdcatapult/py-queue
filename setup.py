# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

def readme():
    with open('README.md') as f:
        return f.read()

setup(name='klein_queue',
      version='0.1.7',
      description='RabbitMQ integration',
      url='http://gitlab.mdcatapult.io/informatics/klein/klein_queue',
      author='Matt Cockayne',
      author_email='matthew.cockayne@md.catapult.org.uk',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          'klein_config',
          'pika',
          'requests'
      ],
      zip_safe=True)