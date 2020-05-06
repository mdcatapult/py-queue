# -*- coding: utf-8 -*-
from setuptools import setup, find_packages


def readme():
    with open('README.md') as f:
        return f.read()


setup(name='klein_queue',
      version='0.2.12',
      description='RabbitMQ integration',
      url='http://gitlab.mdcatapult.io/informatics/klein/klein_queue',
      author='Matt Cockayne',
      author_email='matthew.cockayne@md.catapult.org.uk',
      license='MIT',
      packages=find_packages('src'),
      package_dir={'': 'src'},
      install_requires=[
          'klein_config',
          'pika>=1.1.0',
          'requests'
      ],
      zip_safe=True)
