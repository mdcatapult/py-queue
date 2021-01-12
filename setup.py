# -*- coding: utf-8 -*-
from setuptools import setup, find_packages


def readme():
    with open('README.md') as f:
        return f.read()


__version__ = ''
exec(open("./src/version.py").read())
if __version__ == '':
    raise RuntimeError("unable to find application version")

setup(name='klein_queue',
      version=__version__,
      description='RabbitMQ integration',
      url='http://gitlab.mdcatapult.io/informatics/klein/klein_queue',
      author='Medicines Discovery Catapult',
      author_email='SoftwareEngineering@md.catapult.org.uk',
      license='MIT',
      packages=find_packages('src'),
      package_dir={'': 'src'},
      install_requires=[
          'klein_config>=3.0.0,<4.0.0',
          'pika>=1.1.0,<2.0.0'
      ],
      zip_safe=True)
