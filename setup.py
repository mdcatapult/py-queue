# copyright 2022 Medicines Discovery Catapult
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -*- coding: utf-8 -*-
from setuptools import setup, find_packages
from pathlib import Path
this_directory = Path(__file__).parent
readme_description = (this_directory / "README.md").read_text()

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
      long_description=readme_description,
      long_description_content_type='text/markdown',
      url='http://github.com/mdcatapult/py-queue',
      author='Medicines Discovery Catapult',
      author_email='SoftwareEngineering@md.catapult.org.uk',
      license='Apache V2',
      packages=find_packages('src'),
      package_dir={'': 'src'},
      install_requires=[
          'klein_config>=3.0.0',
          'pika>=1.1.0,<2.0.0',
          'requests>=2.23.0,<3.0.0'
      ],
      zip_safe=True,
      include_package_data=True)
