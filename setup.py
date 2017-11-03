from setuptools import setup

setup(name='lp',
      version='0.1',
      description='Luke Plausin - personal utilities module',
      author='Luke Plausin',
      author_email='luke.plausin@cloudreach.com',
      url='https://github.com/lukeplausin/MyPythonModule',
      packages=['lp'],
      install_requires=['elasticsearch', 'boto3', 'requests_aws4auth', 'ruamel.yaml'],
      dependency_links=[],
      license='All Rights Reserved',
      entry_points={
          'console_scripts': [
            #   'dataconvert = dataconverters.cli:main'
          ]
      })
