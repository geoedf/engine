from setuptools import setup,find_packages

def readme():
    with open('README.md') as f:
        return f.read()

setup(name='geoedfengine',
      version='0.1',
      description='GeoEDF Plug-and-play Workflow Engine',
      url='http://github.com/geoedf/engine',
      author='Rajesh Kalyanam',
      author_email='rkalyanapurdue@gmail.com',
      license='MIT',
      python_requires='~=3.7',
      packages=find_packages(),
      scripts=['bin/build-conn-plugin-subdax','bin/build-proc-plugin-subdax'],
      install_requires=['pyyaml','regex'],
      include_package_data=True,
      zip_safe=False)
