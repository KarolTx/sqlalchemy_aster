import os
import re

from setuptools import setup

v = open(os.path.join(os.path.dirname(__file__), 'sqlalchemy_aster', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.rst')

setup(
    name='sqlalchemy_aster',
    version=VERSION,
    description="Teradata Aster for SQLAlchemy",
    long_description=open(readme).read(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Database :: Front-Ends',
    ],
    keywords='SQLAlchemy Aster',
    author='Karol T',
    author_email='',
    license='MIT',
    packages=['sqlalchemy_aster'],
    include_package_data=True,
    tests_require=['nose >= 0.11'],
    test_suite='nose.collector',
    zip_safe=False,
    entry_points={
        'sqlalchemy.dialects': [
            'aster = sqlalchemy_aster.jdbc:AsterDialect_jdbc',
            'aster.jdbc = sqlalchemy_aster.jdbc:AsterDialect_jdbc',
        ]
    }
)
