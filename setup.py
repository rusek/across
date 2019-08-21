from setuptools import setup
import os.path

import across


with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as handle:
    long_description = handle.read()


setup(
    name='across',
    version=across.__version__,
    author='Krzysztof Rusek',
    author_email='savix5@gmail.com',
    description='Run Python code across the universe',
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.rst')).read(),
    long_description_content_type='text/x-rst',
    url='https://github.com/rusek/across',
    project_urls={
        'Documentation': 'https://acrosspy.readthedocs.io/',
        'Source code': 'https://github.com/rusek/across',
        'Issue tracker': 'https://code.djangoproject.com/',
    },
    license='MIT',
    packages=['across'],
    platforms=['any'],
    python_requires='>=3.4',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Software Development :: Object Brokering',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
