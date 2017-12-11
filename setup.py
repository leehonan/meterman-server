from setuptools import setup

setup(name='meterman',
    version='0.1',
    classifiers=[
        'Programming Language :: Python :: 3.6',
    ],
    python_requires='>=3.5',
    description='meterman',
    url='http://leehonan/meterman',
    author='Lee Honan',
    author_email='lee at leehonan dot com',
    license='MIT',
    packages=['src'],
    include_package_data=True,
    install_requires=[
        'arrow', 'flask', 'flask_httpauth', 'flask_restful', 'ipaddress', 'uptime', 'pytest', 'argparse', 'pyserial'
    ],
    zip_safe=True)