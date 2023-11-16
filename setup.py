from setuptools import setup, find_packages

setup(
    name='ads_bidding_utils',
    version='0.3.14',
    packages=find_packages(),
    description='Python package for ads bidding utils',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Yasmin SarcheshmehPour',
    author_email='sarcheshmehpours@gmail.com',
    url='https://github.com/sahelyiyi/ads-bidding-utils',
    install_requires=[
        'pandas==1.2.4',
        'minio==7.2.0',
        'kubernetes',
    ],
    python_requires='>=3.8',
)