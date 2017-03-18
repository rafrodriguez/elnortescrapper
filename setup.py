from setuptools import setup

def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='elnortescrapper',
    version='1.0.0',
    install_requires=['lxml','bs4', 'pandas'],
    description='Web scrapper for El Norte Avisos de Ocasión real estate classified ads',
    url='http://github.com/rafrodrz/reformascrapper',
    keywords='elnorte avisos ocasion venta casas anuncios',
    author='Rafael Rodriguez Morales',
    author_email='rafarodrz@gmail.com',
    license='BSD',
    packages=['elnortescrapper'],
    zip_safe=False)
