from setuptools import setup, find_packages

setup(
    name="Sparktan",
    version=0.01,
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'sparktan = sparktan.cli.sparktan:main',
        ]
    },
    include_package_data=True,
)

