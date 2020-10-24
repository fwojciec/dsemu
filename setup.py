from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="dsemu",
    version="0.1.2",
    url="https://github.com/fwojciec/dsemu",
    license="MIT",
    author="Filip Wojciechowski",
    author_email="fwojciec@gmail.com",
    description="A wrapper around the datastore emulator instance for use in tests.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=("tests",)),
    install_requires=[],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
    ],
)
