import setuptools

setuptools.setup(
    name="utils",
    version="1.0.2",
    scripts=["./scripts/utils"],
    author="Me",
    description="utils python package install",
    url="https://github.com/dexplorer/utils",
    # packages=setuptools.find_packages(),
    packages=[
        "utils",
    ],
    # packages = find_packages(),
    install_requires=[
        "setuptools",
        "requests",
    ],
    python_requires=">=3.12",
)
