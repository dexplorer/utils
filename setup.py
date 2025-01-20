import setuptools

setuptools.setup(
    name="package_utils",
    version="1.0",
    scripts=["./scripts/utils"],
    author="Me",
    description="utils python package install",
    packages=[
        "utils",
    ],
    # packages = find_packages(),
    install_requires=[
        "setuptools",
    ],
    python_requires=">=3.12",
)
