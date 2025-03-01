from setuptools import setup, find_packages

setup(
    name="windsurf-stock",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "akshare",
        "pandas",
        "pytest",
        "pytest-asyncio",
        "pytest-cov",
        "loguru"
    ],
    python_requires=">=3.8",
)
