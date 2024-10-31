from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="s3nosqldb",
    version="0.1.0",
    author="Dan Zhang",
    author_email="dan.zhang0224@gmail.com",
    description="A NoSQL Database Implementation using Amazon S3",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/s3nosqldb",
    packages=find_packages(exclude=["tests*"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "boto3>=1.26.0",
        "pyarrow>=7.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "isort>=5.0.0",
            "flake8>=4.0.0",
            "moto>=4.0.0",
        ],
    },
)
