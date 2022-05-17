from setuptools import setup  # type: ignore

from henry import __version__ as pkg

NAME = "henry"
VERSION = pkg.__version__
REQUIRES = ["looker-sdk>=21", "tabulate"]

setup(
    author="Joseph Axisa",
    author_email="jax@looker.com",
    description="A Looker Cleanup Tool",
    install_requires=REQUIRES,
    license="MIT",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    keywords=["Looker Cleanup", "Looker Henry", "Henry"],
    name=NAME,
    packages=["henry", "henry/commands", "henry/modules"],
    data_files=[("henry/.support_files/", ["henry/.support_files/help.rtf"])],
    include_package_data=True,
    entry_points={"console_scripts": ["henry=henry.cli:main"]},
    python_requires=">=3.7.0",
    url="https://pypi.python.org/pypi/henry",
    version=VERSION,
)
