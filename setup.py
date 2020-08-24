from setuptools import setup

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="reagan",
    version="1.5.8",
    description="Package for streamlining credentials, data connections, and data flow",
    url="https://github.com/Carat-GM-TechnologyOperations/reagan",
    author="Douglas Schuster",
    author_email="douglas.schuster@carat.com",
    packages=["reagan"],
    zip_safe=False,
    install_requires=requirements,
)

