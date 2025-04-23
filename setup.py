from setuptools import setup, find_packages

setup(
    name="kcycle",
    version="0.1",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
)


#
# cd KCycle_competition
# pip install -e .