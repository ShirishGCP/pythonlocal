from setuptools import setup, find_packages

setup(
    name='even_number_checker',
    version='0.1.0',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    install_requires=[
        # List production dependencies here if any
    ],
    include_package_data=True,
    # Add other metadata as needed
)
