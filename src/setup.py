#
#   Copyright 2020 The SpaceONE Authors.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import os
from setuptools import setup, find_packages

setup(
    name="spaceone-cost-analysis",
    version=os.environ.get("PACKAGE_VERSION"),
    description="SpaceONE cost analysis service",
    long_description="",
    url="https://www.spaceone.dev/",
    author="MEGAZONE SpaceONE Team",
    author_email="admin@spaceone.dev",
    license="Apache License 2.0",
    packages=find_packages(),
    install_requires=[
        "spaceone-core==2.0.96",
        "spaceone-api==2.0.280",
        "pandas==2.0.3",
        "numpy==1.24.4",
        "jinja2==3.1.4",
        "finance-datareader==0.9.94",
        "plotly==5.24.1",
        "bs4==0.0.2",
    ],
    package_data={
        "spaceone": [
            "cost_analysis/template/*.html",
        ]
    },
    zip_safe=False,
)
