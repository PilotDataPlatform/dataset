# dataset

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg?style=for-the-badge)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.7](https://img.shields.io/badge/python-3.7-green?style=for-the-badge)](https://www.python.org/)

Dataset management service for the Pilot Platform.

## Build and Run
Currently some build dependencies are in a private index and you will need the correct username and secret to use them.

```bash
virtualenv -p python3 venv
source venv/bin/activate
pip install -r requirements.txt
PIP_USERNAME=$PIP_USERNAME PIP_PASSWORD=$PIP_PASSWORD pip install -r internal_requirements.txt
python run.py
```
