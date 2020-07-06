# FttX Analyse & Data transfer

This is the repository for FttX analysis and data transfer.

## Running the application locally

1. Python environment setup

```
export VENV=~/env
python3 -m venv $VENV
source $VENV/bin/activate
pip install -r requirements.txt
```

2. Obtain google application credentials needed for data transfer

Download the required service account key from the google cloud console. This key is needed to access resources on the Google Cloud Platform. More info about authentication with a keyfile can be found [here](https://cloud.google.com/docs/authentication/getting-started).

3. Running the data transfer

```
python3 semi_consume.py
```
