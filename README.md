# ITH Dashboard

This is the repository of ITH Dashboard. It contains a Dash dashboard with accompanying files. To get started, first make a copy of export.sh.template and fill in the environment variables needed.

```
mv export.sh.template export.sh
```

Then run the export.sh script to set the environment variables that are needed for the dashboard to run locally. Make sure not to commit this file!

```
./export.sh
```

Then install the in your python environment.

```
pip install -r requirements.txt
```

To run the Dash application, use the following command, including live reload.

```
gunicorn --pythonpath app.py app:server --reload --workers=3
```
