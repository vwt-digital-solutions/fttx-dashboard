# Documentation
Documentation in docstrings is done using [Google Style Python Docstrings](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html).
The docstrings are converted into html documentation using the napoleon plugin.

## Requirements

- Sphinx & extensions

To install run the following command from the /docs directory:

```bash
pip install -r requirements.txt
```

## Build the documentation

```bash
cd fttx-dashboard/docs
sphinx-apidoc -o ./modules/ ../dashboard/ & make html
```

## View the documentation
The build documentation is found in `_build/html/`.

To view the documentation open [index.html](_build/html/index.html) in a browser. 