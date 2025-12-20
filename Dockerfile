FROM astrocrpublic.azurecr.io/runtime:3.1-7

COPY requirements-kg-rdf.txt .
RUN python -m pip install --no-cache-dir -r requirements-kg-rdf.txt

