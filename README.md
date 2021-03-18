# Honeycomb SQLAlchemy

Simple honeycomb instrumentation for SQLAlchemy. Adds spans to an already-initialised beeline when SQLAlchemy queries are issued.

The code here is mostly lifted from the official Honeycomb Python Beeline.

The differences are:

* It can be used in any Python application using SQLAlchemy, and doesn't assume Flask.
* A small bugfix raised against the official lib https://github.com/honeycombio/beeline-python/issues/159
* A bigger bugfix around threading?
* It has tests


## Usage

```
import honeycomb_sqlqlchemy
beeline.init(...)
honeycomb_sqlqlchemy.install()
```

Beeline will now collect spans for SQLAlchemy queries.