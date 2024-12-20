#!/usr/bin/env python3

import sys

from urllib.request import urlopen

from logpyle import __version__

sys._BUILDING_SPHINX_DOCS = True

_conf_url = \
    "https://raw.githubusercontent.com/matthiasdiener/sphinxconfig/main/sphinxconfig.py"
with urlopen(_conf_url) as _inf:
    exec(compile(_inf.read(), _conf_url, "exec"), globals())

# General information about the project.
project = "logpyle"
copyright = "2017, Andreas Kloeckner"
author = "Andreas Kloeckner"
version = __version__
release = __version__


intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "pymbolic": ("https://documen.tician.de/pymbolic/", None),
    "pytools": ("https://documen.tician.de/pytools/", None),
    "mpi4py": ("https://mpi4py.readthedocs.io/en/stable/", None),
}


