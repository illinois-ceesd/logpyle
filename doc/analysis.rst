========
Analysis
========
Logpyle is distributed with tools that can assist in analyzing
data stored in its sqlite output files.


``runalyzer``, for analysis taking advantage of powerful SQL queries.

``runalyzer-gather``, for joining, or gathering, multiple sqlite
files into one.

``htmlalyzer``, for basic analysis benefiting from a GUI.



Runalyzer
=========
``runalyzer`` is a CLI tool that allows plotting, exporting, and viewing of
data in the form sqlite files exported by logpyle, all using SQL commands.

When running ``runalyzer`` without gathering sqlite files together,
``runalyzer`` will `auto gather` these files together and create an
in memory database as if ``runalyzer-gather`` had been run.

Running the binary
------------------
>>> runalyzer log.sqlite

Describing Schema
-----------------
TODO

* getting warnings, logging, constants

Basic Usage
-----------
TODO

* essentially what running .help says
* plotting multiple runs by splitting run ids (look into schema)

Advanced Features
-----------------
TODO

* running with the script flag
* running with the issuing raw commands



Runalyzer-gather
================
``runalyzer-gather`` takes in sqlite files from :mod:`logpyle` and combines them
all into a single sqlite summary file readable by ``runalyzer``.

Running the binary
------------------
>>> runalyzer-gather summary.sqlite log.sqlite


HTMLalyzer
==========
``htmlalyzer`` is a GUI for quickly analyzing files :mod:`logpyle` sqlite files. It
features plotting multiple quantities together and analyzing multiple runs
at the same time.

HTMLalyzer uses PyScript, a framework that runs python in the browser and
runs Pyodide, CPython compiled to WebAssembly.

Running the binary
------------------
>>> htmlalyzer

This will attempt to open a new tab in your default browser
allowing you to upload sqlite files to be analyzed.

passing in the ``--build`` flag will rebuild the HTML file
before serving it.

Usage
-----
After the virtual environment has been setup, click the ``Add file`` button
to add a pannel for analysis.

To analyze a run, click on the browse button to upload one or more files.
These files will be gathered together under the hood. You can then select
quantites from the X and Y dropdowns. If you would like to keep track of
multiple quantities in the same graph, you can press ``Add Line to Plot``
to add a Y dropdown.

Any stdout from python will display in the terminal below any of the panels.

