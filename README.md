[![Build Status](https://travis-ci.org/okfn/data-quality-cli.svg)](https://travis-ci.org/okfn/data-quality-cli)
[![Coverage Status](https://coveralls.io/repos/okfn/data-quality-cli/badge.svg)](https://coveralls.io/r/okfn/data-quality-cli)


USE TABULATOR IN TASKS


# Data Quality CLI

A command line tool that assesses the data quality of a set of data sources (e.g.: CSV files of open data published by a government).

## What's it about?

The `dq` (alias: `dataquality`) CLI is a batch processor

is for administering data that belongs to a Spend Publishing

Dashboard (dashboard data is in itself a github repository:
<a href="https://github.com/okfn/spd-data-example">see the example repository here</a>).

The proposed workflow is this:

* A deployer/administrator runs the validation over a set of data sources at regular intervals
* The data is managed in a git repository (eg), which the developer has locally
* The deployer/administrator should have a config file for each Spend Publishing
Dashboard she is administering
* The deployer/administrator, or possibly content editor, occasionally updates
the `sources.csv` file in the data directory with new data sources
* Periodically (once a month, once a quarter), the deployer/administrator does
`dq run /path/to-config.json --deploy`. This builds a new set of results for the data,
and deploys the updated data back to the central data repository (i.e: GitHub)
* As the Spend Publishing Dashboard is a pure client-side application, as soon as updated
data is deployed, the app will start working with the updated data.

Note that  deployer/administrator does not need a new `dq` environment per Spend Publishing
Dashboard that she administers. Rather, there must be a config file per dashboard,
based on `dq-config.example.json`. For more info about the structure of the config file
see [this section](###structure-of-jsonconfig).

`dq` currently provides two commands: `run` and `deploy`. Read more about these below.

## Install

```
pip install git+https://github.com/okfn/dataquality-cli.git#egg=dataquality
```

## Use

```
dq --help
```

### Run

```
dq run /path/to/config.json --deploy
```

Runs a *data quality assessment* on all data sources in a data repository.

* Writes aggregated results to the results.csv.
* Writes run meta data to the run.csv.
* If `--deploy` is passed, then also commits, tags and pushes the new changes back to the data repositories central repository.

### Deploy

```
dq deploy /path/to/config.json
```

Deploys this Data Quality repository to a remote.

### Structure of json config

```json
{
  # folder that contains the source_file and publisher_file
  "data_dir": "data",

  # folder that will store each source as local cache
  "cache_dir": "fetched",

  # file that will contain the result for each source
  "result_file": "results.csv",

  # file  that will contain the report for each collection of sources
  "run_file": "runs.csv",

  # file containing the collection of sources that will be analyzed
  "source_file": "sources.csv",

  # file containing the publishers of the above mentioned sources
  "publisher_file": "publishers.csv",

  # will contain the results for each publisher
  "performance_file": "performance.csv",

  "remotes": ["origin"],
  "branch": "master",

  # options for GoodTables ("http://goodtables.readthedocs.org/en/latest/")
  "goodtables": {

    # set base url for the report links
    "goodtables_web": "http://goodtables.okfnlabs.org",

    "arguments": {

      # options for pipeline ("http://goodtables.readthedocs.org/en/latest/pipeline.html")
      "pipeline": {

        # what processors will analyze every pipeline
        "processors": ["structure", "schema"],

        # specify encoding for every pipeline 
          (use this if all the files have the same encoding)
        "encoding": "ISO-8859-2",

         # pass options to procesors ("http://goodtables.readthedocs.org/en/latest/pipeline.html#validator-options")
        "options": {
          "schema": {"case_insensitive_headers": true}
        }
      },

      # options for batch ("http://goodtables.readthedocs.org/en/latest/batch.html")
      "batch": {

        # column from source_file containing path/url to data source
        "data_key": "data",

        # column from source_file containing path/url to schema
        "schema_key": "schema",

        # column from source_file containing file format (csv, xls)
        "format_key": "format",

        # column from source_file containings file encoding
          (use this if you want to specify encoding for each source separately)
        "encoding_key": "encoding",

        # time in seconds to wait between pipelines
        "sleep": 2,

        # execute something after the analysis of a batch is finished
        "post_task": "",

        # execute something after the analysis  of a pipeline is finished
        "pipeline_post_task": "",
      }
    }
  }
}
```

### Schema

`Data Quality CLI` expects the following structure of the project folder, where 
the names of files and folders are the ones defined in the json config given to  `dq run`:

```
project
│
└──────data_dir
    │   source_file
    │   publisher_file
    │   run_file
    │   result_file
    │   performance_file
    │
    └───cache_dir

```