# bqwrapper

Wrapper for BigQuery Go package (https://godoc.org/google.golang.org/api/bigquery/v2)

This is mainly because the package is missing a few functions that I needed to use for my project.

 - Insert rows from json/csv file
 - Select rows from BigQuery and dump to json/csv file

These functions are available only if you use Google cloud storage, but not available if you want to do it from/to your own server or laptop.

Source here is a simpler version so it's missing logs, etc. Just hoping this would help other people who have same problem as I did :)


There's only 2 public function:

## Load

Load(projectID, datasetID, tableID, jwtFile, schemaFile, sourceFile, proxy string) error
 
Reads in schemaFile for table schema and sourceFile for the actual data, then insert the data in BigQuery (projectID, datasetID, tableID).

  If dataset and/or table doesn't exist, it'll automaticall create them.

If proxy is set, it'll use the proxy.

## Dump

Dump(projectID, jwtFile, output, fileFormat, delimiter, query, proxy, pretty, printFields, nocache bool, timeout int64) error

Runs the query, then dump the data to the output.

Currently supports only json and csv fileFormat.

If format is json, optional parameter "pretty" is available.

If this is set, the json output will be formatted.

If format is csv, optional parameters "delimiter", "printFields" are available. 

If "delimiter" is set, it'll use this character as a delimiter (default is tab). 

If "printFields" is set, the csv output will have field names on top of the file.
