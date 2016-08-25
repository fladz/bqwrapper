# bqwrapper

Wrapper for BigQuery Go package (https://godoc.org/google.golang.org/api/bigquery/v2)

This is mainly because the package is missing a few functions that I needed to use for my project.

 - Insert rows from json/csv file
 - Select rows from BigQuery and dump to json/csv file

These functions are available only if you use Google cloud storage, not your own server or laptop.

Source here is a simpler version so it's missing logs, etc. Just hoping this would help other people who have same problem as I did :)

