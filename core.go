package bqwrapper

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Load data to BigQuery using source files (json or csv) using HTTP POST.
func Load(projectID, datasetID, tableID, jwtFile, schemaFile, sourceFile, proxy string) error {
	// All params are required.
	if projectID == "" || datasetID == "" || tableID == "" ||
		jwtFile == "" || schemaFile == "" || sourceFile == "" {
		return errors.New("missing params")
	}

	// Check and set source format.
	var format string
	switch {
	case strings.HasSuffix(sourceFile, ".json"):
		format = "NEWLINE_DELIMITED_JSON"
	case strings.HasSuffix(sourceFile, ".csv"):
		format = "CSV"
	default:
		return errors.New("Unsupported source file format")
	}

	// Set proxy if requested.
	if proxy != "" {
		os.Setenv("HTTP_PROXY", proxy)
	}

	// Start BigQuery service.
	client, err := oauthClient(jwtFile)
	if err != nil {
		return err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		return err
	}

	// First, check if the dataset already exists.
	// If it doesn't yet, create before calling load job.
	if err = datasetCreateIfNotExists(bq, projectID, datasetID); err != nil {
		return fmt.Errorf("Error checking/creating dataset - %s", err)
	}

	// Load the schema configuration.
	var fields []TableField
	by, err := ioutil.ReadFile(schemaFile)
	if err != nil {
		return fmt.Errorf("Error reading schema - %s", err)
	}
	if err = json.Unmarshal(by, &fields); err != nil {
		return fmt.Errorf("Error reading schema - %s", err)
	}

	// Generate job configuration.
	var bqConf = jobConf{
		Conf: jobMainConf{
			Load: jobLoadConf{
				Format: format,
				Schema: Schema{Fields: fields},
				Destination: Destination{
					ProjectID: projectID,
					DatasetID: datasetID,
					TableID:   tableID,
				},
			},
		},
	}
	var confBytes []byte
	if confBytes, err = json.Marshal(bqConf); err != nil {
		return err
	}

	// Read in source.
	data, err := ioutil.ReadFile(sourceFile)
	if err != nil {
		return err
	}

	// Initiate the load request.
	req, err := http.NewRequest(
		"POST",
		"https://www.googleapis.com/upload/bigquery/v2/projects/"+projectID+"/jobs?uploadType=resumable",
		bytes.NewBuffer(confBytes),
	)
	if err != nil {
		return fmt.Errorf("Error creating initial request - %s", err)
	}

	// Set header values.
	req.Header.Set("X-Upload-Content-Type", "application/json")
	req.Header.Set("X-Upload-Content-Length", strconv.Itoa(len(data)))
	req.Header.Set("Content-Length", strconv.Itoa(len(confBytes)))
	req.Header.Set("Content-Type", "application/json")

	// Send the request and get upload uri.
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error in intial request - %s", err)
	}
	if res.StatusCode != http.StatusOK {
		code := res.Status
		var errRes ErrorResponse
		json.NewDecoder(res.Body).Decode(&errRes)
		res.Body.Close()
		return fmt.Errorf("did not get OK, got %s (%s)",
			code, errRes.Error.Message)
	}
	loc, err := res.Location()
	if err != nil {
		res.Body.Close()
		return fmt.Errorf("error getting Location header - %s", err)
	}
	res.Body.Close()

	if req, err = http.NewRequest("POST", loc.String(), bytes.NewBuffer(data)); err != nil {
		return fmt.Errorf("Error creating request - %s", err)
	}
	if res, err = client.Do(req); err != nil {
		return fmt.Errorf("Error in response - %s", err)
	}
	if res.StatusCode != http.StatusOK {
		code := res.Status
		res.Body.Close()
		return fmt.Errorf("Did not get OK, got %s", code)
	}

	// Need JobID to check on its status.
	r, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("Error reading response - %s", err)
	}
	var response bigquery.Job
	if err = json.Unmarshal(r, &response); err != nil {
		res.Body.Close()
		return fmt.Errorf("Error decoding response - %s", err)
	}
	if response.JobReference.ProjectId != projectID {
		res.Body.Close()
		return fmt.Errorf("Returned ProjectID %s != configured ID %s",
			response.JobReference.ProjectId, projectID)
	}
	job := response.JobReference.JobId

	// Now wait until this job is done.
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()
	var done bool
	for {
		select {
		case <-tick.C:
			if done, err = jobDone(bq, projectID, job); err != nil {
				return err
			}
			if done {
				return nil
			}
		}
	}

	return nil
}

// Select rows from BigQuery, then dump to a json or csv file.
//
// If output is json, optional "pretty" flag is available.
//   If this is true, output json will be formatted.
// If output is csv, optional "delimiter" flag is available.
//   If this is set, this is used to separate fields.
// If output is csv, optional "printFields" flag is available.
//   If this is set, output file will have field names written in ti.
//
// This function takes query to run, but you can easily modify/add to select the entire table too.
func Dump(projectID, jwtFile, output, fileFormat, delimiter, query, proxy string, pretty, printFields bool, timeout int64, nocache bool) error {
	// Required params check.
	if projectID == "" || jwtFile == "" || output == "" || fileFormat == "" || query == "" {
		return errors.New("no paramters")
	}

	// Check and set per filetype option.
	switch strings.ToLower(fileFormat) {
	case "json":
		fileFormat = "json"
	case "csv":
		fileFormat = "csv"
		if delimiter == "" {
			// Default "," (comma)
			delimiter = ","
		}
	default:
		return errors.New("Unsupported output file format")
	}

	// Set proxy if requested.
	if proxy != "" {
		os.Setenv("HTTP_PROXY", proxy)
	}

	// Start BigQuery service.
	client, err := oauthClient(jwtFile)
	if err != nil {
		return err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		return err
	}

	// Create request.
	conf := &bigquery.QueryRequest{
		Kind:  "bigquery#queryRequest",
		Query: query,
	}

	// Set timeout if passed.
	if timeout != 0 {
		conf.TimeoutMs = timeout
	}

	// If "nocache" is set, we do not use cached data but run the query against
	// the actual table(s) the query refers to.
	if nocache {
		conf.UseQueryCache = new(bool)
	}

	// Send it.
	req := bq.Jobs.Query(projectID, conf)
	res, err := req.Do()
	if err != nil {
		return fmt.Errorf("Error sending request - %s", err)
	}

	// Verify response.
	if len(res.Errors) != 0 {
		return fmt.Errorf("%d errors returned", len(res.Errors))
	}

	// Wait until we get all rows.
	// Since number of rows returned from BigQuery at a time is limited, it's possible
	// that we got only part of results.
	var total = res.TotalRows
	var retrieved = len(res.Rows)
	var rows = res.Rows

	// Make sure we got rows.
	if res.Schema == nil {
		return errors.New("Error getting reply, no schema data returned")
	}
	var fields = res.Schema.Fields

	if retrieved != int(total) {
		// Still rows waiting to be requested, request again until we get all.
		var jobID = res.JobReference.JobId
		var token = res.PageToken
		if res.Schema == nil {
			return errors.New("Error getting reply, no data returned")
		}
		for int(total) != retrieved {
			req := bq.Jobs.GetQueryResults(projectID, jobID)
			req.PageToken(token)
			req.StartIndex(uint64(retrieved))
			res, err := req.Do()
			if err != nil {
				return fmt.Errorf("Error getting query results - %s", err)
			}
			if len(res.Errors) != 0 {
				return fmt.Errorf("%d errors returned", len(res.Errors))
			}
			token = res.PageToken
			rows = append(rows, res.Rows...)
			retrieved += len(res.Rows)
		}
	}

	// Finished getting rows, convert it to map of interface for write.
	result, err := toRows(fields, rows)
	if err != nil {
		return err
	}

	// Write out to a file.
	if fileFormat == "json" {
		return dumpJSON(result, output, pretty)
	} else {
		return dumpCSV(result, output, delimiter, printFields)
	}

	return errors.New("something went wrong!!")
}

// Check status of the requested job.
func jobDone(bq *bigquery.Service, pid, jid string) (bool, error) {
	// Send the Job status call.
	call := bq.Jobs.Get(pid, jid)
	res, err := call.Do()
	if err != nil {
		return false, err
	}

	// If there was an application error from BigQuery, the error will not be set in error
	// from the library's Do() call (FML).
	if len(res.Status.Errors) != 0 {
		return false, fmt.Errorf("%d errors returned", len(res.Status.Errors))
	}

	switch res.Status.State {
	case "PENDING", "RUNNING":
		return false, nil
	case "DONE":
		return true, nil
	}

	return false, fmt.Errorf("Unknown job status returned - %s (%s,%s)",
		res.Status.State, pid, jid)
}

// Check if requested dataset exists under the project and create the dataset
// if it doesn't exist yet.
func datasetCreateIfNotExists(bq *bigquery.Service, projectID, datasetID string) error {
	// Check list of datasets in the requested project.
	checkReq := bq.Datasets.List(projectID)
	res, err := checkReq.Do()
	if err != nil {
		return fmt.Errorf("Error checking datasets - %s", err)
	}

	for _, dataset := range res.Datasets {
		if dataset.DatasetReference.DatasetId == datasetID {
			// The dataset already exists, no need to create.
			return nil
		}
	}

	// If the response has more list, loop all through and try finding
	// this dataset.
	var token = res.NextPageToken
	for token != "" {
		checkReq.PageToken(token)
		if res, err = checkReq.Do(); err != nil {
			return err
		}
		for _, dataset := range res.Datasets {
			if dataset.DatasetReference.DatasetId == datasetID {
				return nil
			}
		}
		token = res.NextPageToken
	}

	// Dataset not exist, need to create.
	createReq := bq.Datasets.Insert(projectID, &bigquery.Dataset{
		DatasetReference: &bigquery.DatasetReference{DatasetId: datasetID},
	})
	if _, err := createReq.Do(); err != nil {
		return err
	}

	return nil
}

// Parse JWT file and initiate http.Client with it.
func oauthClient(jwtFile string) (*http.Client, error) {
	// Parse JWT file and set up credentials.
	by, err := ioutil.ReadFile(jwtFile)
	if err != nil {
		return nil, err
	}
	conf, err := google.JWTConfigFromJSON(by, bigquery.BigqueryScope)
	if err != nil {
		return nil, err
	}
	return conf.Client(oauth2.NoContext), nil
}

// Write out json file with given interface map.
func dumpJSON(data []map[string]interface{}, output string, pretty bool) error {
	// Open file for write.
	f, err := os.Create(output)
	if err != nil {
		return err
	}

	// Marshal the json data then write down.
	if pretty {
		by, err := json.MarshalIndent(data, "", "\t")
		if err != nil {
			return err
		}
		if _, err = f.Write(by); err != nil {
			os.Remove(output)
			return err
		}
	} else {
		enc := json.NewEncoder(f)
		if err = enc.Encode(data); err != nil {
			os.Remove(output)
			return err
		}
	}

	return nil
}

// Write out a csv file with the given interface value.
// If "printField" is set, the output will have field names in the beginning of file.
// The "fields" has to be passed to ensure values for fields and the order are guaranteed.
func dumpCSV(data []map[string]interface{}, output, delim string, printFields bool) error {
	// Get field names from the source data.
	var fields sort.StringSlice
	for key, _ := range data[0] {
		fields = append(fields, key)
	}

	// Sort alphabetically so the field order is always same.
	fields.Sort()

	// Create slic eof strings so it'll be csv writer compatible.
	var lines = make([][]string, len(data), len(data)+1)
	var val interface{}
	var ok bool
	var i int
	for num, rows := range data {
		lines[num] = make([]string, len(fields))
		for i = 0; i < len(fields); i++ {
			if val, ok = rows[fields[i]]; ok {
				lines[num][i] = fmt.Sprintf("%v", val)
			} else {
				lines[num][i] = ""
			}
		}
	}

	// If we need to print fields, prepend it.
	if printFields {
		lines = append([][]string{fields}, lines...)
	}

	// Open file for write.
	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	// Set custom delimiter if specified.
	if delim != "" {
		// Tab is a special word. If a word "tab" is defined, use tab.
		if delim == "tab" {
			w.Comma = rune('\t')
		} else {
			w.Comma = rune(delim[0])
		}
	}

	w.WriteAll(lines)
	return nil
}

// Convert rows and field names returned from BigQuery into map of interface.
func toRows(fields []*bigquery.TableFieldSchema, rows []*bigquery.TableRow) ([]map[string]interface{}, error) {
	// Get list of field names first.
	var names []fieldType
	var name, ftype string
	for _, field := range fields {
		name, ftype = walkFields("", field)
		names = append(names, fieldType{name: name, ftype: ftype})
	}

	// Now read values and save in return slice.
	var results = make([]map[string]interface{}, 0)
	var result map[string]interface{}
	var i int
	var err error
	var row *bigquery.TableRow
	var cell *bigquery.TableCell
	var ival int64
	var fval float64
	var bval bool
	for _, row = range rows {
		result = make(map[string]interface{}, len(row.F))
		for i, cell = range row.F {
			// If the cell value is null, just save the field name
			// with null value.
			if cell.V == nil {
				result[names[i].name] = nil
				continue
			}

			// What type of data is it?
			switch names[i].ftype {
			case "STRING":
				result[names[i].name] = cell.V
			case "INTEGER", "TIMESTAMP":
				if ival, err = strconv.ParseInt(cell.V.(string), 10, 64); err != nil {
					return nil, fmt.Errorf("Invalid %s value (%s) - %s", names[i].name, cell.V, err)
				}
				result[names[i].name] = ival
			case "FLOAT":
				if fval, err = strconv.ParseFloat(cell.V.(string), 64); err != nil {
					return nil, fmt.Errorf("Invalid %s value (%s) - %s", names[i].name, cell.V, err)
				}
				result[names[i].name] = fval
			case "BOOLEAN":
				if bval, err = strconv.ParseBool(cell.V.(string)); err != nil {
					return nil, fmt.Errorf("Invalid %s value (%s) - %s", names[i].name, cell.V, err)
				}
				result[names[i].name] = bval
			default:
				return nil, fmt.Errorf("Unsupported field type %s on %s", names[i].ftype, names[i].name)
			}
		}
		results = append(results, result)
	}

	return results, nil
}

// Walk through the given schema recursively until there's no more nested loop inside.
// Returns field name and field type.
func walkFields(prefix string, schema *bigquery.TableFieldSchema) (string, string) {
	if schema.Type != "RECORD" {
		// No nested field inside, return this one.
		if prefix == "" {
			return schema.Name, schema.Type
		} else {
			return prefix + "." + schema.Name, schema.Type
		}
	}

	for _, field := range schema.Fields {
		return walkFields(field.Name, field)
	}

	return "", ""
}
