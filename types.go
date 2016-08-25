package bqwrapper

// Internal job configuration struct
type jobConf struct {
	Conf jobMainConf `json:"configuration"`
}
type jobMainConf struct {
	Load jobLoadConf `json:"load"`
}
type jobLoadConf struct {
	Format      string      `json:"sourceFormat"`
	Schema      Schema      `json:"schema"`
	Destination Destination `json:"destinationTable"`
}

// Table schema JSON structs
type Schema struct {
	Fields []TableField `json:"fields"`
}
type TableField struct {
	Name   string       `json:"name"`
	Type   string       `json:"type"`
	Mode   string       `json:"mode"`
	Fields []TableField `json:"fields"`
}
type Destination struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	TableID   string `json:"tableId"`
}

// Internal field type definition
type fieldType struct {
	name  string
	ftype string
}

// Error message structure from BigQuery
type ErrorResponse struct {
	Error ErrorMessage `json:"error"`
}
type ErrorMessage struct {
	Message string `json:"message"`
}
