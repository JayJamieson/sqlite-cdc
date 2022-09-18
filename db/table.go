package db

import (
	"bytes"
	"database/sql"
	_ "embed"
	"regexp"
	"text/template"
)

// Inspiration for implementation are from
// https://github.com/maxpert/marmot/blob/master/db/change_log.go

//go:embed log_table.tmpl
var logTableTemplate string

//go:embed trigger.tmpl
var triggerTemplate string

var logTableTpl *template.Template
var triggerTpl *template.Template

var spaceStripper = regexp.MustCompile(`\r?\n\s*`)

type templateData struct {
	Prefix    string
	Triggers  map[string]string
	Columns   []*sql.ColumnType
	TableName string
}

func init() {
	logTableTpl = template.Must(
		template.New("logTableTemplate").Parse(logTableTemplate),
	)
	triggerTpl = template.Must(
		template.New("triggerTemplate").Parse(triggerTemplate),
	)
}

func buildLogSql(tableName string, columns []*sql.ColumnType) (string, error) {
	buf := new(bytes.Buffer)
	err := logTableTpl.Execute(buf, &templateData{
		Prefix:    "__cdc__",
		Triggers:  map[string]string{"insert": "NEW", "update": "NEW", "delete": "OLD"},
		Columns:   columns,
		TableName: tableName,
	})

	if err != nil {
		return "", err
	}

	return spaceStripper.ReplaceAllString(buf.String(), "\n    "), nil
}
func buildTriggerSql(tableName string, columns []*sql.ColumnType) (string, error) {
	buf := new(bytes.Buffer)

	err := triggerTpl.Execute(buf, &templateData{
		Prefix:    "__cdc__",
		Triggers:  map[string]string{"insert": "NEW", "update": "NEW", "delete": "OLD"},
		Columns:   columns,
		TableName: tableName,
	})

	if err != nil {
		return "", err
	}

	return spaceStripper.ReplaceAllString(buf.String(), "\n    "), nil
}
