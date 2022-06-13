package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

type columnParams struct {
	name         string
	typeName     string
	isNull       bool
	primary      bool
	defaultValue interface{}
}

type DbExplorer struct {
	db                 *sql.DB
	columnsInTablesMap map[string]map[string]columnParams
	tableKeys          []string
	tableIdNameMap     map[string]string
}

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {
	tableIdNameMap := make(map[string]string)
	columnsInTablesMap := make(map[string]map[string]columnParams)
	tableKeys := make([]string, 0)

	tables, err := db.Query("SHOW TABLES;")
	if err != nil {
		return nil, err
	}

	for tables.Next() {
		tableName := ""

		if err := tables.Scan(&tableName); err != nil {
			continue
		}
		tableKeys = append(tableKeys, tableName)

		columnsInTablesMap[tableName] = make(map[string]columnParams)
		queryResult, _ := db.Query("SHOW FULL COLUMNS FROM " + tableName)
		columns, err := parsingSqlQueryResult(queryResult)
		if err != nil {
			return nil, err
		}

		for _, value := range columns {
			name := fmt.Sprintf("%v", value["Field"])
			typeName := fmt.Sprintf("%v", value["Type"])
			var defaultValue interface{}

			if strings.Contains(typeName, "text") || strings.Contains(typeName, "varchar") {
				typeName = "string"
				defaultValue = ""
			}

			if typeName == "int" {
				defaultValue = 0
			}

			isNull := false
			if fmt.Sprintf("%v", value["Null"]) == "YES" {
				isNull = true
			}

			primary := false
			if fmt.Sprintf("%v", value["Key"]) == "PRI" {
				primary = true
				tableIdNameMap[tableName] = name
			}

			columnsInTablesMap[tableName][name] = columnParams{
				name:         name,
				typeName:     typeName,
				isNull:       isNull,
				primary:      primary,
				defaultValue: defaultValue,
			}
		}
	}

	return &DbExplorer{
		db:                 db,
		columnsInTablesMap: columnsInTablesMap,
		tableKeys:          tableKeys,
		tableIdNameMap:     tableIdNameMap,
	}, nil
}

func (d DbExplorer) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		d.handlerGet(rw, r)
	case "PUT":
		d.handlerPut(rw, r)
	case "POST":
		d.handlerPost(rw, r)
	case "DELETE":
		d.handlerDelete(rw, r)
	default:
		rw.WriteHeader(http.StatusInternalServerError)
	}
}

func (d DbExplorer) handlerGet(rw http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		responseResult(rw, nil, http.StatusOK, map[string]interface{}{"tables": d.tableKeys})
		return
	}

	tableName, err := getTableName(r.URL.Path, d.tableKeys)
	if err != nil {
		responseResult(rw, err, http.StatusNotFound, nil)
		return
	}
	pathParts := strings.Split(r.URL.Path, "/")
	switch len(pathParts) {

	case 2:
		limit, err := strconv.Atoi(r.FormValue("limit"))
		if err != nil {
			limit = 5
		}

		offset, err := strconv.Atoi(r.FormValue("offset"))
		if err != nil {
			offset = 0
		}

		query := "SELECT * FROM " + tableName + " LIMIT ?,?;"
		queryResult, err := d.db.Query(query, offset, limit)
		if err != nil {
			responseResult(rw, err, http.StatusNotFound, nil)
			return
		}

		records, err := parsingSqlQueryResult(queryResult)
		if err != nil {
			responseResult(rw, err, http.StatusNotFound, nil)
			return
		}

		responseResult(rw, nil, http.StatusOK, map[string]interface{}{"records": records})

	case 3:
		id, err := strconv.Atoi(pathParts[2])
		if err != nil {
			responseResult(rw, errors.New("record not found"), http.StatusNotFound, nil)
			return
		}

		idColumnName := d.tableIdNameMap[tableName]
		query := "SELECT * FROM " + tableName + " WHERE " + idColumnName + " = ?;"
		queryResult, err := d.db.Query(query, id)
		if err != nil {
			responseResult(rw, err, http.StatusNotFound, nil)
			return
		}

		records, err := parsingSqlQueryResult(queryResult)
		if err != nil {
			responseResult(rw, err, http.StatusNotFound, nil)
			return
		}

		responseResult(
			rw,
			nil,
			http.StatusOK,
			map[string]interface{}{"record": records[0]},
		)

	default:
		responseResult(rw, errors.New("not found"), http.StatusNotFound, nil)
		return
	}
}

func (d DbExplorer) handlerPut(rw http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) != 3 {
		responseResult(rw, errors.New("unknown table"), http.StatusNotFound, nil)
		return
	}

	tableName, err := getTableName(r.URL.Path, d.tableKeys)
	if err != nil {
		responseResult(rw, errors.New("unknown table"), http.StatusNotFound, nil)
		return
	}

	requestDataMap, err := getDataForSqlQuery(r.Body, d, tableName)
	if err != nil {
		responseResult(rw, err, http.StatusBadRequest, nil)
		return
	}

	idColumnName := d.tableIdNameMap[tableName]
	lastInsertId, err := d.insertRecord(requestDataMap, tableName)
	result := map[string]int{idColumnName: lastInsertId}
	responseResult(rw, err, http.StatusOK, result)
}

func (d DbExplorer) insertRecord(dataMap map[string]interface{}, tableName string) (lastInsertId int, err error) {
	columName := ""
	columValue := make([]interface{}, 0)

	for key, rd := range d.columnsInTablesMap[tableName] {
		if d.columnsInTablesMap[tableName][key].primary {
			continue
		}

		val, ok := dataMap[key]
		if !ok {
			if rd.isNull {
				continue
			}
			val = rd.defaultValue
		}

		if columName != "" {
			columName += ", "
		}

		columValue = append(columValue, val)
		columName = fmt.Sprintf("%v`%v`", columName, key)
	}

	query := fmt.Sprintf("INSERT INTO %v (%v) VALUES(?"+strings.Repeat(",?", len(columValue)-1)+");", tableName, columName)
	queryResult, err := d.db.Exec(query, columValue...)
	if err != nil {
		return 0, err
	}

	id, err := queryResult.LastInsertId()
	lastInsertId = int(id)
	return lastInsertId, err
}

func (d DbExplorer) handlerPost(rw http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) != 3 {
		responseResult(rw, errors.New("unknown table"), http.StatusNotFound, nil)
		return
	}

	tableName, err := getTableName(r.URL.Path, d.tableKeys)
	if err != nil {
		responseResult(rw, errors.New("unknown table"), http.StatusNotFound, nil)
		return
	}

	id, err := strconv.Atoi(pathParts[2])
	if err != nil {
		responseResult(rw, err, http.StatusBadRequest, nil)
		return
	}

	requestData, err := getDataForSqlQuery(r.Body, d, tableName)
	if err != nil {
		responseResult(rw, err, http.StatusBadRequest, nil)
		return
	}

	affectedCount, err := d.updateRecord(requestData, tableName, id)
	if err != nil {
		responseResult(rw, err, http.StatusBadRequest, nil)
		return
	}

	result := map[string]int{"updated": affectedCount}
	responseResult(rw, nil, http.StatusOK, result)
}

func (d DbExplorer) updateRecord(data map[string]interface{}, tableName string, id int) (int, error) {
	idKey := ""
	for key, val := range d.columnsInTablesMap[tableName] {
		if val.primary {
			idKey = key
			break
		}
	}

	if _, ok := data[idKey]; ok {
		return 0, errors.New("field " + idKey + " have invalid type")
	}

	query := ""
	for key, rd := range data {
		if query != "" {
			query += ", "
		}

		switch d.columnsInTablesMap[tableName][key].typeName {
		case "string":
			if rd == nil {
				query = fmt.Sprintf("%v`%v`= NULL", query, key)
				continue
			}
			query = fmt.Sprintf("%v`%v`='%v'", query, key, rd)
		case "int":
			query = fmt.Sprintf("%v`%v`=%v", query, key, rd)
		default:
			continue
		}

	}

	query = fmt.Sprintf("UPDATE `%v` SET %v WHERE `%v` = ?;", tableName, query, idKey)

	queryResult, err := d.db.Exec(query, id)
	if err != nil {
		return 0, err
	}

	affectedCount, err := queryResult.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(affectedCount), nil
}

func (d DbExplorer) handlerDelete(rw http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) != 3 {
		responseResult(rw, errors.New("unknown table"), http.StatusNotFound, nil)
		return
	}

	tableName, err := getTableName(r.URL.Path, d.tableKeys)
	if err != nil {
		responseResult(rw, errors.New("unknown table"), http.StatusNotFound, nil)
		return
	}

	id, err := strconv.Atoi(pathParts[2])
	if err != nil {
		responseResult(rw, err, http.StatusBadRequest, nil)
		return
	}

	idColumnName := d.tableIdNameMap[tableName]
	query := fmt.Sprintf("DELETE FROM `%v` WHERE %v = ?", tableName, idColumnName)
	queryResult, err := d.db.Exec(query, id)
	if err != nil {
		responseResult(rw, err, http.StatusBadRequest, nil)
		return
	}

	count, err := queryResult.RowsAffected()
	rowsAffected := int(count)
	if err != nil {
		responseResult(rw, err, http.StatusBadRequest, nil)
		return
	}

	result := map[string]int{"deleted": rowsAffected}
	responseResult(rw, err, http.StatusOK, result)
	return
}

//ФУНКЦИИ-ХЕЛПЕРЫ
func getDataForSqlQuery(r io.Reader, d DbExplorer, tableName string) (map[string]interface{}, error) {
	buffer, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	requestDataMap := make(map[string]interface{})
	if err := json.Unmarshal(buffer, &requestDataMap); err != nil {
		return nil, err
	}

	for columnName, column := range d.columnsInTablesMap[tableName] {
		data, ok := requestDataMap[columnName]
		if !ok {
			continue
		}

		switch column.typeName {
		case "int":
			val, ok := data.(float64)
			if !ok {
				return nil, errors.New("field " + column.name + " have invalid type")
			}
			requestDataMap[columnName] = int(val)

		case "string":
			if data == nil {
				if !d.columnsInTablesMap[tableName][columnName].isNull {
					return nil, errors.New("field " + column.name + " have invalid type")
				}
				requestDataMap[columnName] = nil
				continue
			}

			val, ok := data.(string)
			if !ok {
				return nil, errors.New("field " + column.name + " have invalid type")
			}
			requestDataMap[columnName] = val

		default:
			delete(requestDataMap, columnName)
		}
	}

	return requestDataMap, nil
}

func getTableName(url string, tableKeys []string) (string, error) {
	pathParts := strings.Split(url, "/")
	if len(pathParts) < 2 {
		return "", errors.New("unknown table")
	}
	for _, tableName := range tableKeys {
		if tableName == pathParts[1] {
			return tableName, nil
		}
	}
	return "", errors.New("unknown table")
}

func parsingSqlQueryResult(queryResult *sql.Rows) ([]map[string]interface{}, error) {
	result := make([]map[string]interface{}, 0)
	columns, err := queryResult.ColumnTypes()
	if err != nil {
		return nil, err
	}

	for queryResult.Next() {
		values := make([]interface{}, len(columns))
		valuePointers := make([]interface{}, len(columns))
		for i := range columns {
			valuePointers[i] = &values[i]
		}

		if err := queryResult.Scan(valuePointers...); err != nil {
			continue
		}

		record := make(map[string]interface{}, len(columns))
		for i, columnType := range columns {
			var value interface{}

			expectedValue := values[i]
			bytes, ok := expectedValue.([]byte)
			if ok {
				stringValue := string(bytes)
				if columnType.DatabaseTypeName() == "INT" {
					record[columnType.Name()], _ = strconv.Atoi(stringValue)
					continue
				}
				value = stringValue
			} else {
				value = expectedValue
			}

			record[columnType.Name()] = value
		}

		result = append(result, record)
	}

	if len(result) == 0 {
		return nil, errors.New("record not found")
	}
	return result, nil
}

func responseResult(rw http.ResponseWriter, err error, httpStatusCode int, result interface{}) {
	type CR map[string]interface{}
	responseMap := CR{}
	textErr := ""

	if err != nil {
		textErr = err.Error()
		responseMap["error"] = textErr
		rw.WriteHeader(httpStatusCode)
	}
	if result != nil {
		responseMap["response"] = result
	}

	response, err := json.Marshal(responseMap)
	if err != nil {
		fmt.Println(err)
	}

	if _, err := rw.Write(response); err != nil {
		fmt.Println(err)
	}
}
