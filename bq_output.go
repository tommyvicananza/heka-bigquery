// Copyright 2015 Boa Ho Man. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hbq

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/tommyvicananza/heka-bigquery/bq"
	bigquery "google.golang.org/api/bigquery/v2"

	. "github.com/mozilla-services/heka/pipeline"
)

// Interval to tick
const IntervalPeriod time.Duration = 24 * time.Hour

// Hour for 1st tick
const TickHour int = 00

// Minute for 1st tick
const TickMinute int = 00

// Second for 1st tick
const TickSecond int = 00

// Max buffer size before it attempts an upload in bytes, currently 1000 for testing.
const MaxBuffer = 1000

// A BqOutputConfig holds the information needed to configure the heka plugin.
// Service Email: Service account email found in Google Developers console
// Pem File: PKCS12 file that is generated from the .p12 file
// Schema file: BigQuery schema json file. See example schema file `realtime_log.schema.sample`
// BufferPath + BufferFile: Full path to the 'backup' file that is written to at the same time as the buffer
type BqOutputConfig struct {
	ProjectId      string `toml:"project_id"`
	DatasetId      string `toml:"dataset_id"`
	TableId        string `toml:"table_id"`
	ServiceEmail   string `toml:"service_email"`
	PemFilePath    string `toml:"pem_file_path"`
	SchemaFilePath string `toml:"schema_file_path"`
	BufferPath     string `toml:"buffer_path"`
	BufferFile     string `toml:"buffer_file"`
}

// A BqOutput holds the uploader/schema.
type BqOutput struct {
	schema []byte
	config *BqOutputConfig
	bu     *bq.BqUploader
}

// CPU to insert in BigQuery
type CPU struct {
	Nice     int    `json:"nice"`
	System   int    `json:"system"`
	Idle     int    `json:"idle"`
	User     int    `json:"user"`
	Time     string `json:"time"`
	Hostname string `json:"hostname"`
	Id       string `json:"id"`
}

// CPUOrig from payload
type CPUOrig struct {
	Cpus     map[string]Cpus `json:"cpu"`
	Time     string          `json:"time"`
	Hostname string          `json:"hostname"`
}

func (bqo *BqOutput) ConfigStruct() interface{} {
	return &BqOutputConfig{}
}

// Init function that gets run by Heka when the plugin gets loaded
// Reads PEM files/schema files and initializes the BqUploader objects
func (bqo *BqOutput) Init(config interface{}) (err error) {
	bqo.config = config.(*BqOutputConfig)

	pkey, _ := ioutil.ReadFile(bqo.config.PemFilePath)
	schema, _ := ioutil.ReadFile(bqo.config.SchemaFilePath)

	bu := bq.NewBqUploader(pkey, bqo.config.ProjectId, bqo.config.DatasetId, bqo.config.ServiceEmail)

	bqo.schema = schema
	bqo.bu = bu
	return
}

func existsInSlice(tableName string, tables []string) bool {
	for _, n := range tables {
		if n == tableName {
			return true
		}
	}
	return false
}

type pay struct {
	ContainerName string `json:"container_name"`
	Hostname      string `json:"hostname"`
	// { "container_id":"foo",  "container_name":"foo", "hostname":"foo", "time":"foo", "output":"foo", "logger_type":"foo"}
}

// Gets called by Heka when the plugin is running.
// For more information, visit https://hekad.readthedocs.org/en/latest/developing/plugin.html
func (bqo *BqOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	var (
		// Heka messages
		pack    *PipelinePack
		payload []byte

		fullPath string

		files   map[string]*os.File
		buffers map[string]*bytes.Buffer
		tables  []string

		ok = true
	)

	files = make(map[string]*os.File)
	buffers = make(map[string]*bytes.Buffer)
	fileOp := os.O_CREATE | os.O_APPEND | os.O_WRONLY

	// Channel that delivers the heka payloads
	inChan := or.InChan()

	// Ensures that the directories are there before saving
	mkDirectories(bqo.config.BufferPath)

	encoder := or.Encoder()
	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				break
			}
			if encoder != nil {
				payload, err = or.Encode(pack)
				if err != nil {
					or.LogError(err)
					pack.Recycle(err)
					continue
				} else {
					pack.Recycle(nil)
				}
			} else {
				payload = []byte(pack.Message.GetPayload())
				pack.Recycle(nil)
			}

			var p pay
			err = json.Unmarshal(payload, &p)
			if err != nil {
				logError(or, "Reading payload ", err)
				continue
			}
			mapa := make(map[string]interface{})
			err = json.Unmarshal(payload, &mapa)
			if _, ok := mapa["numproc"]; ok {
				p.ContainerName = fmt.Sprintf("loadavg_%s", p.Hostname)
			}
			if _, ok := mapa["memtotal"]; ok {
				p.ContainerName = fmt.Sprintf("mem_%s", p.Hostname)
			}
			if _, ok := mapa["cpu"]; ok {
				p.ContainerName = fmt.Sprintf("cpu_%s", p.Hostname)
			}
			if p.ContainerName == "" {
				p.ContainerName = fmt.Sprintf("syslog_%s", p.Hostname)
			}

			fullPath = fmt.Sprintf("%s/%s", bqo.config.BufferPath, p.ContainerName)

			if e := existsInSlice(p.ContainerName, tables); e == false {
				tables = append(tables, p.ContainerName)
				// Buffer that is used to store logs before uploading to bigquery
				buffers[p.ContainerName] = bytes.NewBuffer(nil)
				//fullPath = fmt.Sprintf("%s/%s", bqo.config.BufferPath, p.ContainerName)
				files[p.ContainerName], err = os.OpenFile(fullPath, fileOp, 0666)
				if err != nil {
					logError(or, "Creating file", err)
				}
				if err = bqo.bu.CreateTable(p.ContainerName, bqo.schema); err != nil {
					logError(or, "Initialize Table", err)
				}
			}

			// Write to both file and buffer
			if p.ContainerName != "cpu" {
				if _, err = files[p.ContainerName].Write(payload); err != nil {
					logError(or, "Write to File", err)
				}
				if _, err = buffers[p.ContainerName].Write(payload); err != nil {
					logError(or, "Write to Buffer", err)
				}
			} else {
				var message CPUOrig
				err := json.Unmarshal(payload, &message)
				if err != nil {
					logError(or, "Reading payload ", err)
					continue
				}
				for key, v := range message.Cpus {
					v.Id = key
					v.Hostname = cpu.Hostname
					v.Time = cpu.Hostname
					c, _ := json.Marshal(v)
					if _, err = files[p.ContainerName].Write(c); err != nil {
						logError(or, "Write to File", err)
					}
					if _, err = buffers[p.ContainerName].Write(c); err != nil {
						logError(or, "Write to Buffer", err)
					}
				}
			}

			// Upload Stuff (1mb)
			if buffers[p.ContainerName].Len() > MaxBuffer {
				files[p.ContainerName].Close() // Close file for uploading
				bqo.UploadAndReset(buffers[p.ContainerName], fullPath, p.ContainerName, or)
				files[p.ContainerName], err = os.OpenFile(fullPath, fileOp, 0666)
				if err != nil {
					logError(or, "Creating file", err)
				}
			}
		}
	}

	logUpdate(or, "Shutting down BQ output runner.")
	return
}

// Prepares data and uploads them to the BigQuery Table.
// Shared by both file/buffer uploads
func (bqo *BqOutput) Upload(i interface{}, tableName string) (err error) {
	var data []byte
	list := make([]map[string]bigquery.JsonValue, 0)

	for {
		data, _ = readData(i)
		if len(data) == 0 {
			break
		}
		list = append(list, bq.BytesToBqJsonRow(data))
	}
	return bqo.bu.InsertRows(tableName, list)
}

func readData(i interface{}) (line []byte, err error) {
	switch v := i.(type) {
	case *bytes.Buffer:
		line, err = v.ReadBytes('\n')
	case *bufio.Reader:
		line, err = v.ReadBytes('\n')
	}
	return
}

// Uploads buffer, and if it fails/contains errors, falls back to using the file to upload.
// After which clears the buffer and deletes the backup file
func (bqo *BqOutput) UploadAndReset(buf *bytes.Buffer, path string, tn string, or OutputRunner) {

	logUpdate(or, "Buffer limit reached, uploading "+tn)

	if err := bqo.Upload(buf, tn); err != nil {
		logError(or, "Upload Buffer", err)
		if err := bqo.UploadFile(path, tn); err != nil {
			logError(or, "Upload File", err)
		} else {
			logUpdate(or, "Upload File Successful")
		}
	} else {
		logUpdate(or, "Upload Buffer Successful")
	}

	// Cleanup and Reset
	buf.Reset()
	_ = os.Remove(path)
}

// Uploads file at `path` to BigQuery table
func (bqo *BqOutput) UploadFile(path string, tableName string) (err error) {
	f, _ := os.Open(path)
	fr := bufio.NewReader(f)
	err = bqo.Upload(fr, tableName)
	f.Close()
	return
}

func formatDate(t time.Time) string {
	return fmt.Sprintf(t.Format("20060102"))
}

func logUpdate(or OutputRunner, title string) {
	or.LogMessage(title)
}

func logError(or OutputRunner, title string, err error) {
	or.LogMessage(fmt.Sprintf("%s - Error -: %s", title, err))
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func mkDirectories(path string) {
	if ok, _ := exists(path); !ok {
		_ = os.MkdirAll(path, 0666)
	}
}

func midnightTickerUpdate() *time.Ticker {
	nextTick := time.Date(time.Now().Year(), time.Now().Month(),
		time.Now().Day(), TickHour, TickMinute, TickSecond,
		0, time.Local)
	if !nextTick.After(time.Now()) {
		nextTick = nextTick.Add(IntervalPeriod)
	}
	diff := nextTick.Sub(time.Now())
	return time.NewTicker(diff)
}

func (bqo *BqOutput) tableName(d time.Time) string {
	return bqo.config.TableId + formatDate(d)
}

func init() {
	RegisterPlugin("BqOutput", func() interface{} {
		return new(BqOutput)
	})
}
