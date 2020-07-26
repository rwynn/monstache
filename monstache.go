// package main provides the monstache binary
package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"plugin"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/coreos/go-systemd/daemon"
	jsonpatch "github.com/evanphx/json-patch"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/olivere/elastic"
	aws "github.com/olivere/elastic/aws/v4"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
	"github.com/rwynn/gtm"
	"github.com/rwynn/gtm/consistent"
	"github.com/rwynn/monstache/monstachemap"
	"golang.org/x/net/context"
	"gopkg.in/Graylog2/go-gelf.v2/gelf"
	"gopkg.in/natefinch/lumberjack.v2"
)

var infoLog = log.New(os.Stdout, "INFO ", log.Flags())
var warnLog = log.New(os.Stdout, "WARN ", log.Flags())
var statsLog = log.New(os.Stdout, "STATS ", log.Flags())
var traceLog = log.New(os.Stdout, "TRACE ", log.Flags())
var errorLog = log.New(os.Stderr, "ERROR ", log.Flags())

var mapperPlugin func(*monstachemap.MapperPluginInput) (*monstachemap.MapperPluginOutput, error)
var filterPlugin func(*monstachemap.MapperPluginInput) (bool, error)
var processPlugin func(*monstachemap.ProcessPluginInput) error
var pipePlugin func(string, bool) ([]interface{}, error)
var mapEnvs = make(map[string]*executionEnv)
var filterEnvs = make(map[string]*executionEnv)
var pipeEnvs = make(map[string]*executionEnv)
var mapIndexTypes = make(map[string]*indexTypeMapping)
var relates = make(map[string][]*relation)
var fileNamespaces = make(map[string]bool)
var patchNamespaces = make(map[string]bool)
var tmNamespaces = make(map[string]bool)
var routingNamespaces = make(map[string]bool)
var mux sync.Mutex

var chunksRegex = regexp.MustCompile("\\.chunks$")
var systemsRegex = regexp.MustCompile("system\\..+$")
var exitStatus = 0
var mongoDialInfo *mgo.DialInfo
var statusReqC = make(chan *statusRequest)

const version = "4.19.6"
const mongoURLDefault string = "localhost"
const resumeNameDefault string = "default"
const elasticMaxConnsDefault int = 4
const elasticClientTimeoutDefault int = 0
const elasticMaxDocsDefault int = -1
const elasticMaxBytesDefault int = 8 * 1024 * 1024
const gtmChannelSizeDefault int = 512
const typeFromFuture string = "_doc"
const fileDownloadersDefault = 10
const relateThreadsDefault = 10
const relateBufferDefault = 1000
const postProcessorsDefault = 10
const redact = "REDACTED"
const configDatabaseNameDefault = "monstache"
const relateQueueOverloadMsg = "Relate queue is full. Skipping relate for %v.(%v) to keep pipeline healthy."

type deleteStrategy int

const (
	statelessDeleteStrategy deleteStrategy = iota
	statefulDeleteStrategy
	ignoreDeleteStrategy
)

type stringargs []string

type awsConnect struct {
	AccessKey string `toml:"access-key"`
	SecretKey string `toml:"secret-key"`
	Region    string
}

type executionEnv struct {
	VM     *otto.Otto
	Script string
	lock   *sync.Mutex
}

type javascript struct {
	Namespace string
	Script    string
	Path      string
	Routing   bool
}

type relation struct {
	Namespace     string
	WithNamespace string `toml:"with-namespace"`
	SrcField      string `toml:"src-field"`
	MatchField    string `toml:"match-field"`
	KeepSrc       bool   `toml:"keep-src"`
	DotNotation   bool   `toml:"dot-notation"`
	MaxDepth      int    `toml:"max-depth"`
	db            string
	col           string
}

type indexTypeMapping struct {
	Namespace string
	Index     string
	Type      string
}

type findConf struct {
	vm            *otto.Otto
	ns            string
	name          string
	session       *mgo.Session
	byId          bool
	multi         bool
	pipe          bool
	pipeAllowDisk bool
}

type findCall struct {
	config  *findConf
	session *mgo.Session
	query   interface{}
	db      string
	col     string
	limit   int
	sort    []string
	sel     map[string]int
}

type logFiles struct {
	Info  string
	Warn  string
	Error string
	Trace string
	Stats string
}

type indexingMeta struct {
	Routing         string
	Index           string
	Type            string
	Parent          string
	Version         int64
	VersionType     string
	Pipeline        string
	RetryOnConflict int
	Skip            bool
	ID              string
}

type outputChans struct {
	indexC   chan *gtm.Op
	processC chan *gtm.Op
	fileC    chan *gtm.Op
	relateC  chan *gtm.Op
	filter   gtm.OpFilter
}

type mongoDialSettings struct {
	Timeout      int
	Ssl          bool
	ReadTimeout  int `toml:"read-timeout"`
	WriteTimeout int `toml:"write-timeout"`
}

type mongoSessionSettings struct {
	SocketTimeout int `toml:"socket-timeout"`
	SyncTimeout   int `toml:"sync-timeout"`
}

type mongoX509Settings struct {
	ClientCertPemFile string `toml:"client-cert-pem-file"`
	ClientKeyPemFile  string `toml:"client-key-pem-file"`
}

type gtmSettings struct {
	ChannelSize    int    `toml:"channel-size"`
	BufferSize     int    `toml:"buffer-size"`
	BufferDuration string `toml:"buffer-duration"`
}

type httpServerCtx struct {
	httpServer *http.Server
	bulk       *elastic.BulkProcessor
	config     *configOptions
	shutdown   bool
	started    time.Time
}

type instanceStatus struct {
	Enabled      bool                `json:"enabled"`
	Pid          int                 `json:"pid"`
	Hostname     string              `json:"hostname"`
	ClusterName  string              `json:"cluster"`
	ResumeName   string              `json:"resumeName"`
	LastTs       bson.MongoTimestamp `json:"lastTs"`
	LastTsFormat string              `json:"lastTsFormat,omitempty"`
}

type statusResponse struct {
	enabled bool
	lastTs  bson.MongoTimestamp
}

type statusRequest struct {
	responseC chan *statusResponse
}

type configOptions struct {
	EnableTemplate           bool
	EnvDelimiter             string
	MongoURL                 string               `toml:"mongo-url"`
	MongoConfigURL           string               `toml:"mongo-config-url"`
	MongoPemFile             string               `toml:"mongo-pem-file"`
	MongoValidatePemFile     bool                 `toml:"mongo-validate-pem-file"`
	MongoOpLogDatabaseName   string               `toml:"mongo-oplog-database-name"`
	MongoOpLogCollectionName string               `toml:"mongo-oplog-collection-name"`
	MongoDialSettings        mongoDialSettings    `toml:"mongo-dial-settings"`
	MongoSessionSettings     mongoSessionSettings `toml:"mongo-session-settings"`
	MongoX509Settings        mongoX509Settings    `toml:"mongo-x509-settings"`
	GtmSettings              gtmSettings          `toml:"gtm-settings"`
	AWSConnect               awsConnect           `toml:"aws-connect"`
	Logs                     logFiles             `toml:"logs"`
	GraylogAddr              string               `toml:"graylog-addr"`
	ElasticUrls              stringargs           `toml:"elasticsearch-urls"`
	ElasticUser              string               `toml:"elasticsearch-user"`
	ElasticPassword          string               `toml:"elasticsearch-password"`
	ElasticPemFile           string               `toml:"elasticsearch-pem-file"`
	ElasticValidatePemFile   bool                 `toml:"elasticsearch-validate-pem-file"`
	ElasticVersion           string               `toml:"elasticsearch-version"`
	ElasticHealth0           int                  `toml:"elasticsearch-healthcheck-timeout-startup"`
	ElasticHealth1           int                  `toml:"elasticsearch-healthcheck-timeout"`
	ResumeName               string               `toml:"resume-name"`
	NsRegex                  string               `toml:"namespace-regex"`
	NsDropRegex              string               `toml:"namespace-drop-regex"`
	NsExcludeRegex           string               `toml:"namespace-exclude-regex"`
	NsDropExcludeRegex       string               `toml:"namespace-drop-exclude-regex"`
	ClusterName              string               `toml:"cluster-name"`
	Print                    bool                 `toml:"print-config"`
	Version                  bool
	Pprof                    bool
	DisableChangeEvents      bool `toml:"disable-change-events"`
	EnableEasyJSON           bool `toml:"enable-easy-json"`
	Stats                    bool
	IndexStats               bool   `toml:"index-stats"`
	StatsDuration            string `toml:"stats-duration"`
	StatsIndexFormat         string `toml:"stats-index-format"`
	Gzip                     bool
	Verbose                  bool
	Resume                   bool
	ResumeWriteUnsafe        bool  `toml:"resume-write-unsafe"`
	ResumeFromTimestamp      int64 `toml:"resume-from-timestamp"`
	Replay                   bool
	DroppedDatabases         bool   `toml:"dropped-databases"`
	DroppedCollections       bool   `toml:"dropped-collections"`
	IndexFiles               bool   `toml:"index-files"`
	IndexAsUpdate            bool   `toml:"index-as-update"`
	FileHighlighting         bool   `toml:"file-highlighting"`
	EnablePatches            bool   `toml:"enable-patches"`
	FailFast                 bool   `toml:"fail-fast"`
	IndexOplogTime           bool   `toml:"index-oplog-time"`
	OplogTsFieldName         string `toml:"oplog-ts-field-name"`
	OplogDateFieldName       string `toml:"oplog-date-field-name"`
	OplogDateFieldFormat     string `toml:"oplog-date-field-format"`
	ExitAfterDirectReads     bool   `toml:"exit-after-direct-reads"`
	MergePatchAttr           string `toml:"merge-patch-attribute"`
	ElasticMaxConns          int    `toml:"elasticsearch-max-conns"`
	ElasticRetry             bool   `toml:"elasticsearch-retry"`
	ElasticMaxDocs           int    `toml:"elasticsearch-max-docs"`
	ElasticMaxBytes          int    `toml:"elasticsearch-max-bytes"`
	ElasticMaxSeconds        int    `toml:"elasticsearch-max-seconds"`
	ElasticClientTimeout     int    `toml:"elasticsearch-client-timeout"`
	ElasticMajorVersion      int
	ElasticMinorVersion      int
	MaxFileSize              int64 `toml:"max-file-size"`
	ConfigFile               string
	Script                   []javascript
	Filter                   []javascript
	Pipeline                 []javascript
	Mapping                  []indexTypeMapping
	Relate                   []relation
	FileNamespaces           stringargs `toml:"file-namespaces"`
	PatchNamespaces          stringargs `toml:"patch-namespaces"`
	Workers                  stringargs
	Worker                   string
	ChangeStreamNs           stringargs     `toml:"change-stream-namespaces"`
	DirectReadNs             stringargs     `toml:"direct-read-namespaces"`
	DirectReadSplitMax       int            `toml:"direct-read-split-max"`
	DirectReadConcur         int            `toml:"direct-read-concur"`
	DirectReadNoTimeout      bool           `toml:"direct-read-no-timeout"`
	MapperPluginPath         string         `toml:"mapper-plugin-path"`
	EnableHTTPServer         bool           `toml:"enable-http-server"`
	HTTPServerAddr           string         `toml:"http-server-addr"`
	TimeMachineNamespaces    stringargs     `toml:"time-machine-namespaces"`
	TimeMachineIndexPrefix   string         `toml:"time-machine-index-prefix"`
	TimeMachineIndexSuffix   string         `toml:"time-machine-index-suffix"`
	TimeMachineDirectReads   bool           `toml:"time-machine-direct-reads"`
	PipeAllowDisk            bool           `toml:"pipe-allow-disk"`
	RoutingNamespaces        stringargs     `toml:"routing-namespaces"`
	DeleteStrategy           deleteStrategy `toml:"delete-strategy"`
	DeleteIndexPattern       string         `toml:"delete-index-pattern"`
	ConfigDatabaseName       string         `toml:"config-database-name"`
	FileDownloaders          int            `toml:"file-downloaders"`
	RelateThreads            int            `toml:"relate-threads"`
	RelateBuffer             int            `toml:"relate-buffer"`
	PostProcessors           int            `toml:"post-processors"`
	PruneInvalidJSON         bool           `toml:"prune-invalid-json"`
	Debug                    bool
}

func (rel *relation) IsIdentity() bool {
	if rel.SrcField == "_id" && rel.MatchField == "_id" {
		return true
	} else {
		return false
	}
}

func (l *logFiles) enabled() bool {
	return l.Info != "" || l.Warn != "" || l.Error != "" || l.Trace != "" || l.Stats != ""
}

func (s *mongoX509Settings) enabled() bool {
	return s.ClientCertPemFile != "" || s.ClientKeyPemFile != ""
}

func (s *mongoX509Settings) validate() error {
	if s.ClientCertPemFile != "" || s.ClientKeyPemFile != "" {
		if s.ClientCertPemFile == "" {
			return errors.New("Client cert pem file missing for X509 authentication")
		} else if s.ClientKeyPemFile == "" {
			return errors.New("Client key pem file missing for X509 authentication")
		}
	}
	return nil
}

func (ac *awsConnect) validate() error {
	if ac.AccessKey == "" && ac.SecretKey == "" {
		return nil
	} else if ac.AccessKey != "" && ac.SecretKey != "" {
		return nil
	}
	return errors.New("AWS connect settings must include both access-key and secret-key")
}

func (ac *awsConnect) enabled() bool {
	return ac.AccessKey != "" || ac.SecretKey != ""
}

func (arg *deleteStrategy) String() string {
	return fmt.Sprintf("%d", *arg)
}

func (arg *deleteStrategy) Set(value string) (err error) {
	var i int
	if i, err = strconv.Atoi(value); err != nil {
		return
	}
	ds := deleteStrategy(i)
	*arg = ds
	return
}

func (args *stringargs) String() string {
	return fmt.Sprintf("%s", *args)
}

func (args *stringargs) Set(value string) error {
	*args = append(*args, value)
	return nil
}

func (config *configOptions) readShards() bool {
	return len(config.ChangeStreamNs) == 0 && config.MongoConfigURL != ""
}

func afterBulk(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if response != nil && response.Errors {
		failed := response.Failed()
		if failed != nil {
			for _, item := range failed {
				if item.Status == 409 {
					// ignore version conflict since this simply means the doc
					// is already in the index
					continue
				}
				json, err := json.Marshal(item)
				if err != nil {
					errorLog.Printf("Unable to marshal bulk response item: %s", err)
				} else {
					errorLog.Printf("Bulk response item: %s", string(json))
				}
			}
		}
	}
}

func (config *configOptions) useTypeFromFuture() (use bool) {
	if config.ElasticMajorVersion > 6 {
		use = true
	} else if config.ElasticMajorVersion == 6 && config.ElasticMinorVersion >= 2 {
		use = true
	}
	return
}

func (config *configOptions) parseElasticsearchVersion(number string) (err error) {
	if number == "" {
		err = errors.New("Elasticsearch version cannot be blank")
	} else {
		versionParts := strings.Split(number, ".")
		var majorVersion, minorVersion int
		majorVersion, err = strconv.Atoi(versionParts[0])
		if err == nil {
			config.ElasticMajorVersion = majorVersion
			if majorVersion == 0 {
				err = errors.New("Invalid Elasticsearch major version 0")
			}
		}
		if len(versionParts) > 1 {
			minorVersion, err = strconv.Atoi(versionParts[1])
			if err == nil {
				config.ElasticMinorVersion = minorVersion
			}
		}
	}
	return
}

func (config *configOptions) newBulkProcessor(client *elastic.Client) (bulk *elastic.BulkProcessor, err error) {
	bulkService := client.BulkProcessor().Name("monstache")
	bulkService.Workers(config.ElasticMaxConns)
	bulkService.Stats(config.Stats)
	bulkService.BulkActions(config.ElasticMaxDocs)
	bulkService.BulkSize(config.ElasticMaxBytes)
	if config.ElasticRetry == false {
		bulkService.Backoff(&elastic.StopBackoff{})
	}
	bulkService.After(afterBulk)
	bulkService.FlushInterval(time.Duration(config.ElasticMaxSeconds) * time.Second)
	return bulkService.Do(context.Background())
}

func (config *configOptions) newStatsBulkProcessor(client *elastic.Client) (bulk *elastic.BulkProcessor, err error) {
	bulkService := client.BulkProcessor().Name("monstache-stats")
	bulkService.Workers(1)
	bulkService.Stats(false)
	bulkService.BulkActions(-1)
	bulkService.BulkSize(-1)
	bulkService.After(afterBulk)
	bulkService.FlushInterval(time.Duration(5) * time.Second)
	return bulkService.Do(context.Background())
}

func (config *configOptions) needsSecureScheme() bool {
	if len(config.ElasticUrls) > 0 {
		for _, url := range config.ElasticUrls {
			if strings.HasPrefix(url, "https") {
				return true
			}
		}
	}
	return false

}

func (config *configOptions) newElasticClient() (client *elastic.Client, err error) {
	var clientOptions []elastic.ClientOptionFunc
	var httpClient *http.Client
	clientOptions = append(clientOptions, elastic.SetSniff(false))
	if config.needsSecureScheme() {
		clientOptions = append(clientOptions, elastic.SetScheme("https"))
	}
	if len(config.ElasticUrls) > 0 {
		clientOptions = append(clientOptions, elastic.SetURL(config.ElasticUrls...))
	} else {
		config.ElasticUrls = append(config.ElasticUrls, elastic.DefaultURL)
	}
	if config.Verbose {
		clientOptions = append(clientOptions, elastic.SetTraceLog(traceLog))
		clientOptions = append(clientOptions, elastic.SetErrorLog(errorLog))
	}
	if config.ElasticUser != "" {
		clientOptions = append(clientOptions, elastic.SetBasicAuth(config.ElasticUser, config.ElasticPassword))
	}
	if config.ElasticRetry {
		d1, d2 := time.Duration(50)*time.Millisecond, time.Duration(20)*time.Second
		retrier := elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(d1, d2))
		clientOptions = append(clientOptions, elastic.SetRetrier(retrier))
	}
	httpClient, err = config.NewHTTPClient()
	if err != nil {
		return client, err
	}
	clientOptions = append(clientOptions, elastic.SetHttpClient(httpClient))
	clientOptions = append(clientOptions,
		elastic.SetHealthcheckTimeoutStartup(time.Duration(config.ElasticHealth0)*time.Second))
	clientOptions = append(clientOptions,
		elastic.SetHealthcheckTimeout(time.Duration(config.ElasticHealth1)*time.Second))
	return elastic.NewClient(clientOptions...)
}

func (config *configOptions) testElasticsearchConn(client *elastic.Client) (err error) {
	var number string
	url := config.ElasticUrls[0]
	number, err = client.ElasticsearchVersion(url)
	if err == nil {
		infoLog.Printf("Successfully connected to Elasticsearch version %s", number)
		err = config.parseElasticsearchVersion(number)
	}
	return
}

func deleteIndexes(client *elastic.Client, db string, config *configOptions) (err error) {
	var indices = []string{strings.ToLower(db + ".*")}
	for ns, m := range mapIndexTypes {
		dbCol := strings.SplitN(ns, ".", 2)
		if dbCol[0] == db && m.Index != "" {
			index := strings.ToLower(m.Index)
			for _, cur := range indices {
				if cur == index {
					index = ""
					break
				}
			}
			if index != "" {
				indices = append(indices, index)
			}
		}
	}
	_, err = client.DeleteIndex(indices...).Do(context.Background())
	return
}

func deleteIndex(client *elastic.Client, namespace string, config *configOptions) (err error) {
	ctx := context.Background()
	index := strings.ToLower(namespace)
	if m := mapIndexTypes[namespace]; m != nil {
		if m.Index != "" {
			index = strings.ToLower(m.Index)
		}
	}
	_, err = client.DeleteIndex(index).Do(ctx)
	return err
}

func ensureFileMapping(client *elastic.Client) (err error) {
	ctx := context.Background()
	pipeline := map[string]interface{}{
		"description": "Extract file information",
		"processors": [1]map[string]interface{}{
			{
				"attachment": map[string]interface{}{
					"field": "file",
				},
			},
		},
	}
	_, err = client.IngestPutPipeline("attachment").BodyJson(pipeline).Do(ctx)
	return err
}

func defaultIndexTypeMapping(config *configOptions, op *gtm.Op) *indexTypeMapping {
	typeName := typeFromFuture
	if !config.useTypeFromFuture() {
		typeName = op.GetCollection()
	}
	return &indexTypeMapping{
		Namespace: op.Namespace,
		Index:     strings.ToLower(op.Namespace),
		Type:      typeName,
	}
}

func mapIndexType(config *configOptions, op *gtm.Op) *indexTypeMapping {
	mapping := defaultIndexTypeMapping(config, op)
	if m := mapIndexTypes[op.Namespace]; m != nil {
		if m.Index != "" {
			mapping.Index = m.Index
		}
		if m.Type != "" {
			mapping.Type = m.Type
		}
	}
	return mapping
}

func opIDToString(op *gtm.Op) string {
	var opIDStr string
	switch id := op.Id.(type) {
	case bson.ObjectId:
		opIDStr = id.Hex()
	case bson.Binary:
		opIDStr = monstachemap.EncodeBinData(monstachemap.Binary{id})
	case float64:
		intID := int(id)
		if id == float64(intID) {
			opIDStr = fmt.Sprintf("%v", intID)
		} else {
			opIDStr = fmt.Sprintf("%v", op.Id)
		}
	case float32:
		intID := int(id)
		if id == float32(intID) {
			opIDStr = fmt.Sprintf("%v", intID)
		} else {
			opIDStr = fmt.Sprintf("%v", op.Id)
		}
	default:
		opIDStr = fmt.Sprintf("%v", op.Id)
	}
	return opIDStr
}

func convertSliceJavascript(a []interface{}) []interface{} {
	var avs []interface{}
	for _, av := range a {
		var avc interface{}
		switch achild := av.(type) {
		case map[string]interface{}:
			avc = convertMapJavascript(achild)
		case []interface{}:
			avc = convertSliceJavascript(achild)
		case bson.ObjectId:
			avc = achild.Hex()
		default:
			avc = av
		}
		avs = append(avs, avc)
	}
	return avs
}

func convertMapJavascript(e map[string]interface{}) map[string]interface{} {
	o := make(map[string]interface{})
	for k, v := range e {
		switch child := v.(type) {
		case map[string]interface{}:
			o[k] = convertMapJavascript(child)
		case []interface{}:
			o[k] = convertSliceJavascript(child)
		case bson.ObjectId:
			o[k] = child.Hex()
		default:
			o[k] = v
		}
	}
	return o
}

func fixSlicePruneInvalidJSON(id string, key string, a []interface{}) []interface{} {
	var avs []interface{}
	for _, av := range a {
		var avc interface{}
		switch achild := av.(type) {
		case map[string]interface{}:
			avc = fixPruneInvalidJSON(id, achild)
		case []interface{}:
			avc = fixSlicePruneInvalidJSON(id, key, achild)
		case time.Time:
			year := achild.Year()
			if year < 0 || year > 9999 {
				// year outside of valid range
				warnLog.Printf("Dropping key %s element: invalid time.Time value: %s for document _id: %s", key, achild, id)
				continue
			} else {
				avc = av
			}
		case float64:
			if math.IsNaN(achild) {
				// causes an error in the json serializer
				warnLog.Printf("Dropping key %s element: invalid float64 value: %v for document _id: %s", key, achild, id)
				continue
			} else if math.IsInf(achild, 0) {
				// causes an error in the json serializer
				warnLog.Printf("Dropping key %s element: invalid float64 value: %v for document _id: %s", key, achild, id)
				continue
			} else {
				avc = av
			}
		default:
			avc = av
		}
		avs = append(avs, avc)
	}
	return avs
}

func fixPruneInvalidJSON(id string, e map[string]interface{}) map[string]interface{} {
	o := make(map[string]interface{})
	for k, v := range e {
		switch child := v.(type) {
		case map[string]interface{}:
			o[k] = fixPruneInvalidJSON(id, child)
		case []interface{}:
			o[k] = fixSlicePruneInvalidJSON(id, k, child)
		case time.Time:
			year := child.Year()
			if year < 0 || year > 9999 {
				// year outside of valid range
				warnLog.Printf("Dropping key %s: invalid time.Time value: %s for document _id: %s", k, child, id)
				continue
			} else {
				o[k] = v
			}
		case float64:
			if math.IsNaN(child) {
				// causes an error in the json serializer
				warnLog.Printf("Dropping key %s: invalid float64 value: %v for document _id: %s", k, child, id)
				continue
			} else if math.IsInf(child, 0) {
				// causes an error in the json serializer
				warnLog.Printf("Dropping key %s: invalid float64 value: %v for document _id: %s", k, child, id)
				continue
			} else {
				o[k] = v
			}
		default:
			o[k] = v
		}
	}
	return o
}

func deepExportValue(a interface{}) (b interface{}) {
	switch t := a.(type) {
	case otto.Value:
		ex, err := t.Export()
		if t.Class() == "Date" {
			ex, err = time.Parse("Mon, 2 Jan 2006 15:04:05 MST", t.String())
		}
		if err == nil {
			b = deepExportValue(ex)
		} else {
			errorLog.Printf("Error exporting from javascript: %s", err)
		}
	case map[string]interface{}:
		b = deepExportMap(t)
	case []map[string]interface{}:
		b = deepExportMapSlice(t)
	case []interface{}:
		b = deepExportSlice(t)
	default:
		b = a
	}
	return
}

func deepExportMapSlice(a []map[string]interface{}) []interface{} {
	var avs []interface{}
	for _, av := range a {
		avs = append(avs, deepExportMap(av))
	}
	return avs
}

func deepExportSlice(a []interface{}) []interface{} {
	var avs []interface{}
	for _, av := range a {
		avs = append(avs, deepExportValue(av))
	}
	return avs
}

func deepExportMap(e map[string]interface{}) map[string]interface{} {
	o := make(map[string]interface{})
	for k, v := range e {
		o[k] = deepExportValue(v)
	}
	return o
}

func mapDataJavascript(op *gtm.Op) error {
	names := []string{"", op.Namespace}
	for _, name := range names {
		if env := mapEnvs[name]; env != nil {
			env.lock.Lock()
			defer env.lock.Unlock()
			arg := convertMapJavascript(op.Data)
			arg2 := op.Namespace
			arg3 := convertMapJavascript(op.UpdateDescription)
			val, err := env.VM.Call("module.exports", arg, arg, arg2, arg3)
			if err != nil {
				return err
			}
			if strings.ToLower(val.Class()) == "object" {
				data, err := val.Export()
				if err != nil {
					return err
				} else if data == val {
					return errors.New("Exported function must return an object")
				} else {
					dm := data.(map[string]interface{})
					op.Data = deepExportMap(dm)
				}
			} else {
				indexed, err := val.ToBoolean()
				if err != nil {
					return err
				} else if !indexed {
					op.Data = nil
					break
				}
			}
		}
	}
	return nil
}

func mapDataGolang(s *mgo.Session, op *gtm.Op) error {
	session := s.Copy()
	defer session.Close()
	input := &monstachemap.MapperPluginInput{
		Document:          op.Data,
		Namespace:         op.Namespace,
		Database:          op.GetDatabase(),
		Collection:        op.GetCollection(),
		Operation:         op.Operation,
		Session:           session,
		UpdateDescription: op.UpdateDescription,
	}
	output, err := mapperPlugin(input)
	if err != nil {
		return err
	}
	if output != nil {
		if output.Drop {
			op.Data = nil
		} else {
			if output.Skip {
				op.Data = map[string]interface{}{}
			} else if output.Passthrough == false {
				if output.Document == nil {
					return errors.New("Map function must return a non-nil document")
				}
				op.Data = output.Document
			}
			meta := make(map[string]interface{})
			if output.Skip {
				meta["skip"] = true
			}
			if output.Index != "" {
				meta["index"] = output.Index
			}
			if output.ID != "" {
				meta["id"] = output.ID
			}
			if output.Type != "" {
				meta["type"] = output.Type
			}
			if output.Routing != "" {
				meta["routing"] = output.Routing
			}
			if output.Parent != "" {
				meta["parent"] = output.Parent
			}
			if output.Version != 0 {
				meta["version"] = output.Version
			}
			if output.VersionType != "" {
				meta["versionType"] = output.VersionType
			}
			if output.Pipeline != "" {
				meta["pipeline"] = output.Pipeline
			}
			if output.RetryOnConflict != 0 {
				meta["retryOnConflict"] = output.RetryOnConflict
			}
			if len(meta) > 0 {
				op.Data["_meta_monstache"] = meta
			}
		}
	}
	return nil
}

func mapData(session *mgo.Session, config *configOptions, op *gtm.Op) error {
	if mapperPlugin != nil {
		return mapDataGolang(session, op)
	}
	return mapDataJavascript(op)
}

func extractData(srcField string, data map[string]interface{}) (result interface{}, err error) {
	var cur map[string]interface{} = data
	fields := strings.Split(srcField, ".")
	flen := len(fields)
	for i, field := range fields {
		if i+1 == flen {
			result = cur[field]
		} else {
			if next, ok := cur[field].(map[string]interface{}); ok {
				cur = next
			} else {
				break
			}
		}
	}
	if result == nil {
		var detail interface{}
		b, e := json.Marshal(data)
		if e == nil {
			detail = string(b)
		} else {
			detail = err
		}
		err = fmt.Errorf("Source field %s not found in document: %s", srcField, detail)
	}
	return
}

func buildSelector(matchField string, data interface{}) bson.M {
	sel := bson.M{}
	var cur bson.M = sel
	fields := strings.Split(matchField, ".")
	flen := len(fields)
	for i, field := range fields {
		if i+1 == flen {
			cur[field] = data
		} else {
			next := bson.M{}
			cur[field] = next
			cur = next
		}
	}
	return sel
}

func processRelated(session *mgo.Session, bulk *elastic.BulkProcessor, elastic *elastic.Client, config *configOptions, root *gtm.Op, out *outputChans) (err error) {
	var q []*gtm.Op
	batch := []*gtm.Op{root}
	depth := 1
	s := session.Copy()
	if config.DirectReadNoTimeout {
		s.SetCursorTimeout(0)
	}
	defer s.Close()
	for len(batch) > 0 {
		for _, e := range batch {
			op := e
			if op.Data == nil {
				continue
			}
			rs := relates[op.Namespace]
			if len(rs) == 0 {
				continue
			}
			for _, r := range rs {
				if r.MaxDepth > 0 && r.MaxDepth < depth {
					continue
				}
				if op.IsDelete() && r.IsIdentity() {
					rop := &gtm.Op{
						Id:        op.Id,
						Operation: op.Operation,
						Namespace: r.WithNamespace,
						Source:    op.Source,
						Timestamp: op.Timestamp,
						Data:      op.Data,
					}
					doDelete(config, elastic, session, bulk, rop)
					q = append(q, rop)
					continue
				}
				var srcData interface{}
				if srcData, err = extractData(r.SrcField, op.Data); err != nil {
					processErr(err, config)
					continue
				}
				var sel bson.M
				if r.DotNotation {
					sel = bson.M{r.MatchField: srcData}
				} else {
					sel = buildSelector(r.MatchField, srcData)
				}
				col := s.DB(r.db).C(r.col)
				query := col.Find(sel)
				iter := query.Iter()
				doc := map[string]interface{}{}
				for iter.Next(doc) {
					now := time.Now().UTC()
					tstamp := bson.MongoTimestamp(now.Unix() << 32)
					offset := bson.MongoTimestamp(now.Nanosecond())
					rop := &gtm.Op{
						Id:                doc["_id"],
						Data:              doc,
						Operation:         root.Operation,
						Namespace:         r.WithNamespace,
						Source:            gtm.DirectQuerySource,
						Timestamp:         tstamp | offset,
						UpdateDescription: root.UpdateDescription,
					}
					doc = map[string]interface{}{}
					if out.filter != nil && !out.filter(rop) {
						continue
					}
					if processPlugin != nil {
						pop := &gtm.Op{
							Id:                rop.Id,
							Operation:         rop.Operation,
							Namespace:         rop.Namespace,
							Source:            rop.Source,
							Timestamp:         rop.Timestamp,
							UpdateDescription: rop.UpdateDescription,
						}
						var data []byte
						data, err = bson.Marshal(rop.Data)
						if err == nil {
							var m map[string]interface{}
							err = bson.Unmarshal(data, &m)
							if err == nil {
								pop.Data = m
							}
						}
						out.processC <- pop
					}
					skip := false
					if rs2 := relates[rop.Namespace]; len(rs2) != 0 {
						skip = true
						visit := false
						for _, r2 := range rs2 {
							if r2.KeepSrc {
								skip = false
							}
							if r2.MaxDepth < 1 || r2.MaxDepth >= (depth+1) {
								visit = true
							}
						}
						if visit {
							q = append(q, rop)
						}
					}
					if !skip {
						if hasFileContent(rop, config) {
							out.fileC <- rop
						} else {
							out.indexC <- rop
						}
					}
				}
				iter.Close()
			}
		}
		depth++
		batch = q
		q = nil
	}
	return
}

func prepareDataForIndexing(config *configOptions, op *gtm.Op) {
	data := op.Data
	if config.IndexOplogTime {
		secs := int64(op.Timestamp >> 32)
		t := time.Unix(secs, 0).UTC()
		data[config.OplogTsFieldName] = op.Timestamp
		data[config.OplogDateFieldName] = t.Format(config.OplogDateFieldFormat)
	}
	delete(data, "_id")
	delete(data, "_meta_monstache")
	if config.PruneInvalidJSON {
		op.Data = fixPruneInvalidJSON(opIDToString(op), data)
	}
	op.Data = monstachemap.ConvertMapForJSON(op.Data)
}

func parseIndexMeta(op *gtm.Op) (meta *indexingMeta) {
	meta = &indexingMeta{
		Version:     int64(op.Timestamp),
		VersionType: "external",
	}
	if m, ok := op.Data["_meta_monstache"]; ok {
		switch m.(type) {
		case map[string]interface{}:
			metaAttrs := m.(map[string]interface{})
			meta.load(metaAttrs)
		case otto.Value:
			ex, err := m.(otto.Value).Export()
			if err == nil && ex != m {
				switch ex.(type) {
				case map[string]interface{}:
					metaAttrs := ex.(map[string]interface{})
					meta.load(metaAttrs)
				default:
					errorLog.Println("Invalid indexing metadata")
				}
			}
		default:
			errorLog.Println("Invalid indexing metadata")
		}
	}
	return meta
}

func addFileContent(s *mgo.Session, op *gtm.Op, config *configOptions) (err error) {
	session := s.Copy()
	defer session.Close()
	op.Data["file"] = ""
	var gridByteBuffer bytes.Buffer
	db, bucket :=
		session.DB(op.GetDatabase()),
		strings.SplitN(op.GetCollection(), ".", 2)[0]
	encoder := base64.NewEncoder(base64.StdEncoding, &gridByteBuffer)
	file, err := db.GridFS(bucket).OpenId(op.Id)
	if err != nil {
		return
	}
	defer file.Close()
	if config.MaxFileSize > 0 {
		if file.Size() > config.MaxFileSize {
			warnLog.Printf("File %s md5(%s) exceeds max file size. file content omitted.",
				file.Name(), file.MD5())
			return
		}
	}
	if _, err = io.Copy(encoder, file); err != nil {
		return
	}
	if err = encoder.Close(); err != nil {
		return
	}
	op.Data["file"] = string(gridByteBuffer.Bytes())
	return
}

func notMonstache(config *configOptions) gtm.OpFilter {
	db := config.ConfigDatabaseName
	return func(op *gtm.Op) bool {
		return op.GetDatabase() != db
	}
}

func notChunks(op *gtm.Op) bool {
	return !chunksRegex.MatchString(op.GetCollection())
}

func notConfig(op *gtm.Op) bool {
	return op.GetDatabase() != "config"
}

func notSystem(op *gtm.Op) bool {
	return !systemsRegex.MatchString(op.GetCollection())
}

func filterWithRegex(regex string) gtm.OpFilter {
	var validNameSpace = regexp.MustCompile(regex)
	return func(op *gtm.Op) bool {
		if op.IsDrop() {
			return true
		}
		return validNameSpace.MatchString(op.Namespace)
	}
}

func filterDropWithRegex(regex string) gtm.OpFilter {
	var validNameSpace = regexp.MustCompile(regex)
	return func(op *gtm.Op) bool {
		if op.IsDrop() {
			return validNameSpace.MatchString(op.Namespace)
		}
		return true
	}
}

func filterWithPlugin() gtm.OpFilter {
	return func(op *gtm.Op) bool {
		var keep bool = true
		if (op.IsInsert() || op.IsUpdate()) && op.Data != nil {
			keep = false
			input := &monstachemap.MapperPluginInput{
				Document:          op.Data,
				Namespace:         op.Namespace,
				Database:          op.GetDatabase(),
				Collection:        op.GetCollection(),
				Operation:         op.Operation,
				UpdateDescription: op.UpdateDescription,
			}
			if ok, err := filterPlugin(input); err == nil {
				keep = ok
			} else {
				errorLog.Println(err)
			}
		}
		return keep
	}
}

func filterWithScript() gtm.OpFilter {
	return func(op *gtm.Op) bool {
		var keep = true
		if (op.IsInsert() || op.IsUpdate()) && op.Data != nil {
			nss := []string{"", op.Namespace}
			for _, ns := range nss {
				if env := filterEnvs[ns]; env != nil {
					keep = false
					arg := convertMapJavascript(op.Data)
					arg2 := op.Namespace
					arg3 := convertMapJavascript(op.UpdateDescription)
					env.lock.Lock()
					defer env.lock.Unlock()
					val, err := env.VM.Call("module.exports", arg, arg, arg2, arg3)
					if err != nil {
						errorLog.Println(err)
					} else {
						if ok, err := val.ToBoolean(); err == nil {
							keep = ok
						} else {
							errorLog.Println(err)
						}
					}
				}
				if !keep {
					break
				}
			}
		}
		return keep
	}
}

func filterInverseWithRegex(regex string) gtm.OpFilter {
	var invalidNameSpace = regexp.MustCompile(regex)
	return func(op *gtm.Op) bool {
		if op.IsDrop() {
			return true
		}
		return !invalidNameSpace.MatchString(op.Namespace)
	}
}

func filterDropInverseWithRegex(regex string) gtm.OpFilter {
	var invalidNameSpace = regexp.MustCompile(regex)
	return func(op *gtm.Op) bool {
		if op.IsDrop() {
			return !invalidNameSpace.MatchString(op.Namespace)
		}
		return true
	}
}

func ensureClusterTTL(session *mgo.Session, config *configOptions) error {
	col := session.DB(config.ConfigDatabaseName).C("cluster")
	return col.EnsureIndex(mgo.Index{
		Key:         []string{"expireAt"},
		Background:  true,
		ExpireAfter: time.Duration(30) * time.Second,
	})
}

func enableProcess(s *mgo.Session, config *configOptions) (bool, error) {
	session := s.Copy()
	defer session.Close()
	col := session.DB(config.ConfigDatabaseName).C("cluster")
	doc := make(map[string]interface{})
	doc["_id"] = config.ResumeName
	doc["pid"] = os.Getpid()
	if host, err := os.Hostname(); err == nil {
		doc["host"] = host
	} else {
		return false, err
	}
	doc["expireAt"] = time.Now().UTC()
	err := col.Insert(doc)
	if err == nil {
		ensureEnabled(s, config)
		return true, nil
	}
	if mgo.IsDup(err) {
		return false, nil
	}
	return false, err
}

func resetClusterState(session *mgo.Session, config *configOptions) error {
	col := session.DB(config.ConfigDatabaseName).C("cluster")
	return col.RemoveId(config.ResumeName)
}

func ensureEnabled(s *mgo.Session, config *configOptions) (enabled bool, err error) {
	session := s.Copy()
	defer session.Close()
	col := session.DB(config.ConfigDatabaseName).C("cluster")
	doc := make(map[string]interface{})
	if err = col.FindId(config.ResumeName).One(doc); err == nil {
		if doc["pid"] != nil && doc["host"] != nil {
			var hostname string
			pid := doc["pid"].(int)
			host := doc["host"].(string)
			if hostname, err = os.Hostname(); err == nil {
				enabled = (pid == os.Getpid() && host == hostname)
				if enabled {
					err = col.UpdateId(config.ResumeName,
						bson.M{"$currentDate": bson.M{"expireAt": true}})
				}
			}
		}
	}
	return
}

func resumeWork(ctx *gtm.OpCtxMulti, session *mgo.Session, config *configOptions) {
	col := session.DB(config.ConfigDatabaseName).C("monstache")
	doc := make(map[string]interface{})
	col.FindId(config.ResumeName).One(doc)
	if doc["ts"] != nil {
		ts := doc["ts"].(bson.MongoTimestamp)
		ctx.Since(ts)
	}
	drained := false
	for !drained {
		select {
		case _, open := <-ctx.OpC:
			if !open {
				drained = true
			}
		default:
			drained = true
		}
	}
	ctx.Resume()
}

func saveTimestamp(s *mgo.Session, ts bson.MongoTimestamp, config *configOptions) error {
	session := s.Copy()
	if config.ResumeWriteUnsafe {
		session.SetSafe(nil)
	}
	defer session.Close()
	col := session.DB(config.ConfigDatabaseName).C("monstache")
	doc := make(map[string]interface{})
	doc["ts"] = ts
	_, err := col.UpsertId(config.ResumeName, bson.M{"$set": doc})
	return err
}

func (config *configOptions) parseCommandLineFlags() *configOptions {
	flag.BoolVar(&config.Print, "print-config", false, "Print the configuration and then exit")
	flag.BoolVar(&config.EnableTemplate, "tpl", false, "True to interpret the config file as a template")
	flag.StringVar(&config.EnvDelimiter, "env-delimiter", ",", "A delimiter to use when splitting environment variable values")
	flag.StringVar(&config.MongoURL, "mongo-url", "", "MongoDB server or router server connection URL")
	flag.StringVar(&config.MongoConfigURL, "mongo-config-url", "", "MongoDB config server connection URL")
	flag.StringVar(&config.MongoPemFile, "mongo-pem-file", "", "Path to a PEM file for secure connections to MongoDB")
	flag.BoolVar(&config.MongoValidatePemFile, "mongo-validate-pem-file", true, "Set to boolean false to not validate the MongoDB PEM file")
	flag.StringVar(&config.MongoOpLogDatabaseName, "mongo-oplog-database-name", "", "Override the database name which contains the mongodb oplog")
	flag.StringVar(&config.MongoOpLogCollectionName, "mongo-oplog-collection-name", "", "Override the collection name which contains the mongodb oplog")
	flag.StringVar(&config.GraylogAddr, "graylog-addr", "", "Send logs to a Graylog server at this address")
	flag.StringVar(&config.ElasticVersion, "elasticsearch-version", "", "Specify elasticsearch version directly instead of getting it from the server")
	flag.StringVar(&config.ElasticUser, "elasticsearch-user", "", "The elasticsearch user name for basic auth")
	flag.StringVar(&config.ElasticPassword, "elasticsearch-password", "", "The elasticsearch password for basic auth")
	flag.StringVar(&config.ElasticPemFile, "elasticsearch-pem-file", "", "Path to a PEM file for secure connections to elasticsearch")
	flag.BoolVar(&config.ElasticValidatePemFile, "elasticsearch-validate-pem-file", true, "Set to boolean false to not validate the Elasticsearch PEM file")
	flag.IntVar(&config.ElasticMaxConns, "elasticsearch-max-conns", 0, "Elasticsearch max connections")
	flag.IntVar(&config.PostProcessors, "post-processors", 0, "Number of post-processing go routines")
	flag.IntVar(&config.FileDownloaders, "file-downloaders", 0, "GridFs download go routines")
	flag.IntVar(&config.RelateThreads, "relate-threads", 0, "Number of threads dedicated to processing relationships")
	flag.IntVar(&config.RelateBuffer, "relate-buffer", 0, "Number of relates to queue before skipping and reporting an error")
	flag.BoolVar(&config.ElasticRetry, "elasticsearch-retry", false, "True to retry failed request to Elasticsearch")
	flag.IntVar(&config.ElasticMaxDocs, "elasticsearch-max-docs", 0, "Number of docs to hold before flushing to Elasticsearch")
	flag.IntVar(&config.ElasticMaxBytes, "elasticsearch-max-bytes", 0, "Number of bytes to hold before flushing to Elasticsearch")
	flag.IntVar(&config.ElasticMaxSeconds, "elasticsearch-max-seconds", 0, "Number of seconds before flushing to Elasticsearch")
	flag.IntVar(&config.ElasticClientTimeout, "elasticsearch-client-timeout", 0, "Number of seconds before a request to Elasticsearch is timed out")
	flag.Int64Var(&config.MaxFileSize, "max-file-size", 0, "GridFs file content exceeding this limit in bytes will not be indexed in Elasticsearch")
	flag.StringVar(&config.ConfigFile, "f", "", "Location of configuration file")
	flag.BoolVar(&config.DroppedDatabases, "dropped-databases", true, "True to delete indexes from dropped databases")
	flag.BoolVar(&config.DroppedCollections, "dropped-collections", true, "True to delete indexes from dropped collections")
	flag.BoolVar(&config.Version, "v", false, "True to print the version number")
	flag.BoolVar(&config.Gzip, "gzip", false, "True to enable gzip for requests to Elasticsearch")
	flag.BoolVar(&config.Verbose, "verbose", false, "True to output verbose messages")
	flag.BoolVar(&config.Pprof, "pprof", false, "True to enable pprof endpoints")
	flag.BoolVar(&config.DisableChangeEvents, "disable-change-events", false, "True to disable listening for changes.  You must provide direct-reads in this case")
	flag.BoolVar(&config.EnableEasyJSON, "enable-easy-json", false, "True to enable easy-json serialization")
	flag.BoolVar(&config.Stats, "stats", false, "True to print out statistics")
	flag.BoolVar(&config.IndexStats, "index-stats", false, "True to index stats in elasticsearch")
	flag.StringVar(&config.StatsDuration, "stats-duration", "", "The duration after which stats are logged")
	flag.StringVar(&config.StatsIndexFormat, "stats-index-format", "", "time.Time supported format to use for the stats index names")
	flag.BoolVar(&config.Resume, "resume", false, "True to capture the last timestamp of this run and resume on a subsequent run")
	flag.Int64Var(&config.ResumeFromTimestamp, "resume-from-timestamp", 0, "Timestamp to resume syncing from")
	flag.BoolVar(&config.ResumeWriteUnsafe, "resume-write-unsafe", false, "True to speedup writes of the last timestamp synched for resuming at the cost of error checking")
	flag.BoolVar(&config.Replay, "replay", false, "True to replay all events from the oplog and index them in elasticsearch")
	flag.BoolVar(&config.IndexFiles, "index-files", false, "True to index gridfs files into elasticsearch. Requires the elasticsearch mapper-attachments (deprecated) or ingest-attachment plugin")
	flag.BoolVar(&config.IndexAsUpdate, "index-as-update", false, "True to index documents as updates instead of overwrites")
	flag.BoolVar(&config.FileHighlighting, "file-highlighting", false, "True to enable the ability to highlight search times for a file query")
	flag.BoolVar(&config.EnablePatches, "enable-patches", false, "True to include an json-patch field on updates")
	flag.BoolVar(&config.FailFast, "fail-fast", false, "True to exit if a single _bulk request fails")
	flag.BoolVar(&config.IndexOplogTime, "index-oplog-time", false, "True to add date/time information from the oplog to each document when indexing")
	flag.BoolVar(&config.ExitAfterDirectReads, "exit-after-direct-reads", false, "True to exit the program after reading directly from the configured namespaces")
	flag.StringVar(&config.MergePatchAttr, "merge-patch-attribute", "", "Attribute to store json-patch values under")
	flag.StringVar(&config.ResumeName, "resume-name", "", "Name under which to load/store the resume state. Defaults to 'default'")
	flag.StringVar(&config.ClusterName, "cluster-name", "", "Name of the monstache process cluster")
	flag.StringVar(&config.Worker, "worker", "", "The name of this worker in a multi-worker configuration")
	flag.StringVar(&config.MapperPluginPath, "mapper-plugin-path", "", "The path to a .so file to load as a document mapper plugin")
	flag.StringVar(&config.NsRegex, "namespace-regex", "", "A regex which is matched against an operation's namespace (<database>.<collection>).  Only operations which match are synched to elasticsearch")
	flag.StringVar(&config.NsDropRegex, "namespace-drop-regex", "", "A regex which is matched against a drop operation's namespace (<database>.<collection>).  Only drop operations which match are synched to elasticsearch")
	flag.StringVar(&config.NsExcludeRegex, "namespace-exclude-regex", "", "A regex which is matched against an operation's namespace (<database>.<collection>).  Only operations which do not match are synched to elasticsearch")
	flag.StringVar(&config.NsDropExcludeRegex, "namespace-drop-exclude-regex", "", "A regex which is matched against a drop operation's namespace (<database>.<collection>).  Only drop operations which do not match are synched to elasticsearch")
	flag.Var(&config.ChangeStreamNs, "change-stream-namespace", "A list of change stream namespaces")
	flag.Var(&config.DirectReadNs, "direct-read-namespace", "A list of direct read namespaces")
	flag.IntVar(&config.DirectReadSplitMax, "direct-read-split-max", 0, "Max number of times to split a collection for direct reads")
	flag.IntVar(&config.DirectReadConcur, "direct-read-concur", 0, "Max number of direct-read-namespaces to read concurrently. By default all given are read concurrently")
	flag.Var(&config.RoutingNamespaces, "routing-namespace", "A list of namespaces that override routing information")
	flag.Var(&config.TimeMachineNamespaces, "time-machine-namespace", "A list of direct read namespaces")
	flag.StringVar(&config.TimeMachineIndexPrefix, "time-machine-index-prefix", "", "A prefix to preprend to time machine indexes")
	flag.StringVar(&config.TimeMachineIndexSuffix, "time-machine-index-suffix", "", "A suffix to append to time machine indexes")
	flag.BoolVar(&config.DirectReadNoTimeout, "direct-read-no-timeout", false, "True to set the no cursor timeout flag for direct reads")
	flag.BoolVar(&config.TimeMachineDirectReads, "time-machine-direct-reads", false, "True to index the results of direct reads into the any time machine indexes")
	flag.BoolVar(&config.PipeAllowDisk, "pipe-allow-disk", false, "True to allow MongoDB to use the disk for pipeline options with lots of results")
	flag.Var(&config.ElasticUrls, "elasticsearch-url", "A list of Elasticsearch URLs")
	flag.Var(&config.FileNamespaces, "file-namespace", "A list of file namespaces")
	flag.Var(&config.PatchNamespaces, "patch-namespace", "A list of patch namespaces")
	flag.Var(&config.Workers, "workers", "A list of worker names")
	flag.BoolVar(&config.EnableHTTPServer, "enable-http-server", false, "True to enable an internal http server")
	flag.StringVar(&config.HTTPServerAddr, "http-server-addr", "", "The address the internal http server listens on")
	flag.BoolVar(&config.PruneInvalidJSON, "prune-invalid-json", false, "True to omit values which do not serialize to JSON such as +Inf and -Inf and thus cause errors")
	flag.Var(&config.DeleteStrategy, "delete-strategy", "Stategy to use for deletes. 0=stateless,1=stateful,2=ignore")
	flag.StringVar(&config.DeleteIndexPattern, "delete-index-pattern", "", "An Elasticsearch index-pattern to restric the scope of stateless deletes")
	flag.StringVar(&config.ConfigDatabaseName, "config-database-name", "", "The MongoDB database name that monstache uses to store metadata")
	flag.StringVar(&config.OplogTsFieldName, "oplog-ts-field-name", "", "Field name to use for the oplog timestamp")
	flag.StringVar(&config.OplogDateFieldName, "oplog-date-field-name", "", "Field name to use for the oplog date")
	flag.StringVar(&config.OplogDateFieldFormat, "oplog-date-field-format", "", "Format to use for the oplog date")
	flag.BoolVar(&config.Debug, "debug", false, "True to enable verbose debug information")
	flag.Parse()
	return config
}

func (config *configOptions) loadReplacements() {
	if config.Relate != nil {
		for _, r := range config.Relate {
			if r.Namespace != "" || r.WithNamespace != "" {
				dbCol := strings.SplitN(r.WithNamespace, ".", 2)
				if len(dbCol) != 2 {
					panic("Replacement namespace is invalid: " + r.WithNamespace)
				}
				database, collection := dbCol[0], dbCol[1]
				r := &relation{
					Namespace:     r.Namespace,
					WithNamespace: r.WithNamespace,
					SrcField:      r.SrcField,
					MatchField:    r.MatchField,
					DotNotation:   r.DotNotation,
					KeepSrc:       r.KeepSrc,
					MaxDepth:      r.MaxDepth,
					db:            database,
					col:           collection,
				}
				if r.SrcField == "" {
					r.SrcField = "_id"
				}
				if r.MatchField == "" {
					r.MatchField = "_id"
				}
				relates[r.Namespace] = append(relates[r.Namespace], r)
			} else {
				panic("Relates must specify namespace and with-namespace")
			}
		}
	}
}

func (config *configOptions) loadIndexTypes() {
	if config.Mapping != nil {
		for _, m := range config.Mapping {
			if m.Namespace != "" && (m.Index != "" || m.Type != "") {
				mapIndexTypes[m.Namespace] = &indexTypeMapping{
					Namespace: m.Namespace,
					Index:     strings.ToLower(m.Index),
					Type:      m.Type,
				}
			} else {
				panic("Mappings must specify namespace and at least one of index and type")
			}
		}
	}
}

func (config *configOptions) loadPipelines() {
	for _, s := range config.Pipeline {
		if s.Path == "" && s.Script == "" {
			panic("Pipelines must specify path or script attributes")
		}
		if s.Path != "" && s.Script != "" {
			panic("Pipelines must specify path or script but not both")
		}
		if s.Path != "" {
			if script, err := ioutil.ReadFile(s.Path); err == nil {
				s.Script = string(script[:])
			} else {
				panic(fmt.Sprintf("Unable to load pipeline at path %s: %s", s.Path, err))
			}
		}
		if _, exists := filterEnvs[s.Namespace]; exists {
			panic(fmt.Sprintf("Multiple pipelines with namespace: %s", s.Namespace))
		}
		env := &executionEnv{
			VM:     otto.New(),
			Script: s.Script,
			lock:   &sync.Mutex{},
		}
		if err := env.VM.Set("module", make(map[string]interface{})); err != nil {
			panic(err)
		}
		if _, err := env.VM.Run(env.Script); err != nil {
			panic(err)
		}
		val, err := env.VM.Run("module.exports")
		if err != nil {
			panic(err)
		} else if !val.IsFunction() {
			panic("module.exports must be a function")
		}
		pipeEnvs[s.Namespace] = env
	}
}

func (config *configOptions) loadFilters() {
	for _, s := range config.Filter {
		if s.Script != "" || s.Path != "" {
			if s.Path != "" && s.Script != "" {
				panic("Filters must specify path or script but not both")
			}
			if s.Path != "" {
				if script, err := ioutil.ReadFile(s.Path); err == nil {
					s.Script = string(script[:])
				} else {
					panic(fmt.Sprintf("Unable to load filter at path %s: %s", s.Path, err))
				}
			}
			if _, exists := filterEnvs[s.Namespace]; exists {
				panic(fmt.Sprintf("Multiple filters with namespace: %s", s.Namespace))
			}
			env := &executionEnv{
				VM:     otto.New(),
				Script: s.Script,
				lock:   &sync.Mutex{},
			}
			if err := env.VM.Set("module", make(map[string]interface{})); err != nil {
				panic(err)
			}
			if _, err := env.VM.Run(env.Script); err != nil {
				panic(err)
			}
			val, err := env.VM.Run("module.exports")
			if err != nil {
				panic(err)
			} else if !val.IsFunction() {
				panic("module.exports must be a function")
			}
			filterEnvs[s.Namespace] = env
		} else {
			panic("Filters must specify path or script attributes")
		}
	}
}

func (config *configOptions) loadScripts() {
	for _, s := range config.Script {
		if s.Script != "" || s.Path != "" {
			if s.Path != "" && s.Script != "" {
				panic("Scripts must specify path or script but not both")
			}
			if s.Path != "" {
				if script, err := ioutil.ReadFile(s.Path); err == nil {
					s.Script = string(script[:])
				} else {
					panic(fmt.Sprintf("Unable to load script at path %s: %s", s.Path, err))
				}
			}
			if _, exists := mapEnvs[s.Namespace]; exists {
				panic(fmt.Sprintf("Multiple scripts with namespace: %s", s.Namespace))
			}
			env := &executionEnv{
				VM:     otto.New(),
				Script: s.Script,
				lock:   &sync.Mutex{},
			}
			if err := env.VM.Set("module", make(map[string]interface{})); err != nil {
				panic(err)
			}
			if _, err := env.VM.Run(env.Script); err != nil {
				panic(err)
			}
			val, err := env.VM.Run("module.exports")
			if err != nil {
				panic(err)
			} else if !val.IsFunction() {
				panic("module.exports must be a function")
			}

			mapEnvs[s.Namespace] = env
			if s.Routing {
				routingNamespaces[s.Namespace] = true
			}
		} else {
			panic("Scripts must specify path or script")
		}
	}
}

func (config *configOptions) loadPlugins() *configOptions {
	if config.MapperPluginPath != "" {
		funcDefined := false
		p, err := plugin.Open(config.MapperPluginPath)
		if err != nil {
			panic(fmt.Sprintf("Unable to load mapper plugin %s: %s", config.MapperPluginPath, err))
		}
		mapper, err := p.Lookup("Map")
		if err == nil {
			funcDefined = true
			switch mapper.(type) {
			case func(*monstachemap.MapperPluginInput) (*monstachemap.MapperPluginOutput, error):
				mapperPlugin = mapper.(func(*monstachemap.MapperPluginInput) (*monstachemap.MapperPluginOutput, error))
			default:
				panic(fmt.Sprintf("Plugin 'Map' function must be typed %T", mapperPlugin))
			}
		}
		filter, err := p.Lookup("Filter")
		if err == nil {
			funcDefined = true
			switch filter.(type) {
			case func(*monstachemap.MapperPluginInput) (bool, error):
				filterPlugin = filter.(func(*monstachemap.MapperPluginInput) (bool, error))
			default:
				panic(fmt.Sprintf("Plugin 'Filter' function must be typed %T", filterPlugin))
			}

		}
		process, err := p.Lookup("Process")
		if err == nil {
			funcDefined = true
			switch process.(type) {
			case func(*monstachemap.ProcessPluginInput) error:
				processPlugin = process.(func(*monstachemap.ProcessPluginInput) error)
			default:
				panic(fmt.Sprintf("Plugin 'Process' function must be typed %T", processPlugin))
			}
		}
		pipe, err := p.Lookup("Pipeline")
		if err == nil {
			funcDefined = true
			switch pipe.(type) {
			case func(string, bool) ([]interface{}, error):
				pipePlugin = pipe.(func(string, bool) ([]interface{}, error))
			default:
				panic(fmt.Sprintf("Plugin 'Pipeline' function must be typed %T", pipePlugin))
			}
		}
		if !funcDefined {
			warnLog.Println("Plugin loaded but did not find a Map, Filter, Process or Pipeline function")
		}
	}
	return config
}

func (config *configOptions) decodeAsTemplate() *configOptions {
	env := map[string]string{}
	for _, e := range os.Environ() {
		pair := strings.SplitN(e, "=", 2)
		if len(pair) < 2 {
			continue
		}
		name, val := pair[0], pair[1]
		env[name] = val
	}
	tpl, err := ioutil.ReadFile(config.ConfigFile)
	if err != nil {
		panic(err)
	}
	var t = template.Must(template.New("config").Parse(string(tpl)))
	var b bytes.Buffer
	err = t.Execute(&b, env)
	if err != nil {
		panic(err)
	}
	if _, err := toml.Decode(b.String(), config); err != nil {
		panic(err)
	}
	return config
}

func (config *configOptions) loadConfigFile() *configOptions {
	if config.ConfigFile != "" {
		var tomlConfig = configOptions{
			ConfigFile:             config.ConfigFile,
			DroppedDatabases:       true,
			DroppedCollections:     true,
			MongoValidatePemFile:   true,
			ElasticValidatePemFile: true,
			MongoDialSettings:      mongoDialSettings{Timeout: -1, ReadTimeout: -1, WriteTimeout: -1},
			MongoSessionSettings:   mongoSessionSettings{SocketTimeout: -1, SyncTimeout: -1},
			GtmSettings:            gtmDefaultSettings(),
		}
		if config.EnableTemplate {
			tomlConfig.decodeAsTemplate()
		} else {
			if _, err := toml.DecodeFile(tomlConfig.ConfigFile, &tomlConfig); err != nil {
				panic(err)
			}
		}
		if config.MongoURL == "" {
			config.MongoURL = tomlConfig.MongoURL
		}
		if config.MongoConfigURL == "" {
			config.MongoConfigURL = tomlConfig.MongoConfigURL
		}
		if config.MongoPemFile == "" {
			config.MongoPemFile = tomlConfig.MongoPemFile
		}
		if config.MongoValidatePemFile {
			config.MongoValidatePemFile = tomlConfig.MongoValidatePemFile
		}
		if config.MongoOpLogDatabaseName == "" {
			config.MongoOpLogDatabaseName = tomlConfig.MongoOpLogDatabaseName
		}
		if config.MongoOpLogCollectionName == "" {
			config.MongoOpLogCollectionName = tomlConfig.MongoOpLogCollectionName
		}
		if config.ElasticUser == "" {
			config.ElasticUser = tomlConfig.ElasticUser
		}
		if config.ElasticPassword == "" {
			config.ElasticPassword = tomlConfig.ElasticPassword
		}
		if config.ElasticPemFile == "" {
			config.ElasticPemFile = tomlConfig.ElasticPemFile
		}
		if config.ElasticValidatePemFile && !tomlConfig.ElasticValidatePemFile {
			config.ElasticValidatePemFile = false
		}
		if config.ElasticVersion == "" {
			config.ElasticVersion = tomlConfig.ElasticVersion
		}
		if config.ElasticMaxConns == 0 {
			config.ElasticMaxConns = tomlConfig.ElasticMaxConns
		}
		if config.ElasticHealth0 == 0 {
			config.ElasticHealth0 = tomlConfig.ElasticHealth0
		}
		if config.ElasticHealth1 == 0 {
			config.ElasticHealth1 = tomlConfig.ElasticHealth1
		}
		if config.DirectReadSplitMax == 0 {
			config.DirectReadSplitMax = tomlConfig.DirectReadSplitMax
		}
		if config.DirectReadConcur == 0 {
			config.DirectReadConcur = tomlConfig.DirectReadConcur
		}
		if !config.DirectReadNoTimeout && tomlConfig.DirectReadNoTimeout {
			config.DirectReadNoTimeout = true
		}
		if !config.ElasticRetry && tomlConfig.ElasticRetry {
			config.ElasticRetry = true
		}
		if config.ElasticMaxDocs == 0 {
			config.ElasticMaxDocs = tomlConfig.ElasticMaxDocs
		}
		if config.ElasticMaxBytes == 0 {
			config.ElasticMaxBytes = tomlConfig.ElasticMaxBytes
		}
		if config.ElasticMaxSeconds == 0 {
			config.ElasticMaxSeconds = tomlConfig.ElasticMaxSeconds
		}
		if config.ElasticClientTimeout == 0 {
			config.ElasticClientTimeout = tomlConfig.ElasticClientTimeout
		}
		if config.MaxFileSize == 0 {
			config.MaxFileSize = tomlConfig.MaxFileSize
		}
		if !config.IndexFiles {
			config.IndexFiles = tomlConfig.IndexFiles
		}
		if config.FileDownloaders == 0 {
			config.FileDownloaders = tomlConfig.FileDownloaders
		}
		if config.RelateThreads == 0 {
			config.RelateThreads = tomlConfig.RelateThreads
		}
		if config.RelateBuffer == 0 {
			config.RelateBuffer = tomlConfig.RelateBuffer
		}
		if config.PostProcessors == 0 {
			config.PostProcessors = tomlConfig.PostProcessors
		}
		if config.DeleteStrategy == 0 {
			config.DeleteStrategy = tomlConfig.DeleteStrategy
		}
		if config.DeleteIndexPattern == "" {
			config.DeleteIndexPattern = tomlConfig.DeleteIndexPattern
		}
		if config.DroppedDatabases && !tomlConfig.DroppedDatabases {
			config.DroppedDatabases = false
		}
		if config.DroppedCollections && !tomlConfig.DroppedCollections {
			config.DroppedCollections = false
		}
		if !config.Gzip && tomlConfig.Gzip {
			config.Gzip = true
		}
		if !config.Verbose && tomlConfig.Verbose {
			config.Verbose = true
		}
		if !config.Stats && tomlConfig.Stats {
			config.Stats = true
		}
		if !config.Pprof && tomlConfig.Pprof {
			config.Pprof = true
		}
		if !config.EnableEasyJSON && tomlConfig.EnableEasyJSON {
			config.EnableEasyJSON = true
		}
		if !config.DisableChangeEvents && tomlConfig.DisableChangeEvents {
			config.DisableChangeEvents = true
		}
		if !config.IndexStats && tomlConfig.IndexStats {
			config.IndexStats = true
		}
		if config.StatsDuration == "" {
			config.StatsDuration = tomlConfig.StatsDuration
		}
		if config.StatsIndexFormat == "" {
			config.StatsIndexFormat = tomlConfig.StatsIndexFormat
		}
		if !config.IndexAsUpdate && tomlConfig.IndexAsUpdate {
			config.IndexAsUpdate = true
		}
		if !config.FileHighlighting && tomlConfig.FileHighlighting {
			config.FileHighlighting = true
		}
		if !config.EnablePatches && tomlConfig.EnablePatches {
			config.EnablePatches = true
		}
		if !config.PruneInvalidJSON && tomlConfig.PruneInvalidJSON {
			config.PruneInvalidJSON = true
		}
		if !config.Debug && tomlConfig.Debug {
			config.Debug = true
		}
		if !config.Replay && tomlConfig.Replay {
			config.Replay = true
		}
		if !config.Resume && tomlConfig.Resume {
			config.Resume = true
		}
		if !config.ResumeWriteUnsafe && tomlConfig.ResumeWriteUnsafe {
			config.ResumeWriteUnsafe = true
		}
		if config.ResumeFromTimestamp == 0 {
			config.ResumeFromTimestamp = tomlConfig.ResumeFromTimestamp
		}
		if config.MergePatchAttr == "" {
			config.MergePatchAttr = tomlConfig.MergePatchAttr
		}
		if !config.FailFast && tomlConfig.FailFast {
			config.FailFast = true
		}
		if !config.IndexOplogTime && tomlConfig.IndexOplogTime {
			config.IndexOplogTime = true
		}
		if config.OplogTsFieldName == "" {
			config.OplogTsFieldName = tomlConfig.OplogTsFieldName
		}
		if config.OplogDateFieldName == "" {
			config.OplogDateFieldName = tomlConfig.OplogDateFieldName
		}
		if config.OplogDateFieldFormat == "" {
			config.OplogDateFieldFormat = tomlConfig.OplogDateFieldFormat
		}
		if config.ConfigDatabaseName == "" {
			config.ConfigDatabaseName = tomlConfig.ConfigDatabaseName
		}
		if !config.ExitAfterDirectReads && tomlConfig.ExitAfterDirectReads {
			config.ExitAfterDirectReads = true
		}
		if config.ResumeName == "" {
			config.ResumeName = tomlConfig.ResumeName
		}
		if config.ClusterName == "" {
			config.ClusterName = tomlConfig.ClusterName
		}
		if config.NsRegex == "" {
			config.NsRegex = tomlConfig.NsRegex
		}
		if config.NsDropRegex == "" {
			config.NsDropRegex = tomlConfig.NsDropRegex
		}
		if config.NsExcludeRegex == "" {
			config.NsExcludeRegex = tomlConfig.NsExcludeRegex
		}
		if config.NsDropExcludeRegex == "" {
			config.NsDropExcludeRegex = tomlConfig.NsDropExcludeRegex
		}
		if config.IndexFiles {
			if len(config.FileNamespaces) == 0 {
				config.FileNamespaces = tomlConfig.FileNamespaces
				config.loadGridFsConfig()
			}
		}
		if config.Worker == "" {
			config.Worker = tomlConfig.Worker
		}
		if config.GraylogAddr == "" {
			config.GraylogAddr = tomlConfig.GraylogAddr
		}
		if config.MapperPluginPath == "" {
			config.MapperPluginPath = tomlConfig.MapperPluginPath
		}
		if config.EnablePatches {
			if len(config.PatchNamespaces) == 0 {
				config.PatchNamespaces = tomlConfig.PatchNamespaces
				config.loadPatchNamespaces()
			}
		}
		if len(config.RoutingNamespaces) == 0 {
			config.RoutingNamespaces = tomlConfig.RoutingNamespaces
			config.loadRoutingNamespaces()
		}
		if len(config.TimeMachineNamespaces) == 0 {
			config.TimeMachineNamespaces = tomlConfig.TimeMachineNamespaces
			config.loadTimeMachineNamespaces()
		}
		if config.TimeMachineIndexPrefix == "" {
			config.TimeMachineIndexPrefix = tomlConfig.TimeMachineIndexPrefix
		}
		if config.TimeMachineIndexSuffix == "" {
			config.TimeMachineIndexSuffix = tomlConfig.TimeMachineIndexSuffix
		}
		if !config.TimeMachineDirectReads {
			config.TimeMachineDirectReads = tomlConfig.TimeMachineDirectReads
		}
		if !config.PipeAllowDisk {
			config.PipeAllowDisk = tomlConfig.PipeAllowDisk
		}
		if len(config.DirectReadNs) == 0 {
			config.DirectReadNs = tomlConfig.DirectReadNs
		}
		if len(config.ChangeStreamNs) == 0 {
			config.ChangeStreamNs = tomlConfig.ChangeStreamNs
		}
		if len(config.ElasticUrls) == 0 {
			config.ElasticUrls = tomlConfig.ElasticUrls
		}
		if len(config.Workers) == 0 {
			config.Workers = tomlConfig.Workers
		}
		if !config.EnableHTTPServer && tomlConfig.EnableHTTPServer {
			config.EnableHTTPServer = true
		}
		if config.HTTPServerAddr == "" {
			config.HTTPServerAddr = tomlConfig.HTTPServerAddr
		}
		if !config.AWSConnect.enabled() {
			config.AWSConnect = tomlConfig.AWSConnect
		}
		if !config.Logs.enabled() {
			config.Logs = tomlConfig.Logs
		}
		config.MongoDialSettings = tomlConfig.MongoDialSettings
		config.MongoSessionSettings = tomlConfig.MongoSessionSettings
		config.MongoX509Settings = tomlConfig.MongoX509Settings
		config.GtmSettings = tomlConfig.GtmSettings
		config.Relate = tomlConfig.Relate
		tomlConfig.loadScripts()
		tomlConfig.loadFilters()
		tomlConfig.loadPipelines()
		tomlConfig.loadIndexTypes()
		tomlConfig.loadReplacements()
	}
	return config
}

func (config *configOptions) newLogger(path string) *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename:   path,
		MaxSize:    500, // megabytes
		MaxBackups: 5,
		MaxAge:     28, //days
	}
}

func (config *configOptions) setupLogging() *configOptions {
	if config.GraylogAddr != "" {
		gelfWriter, err := gelf.NewUDPWriter(config.GraylogAddr)
		if err != nil {
			errorLog.Fatalf("Error creating gelf writer: %s", err)
		}
		infoLog.SetOutput(gelfWriter)
		warnLog.SetOutput(gelfWriter)
		errorLog.SetOutput(gelfWriter)
		traceLog.SetOutput(gelfWriter)
		statsLog.SetOutput(gelfWriter)
	} else {
		logs := config.Logs
		if logs.Info != "" {
			infoLog.SetOutput(config.newLogger(logs.Info))
		}
		if logs.Warn != "" {
			warnLog.SetOutput(config.newLogger(logs.Warn))
		}
		if logs.Error != "" {
			errorLog.SetOutput(config.newLogger(logs.Error))
		}
		if logs.Trace != "" {
			traceLog.SetOutput(config.newLogger(logs.Trace))
		}
		if logs.Stats != "" {
			statsLog.SetOutput(config.newLogger(logs.Stats))
		}
	}
	if config.Debug {
		mgo.SetDebug(true)
		mgo.SetLogger(traceLog)
	}
	return config
}

func (config *configOptions) loadEnvironment() *configOptions {
	del := config.EnvDelimiter
	if del == "" {
		del = ","
	}
	for _, e := range os.Environ() {
		pair := strings.SplitN(e, "=", 2)
		if len(pair) < 2 {
			continue
		}
		name, val := pair[0], pair[1]
		if val == "" {
			continue
		}
		if strings.HasSuffix(name, "__FILE") {
			var err error
			name, val, err = config.loadVariableValueFromFile(name, val)
			if err != nil {
				panic(err)
			}
		}
		switch name {
		case "MONSTACHE_MONGO_URL":
			if config.MongoURL == "" {
				config.MongoURL = val
			}
			break
		case "MONSTACHE_MONGO_CONFIG_URL":
			if config.MongoConfigURL == "" {
				config.MongoConfigURL = val
			}
			break
		case "MONSTACHE_MONGO_PEM":
			if config.MongoPemFile == "" {
				config.MongoPemFile = val
			}
			break
		case "MONSTACHE_MONGO_VALIDATE_PEM":
			v, err := strconv.ParseBool(val)
			if err != nil {
				errorLog.Fatalf("Failed to load MONSTACHE_MONGO_VALIDATE_PEM: %s", err)
			}
			config.MongoValidatePemFile = v
			break
		case "MONSTACHE_MONGO_OPLOG_DB":
			if config.MongoOpLogDatabaseName == "" {
				config.MongoOpLogDatabaseName = val
			}
			break
		case "MONSTACHE_MONGO_OPLOG_COL":
			if config.MongoOpLogCollectionName == "" {
				config.MongoOpLogCollectionName = val
			}
			break
		case "MONSTACHE_ES_URLS":
			if len(config.ElasticUrls) == 0 {
				config.ElasticUrls = strings.Split(val, del)
			}
			break
		case "MONSTACHE_ES_USER":
			if config.ElasticUser == "" {
				config.ElasticUser = val
			}
			break
		case "MONSTACHE_ES_PASS":
			if config.ElasticPassword == "" {
				config.ElasticPassword = val
			}
			break
		case "MONSTACHE_ES_PEM":
			if config.ElasticPemFile == "" {
				config.ElasticPemFile = val
			}
			break
		case "MONSTACHE_ES_VALIDATE_PEM":
			v, err := strconv.ParseBool(val)
			if err != nil {
				errorLog.Fatalf("Failed to load MONSTACHE_ES_VALIDATE_PEM: %s", err)
			}
			config.ElasticValidatePemFile = v
			break
		case "MONSTACHE_WORKER":
			if config.Worker == "" {
				config.Worker = val
			}
			break
		case "MONSTACHE_CLUSTER":
			if config.ClusterName == "" {
				config.ClusterName = val
			}
			break
		case "MONSTACHE_DIRECT_READ_NS":
			if len(config.DirectReadNs) == 0 {
				config.DirectReadNs = strings.Split(val, del)
			}
			break
		case "MONSTACHE_CHANGE_STREAM_NS":
			if len(config.ChangeStreamNs) == 0 {
				config.ChangeStreamNs = strings.Split(val, del)
			}
			break
		case "MONSTACHE_NS_REGEX":
			if config.NsRegex == "" {
				config.NsRegex = val
			}
			break
		case "MONSTACHE_NS_EXCLUDE_REGEX":
			if config.NsExcludeRegex == "" {
				config.NsExcludeRegex = val
			}
			break
		case "MONSTACHE_NS_DROP_REGEX":
			if config.NsDropRegex == "" {
				config.NsDropRegex = val
			}
			break
		case "MONSTACHE_NS_DROP_EXCLUDE_REGEX":
			if config.NsDropExcludeRegex == "" {
				config.NsDropExcludeRegex = val
			}
			break
		case "MONSTACHE_GRAYLOG_ADDR":
			if config.GraylogAddr == "" {
				config.GraylogAddr = val
			}
			break
		case "MONSTACHE_AWS_ACCESS_KEY":
			config.AWSConnect.AccessKey = val
			break
		case "MONSTACHE_AWS_SECRET_KEY":
			config.AWSConnect.SecretKey = val
			break
		case "MONSTACHE_AWS_REGION":
			config.AWSConnect.Region = val
			break
		case "MONSTACHE_LOG_DIR":
			config.Logs.Info = val + "/info.log"
			config.Logs.Warn = val + "/warn.log"
			config.Logs.Error = val + "/error.log"
			config.Logs.Trace = val + "/trace.log"
			config.Logs.Stats = val + "/stats.log"
			break
		case "MONSTACHE_HTTP_ADDR":
			if config.HTTPServerAddr == "" {
				config.HTTPServerAddr = val
			}
			break
		case "MONSTACHE_FILE_NS":
			if len(config.FileNamespaces) == 0 {
				config.FileNamespaces = strings.Split(val, del)
			}
			break
		case "MONSTACHE_PATCH_NS":
			if len(config.PatchNamespaces) == 0 {
				config.PatchNamespaces = strings.Split(val, del)
			}
			break
		case "MONSTACHE_TIME_MACHINE_NS":
			if len(config.TimeMachineNamespaces) == 0 {
				config.TimeMachineNamespaces = strings.Split(val, del)
			}
			break
		default:
			continue
		}
	}
	return config
}

func (config *configOptions) loadVariableValueFromFile(name string, path string) (n string, v string, err error) {
	name = strings.TrimSuffix(name, "__FILE")
	f, err := os.Open(path)
	if err != nil {
		return name, "", fmt.Errorf("read value for %s from file failed: %s", name, err)
	}
	defer f.Close()
	c, err := ioutil.ReadAll(f)
	if err != nil {
		return name, "", fmt.Errorf("read value for %s from file failed: %s", name, err)
	}
	return name, string(c), nil
}

func (config *configOptions) loadRoutingNamespaces() *configOptions {
	for _, namespace := range config.RoutingNamespaces {
		routingNamespaces[namespace] = true
	}
	return config
}

func (config *configOptions) loadTimeMachineNamespaces() *configOptions {
	for _, namespace := range config.TimeMachineNamespaces {
		tmNamespaces[namespace] = true
	}
	return config
}

func (config *configOptions) loadPatchNamespaces() *configOptions {
	for _, namespace := range config.PatchNamespaces {
		patchNamespaces[namespace] = true
	}
	return config
}

func (config *configOptions) loadGridFsConfig() *configOptions {
	for _, namespace := range config.FileNamespaces {
		fileNamespaces[namespace] = true
	}
	return config
}

func (config configOptions) dump() {
	if config.MongoURL != "" {
		config.MongoURL = cleanMongoURL(config.MongoURL)
	}
	if config.MongoConfigURL != "" {
		config.MongoConfigURL = cleanMongoURL(config.MongoConfigURL)
	}
	if config.ElasticUser != "" {
		config.ElasticUser = redact
	}
	if config.ElasticPassword != "" {
		config.ElasticPassword = redact
	}
	if config.AWSConnect.AccessKey != "" {
		config.AWSConnect.AccessKey = redact
	}
	if config.AWSConnect.SecretKey != "" {
		config.AWSConnect.SecretKey = redact
	}
	if config.AWSConnect.Region != "" {
		config.AWSConnect.Region = redact
	}
	json, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		errorLog.Printf("Unable to print configuration: %s", err)
	} else {
		infoLog.Println(string(json))
	}
}

/*
if ssl=true is set on the connection string, remove the option
from the connection string and enable TLS because the mgo
driver does not support the option in the connection string
*/
func (config *configOptions) parseMongoURL(inURL string) (outURL string) {
	const queryDelim string = "?"
	outURL = inURL
	hostQuery := strings.SplitN(outURL, queryDelim, 2)
	if len(hostQuery) == 2 {
		host, query := hostQuery[0], hostQuery[1]
		r := regexp.MustCompile(`ssl=true&?|&ssl=true$`)
		qstr := r.ReplaceAllString(query, "")
		if qstr != query {
			config.MongoDialSettings.Ssl = true
			if qstr == "" {
				outURL = host
			} else {
				outURL = strings.Join([]string{host, qstr}, queryDelim)
			}
		}
	}
	return
}

func (config *configOptions) validate() {
	const schemeSrv = "mongodb+srv://"
	if strings.HasPrefix(config.MongoURL, schemeSrv) {
		panic("The mongodb+srv scheme is not yet supported")
	}
	if config.DisableChangeEvents && len(config.DirectReadNs) == 0 {
		panic("Direct read namespaces must be specified if change events are disabled")
	}
	if config.AWSConnect.enabled() {
		if err := config.AWSConnect.validate(); err != nil {
			panic(err)
		}
	}
	if config.MongoX509Settings.enabled() {
		if err := config.MongoX509Settings.validate(); err != nil {
			panic(err)
		}
	}
	ds := config.MongoDialSettings
	ss := config.MongoSessionSettings
	if ds.ReadTimeout < 1 {
		panic("MongoDB read timeout must be greater than 0")
	}
	if ds.WriteTimeout < 1 {
		panic("MongoDB write timeout must be greater than 0")
	}
	if ss.SyncTimeout < 1 {
		panic("MongoDB sync timeout must be greater than 0")
	}
	if len(config.DirectReadNs) > 0 {
		if config.ElasticMaxSeconds < 5 {
			warnLog.Println("Direct read performance degrades with small values for elasticsearch-max-seconds. Set to 5s or greater to remove this warning.")
		}
		if config.ElasticMaxDocs > 0 {
			warnLog.Println("For performance reasons it is recommended to use elasticsearch-max-bytes instead of elasticsearch-max-docs since doc size may vary")
		}
	}
}

func (config *configOptions) setDefaults() *configOptions {
	ds := config.MongoDialSettings
	ss := config.MongoSessionSettings
	if ds.Timeout == -1 {
		config.MongoDialSettings.Timeout = 15
	}
	if ds.ReadTimeout == -1 {
		config.MongoDialSettings.ReadTimeout = 30
	}
	if ds.WriteTimeout == -1 {
		config.MongoDialSettings.WriteTimeout = 30
	}
	if ss.SyncTimeout == -1 {
		config.MongoSessionSettings.SyncTimeout = 30
	}
	if ss.SocketTimeout == -1 {
		config.MongoSessionSettings.SocketTimeout = 0
	}
	if config.MongoURL == "" {
		config.MongoURL = mongoURLDefault
	}
	if config.ClusterName != "" {
		if config.Worker != "" {
			config.ResumeName = fmt.Sprintf("%s:%s", config.ClusterName, config.Worker)
		} else {
			config.ResumeName = config.ClusterName
		}
		config.Resume = true
	} else if config.Worker != "" {
		config.ResumeName = config.Worker
	} else if config.ResumeName == "" {
		config.ResumeName = resumeNameDefault
	}
	if config.ElasticMaxConns == 0 {
		config.ElasticMaxConns = elasticMaxConnsDefault
	}
	if config.ElasticClientTimeout == 0 {
		config.ElasticClientTimeout = elasticClientTimeoutDefault
	}
	if config.MergePatchAttr == "" {
		config.MergePatchAttr = "json-merge-patches"
	}
	if config.ElasticMaxSeconds == 0 {
		if len(config.DirectReadNs) > 0 {
			config.ElasticMaxSeconds = 5
		} else {
			config.ElasticMaxSeconds = 1
		}
	}
	if config.ElasticMaxDocs == 0 {
		config.ElasticMaxDocs = elasticMaxDocsDefault
	}
	if config.ElasticMaxBytes == 0 {
		config.ElasticMaxBytes = elasticMaxBytesDefault
	}
	if config.ElasticHealth0 == 0 {
		config.ElasticHealth0 = 15
	}
	if config.ElasticHealth1 == 0 {
		config.ElasticHealth1 = 5
	}
	if config.MongoURL != "" {
		config.MongoURL = config.parseMongoURL(config.MongoURL)
	}
	if config.MongoConfigURL != "" {
		config.MongoConfigURL = config.parseMongoURL(config.MongoConfigURL)
	}
	if config.HTTPServerAddr == "" {
		config.HTTPServerAddr = ":8080"
	}
	if config.StatsIndexFormat == "" {
		config.StatsIndexFormat = "monstache.stats.2006-01-02"
	}
	if config.TimeMachineIndexPrefix == "" {
		config.TimeMachineIndexPrefix = "log"
	}
	if config.TimeMachineIndexSuffix == "" {
		config.TimeMachineIndexSuffix = "2006-01-02"
	}
	if config.DeleteIndexPattern == "" {
		config.DeleteIndexPattern = "*"
	}
	if config.FileDownloaders == 0 && config.IndexFiles {
		config.FileDownloaders = fileDownloadersDefault
	}
	if config.RelateThreads == 0 {
		config.RelateThreads = relateThreadsDefault
	}
	if config.RelateBuffer == 0 {
		config.RelateBuffer = relateBufferDefault
	}
	if config.PostProcessors == 0 && processPlugin != nil {
		config.PostProcessors = postProcessorsDefault
	}
	if config.OplogTsFieldName == "" {
		config.OplogTsFieldName = "oplog_ts"
	}
	if config.OplogDateFieldName == "" {
		config.OplogDateFieldName = "oplog_date"
	}
	if config.OplogDateFieldFormat == "" {
		config.OplogDateFieldFormat = "2006/01/02 15:04:05"
	}
	if config.ConfigDatabaseName == "" {
		config.ConfigDatabaseName = configDatabaseNameDefault
	}
	if config.ResumeFromTimestamp > 0 {
		if config.ResumeFromTimestamp <= math.MaxInt32 {
			config.ResumeFromTimestamp = config.ResumeFromTimestamp << 32
		}
	}
	return config
}

func cleanMongoURL(inURL string) string {
	const scheme = "mongodb://"
	const schemeSrv = "mongodb+srv://"
	hasScheme := strings.HasPrefix(inURL, scheme)
	hasSchemeSrv := strings.HasPrefix(inURL, schemeSrv)
	url := strings.TrimPrefix(inURL, scheme)
	url = strings.TrimPrefix(url, schemeSrv)
	userEnd := strings.IndexAny(url, "@")
	if userEnd != -1 {
		url = redact + "@" + url[userEnd+1:]
	}
	if hasScheme {
		url = scheme + url
	} else if hasSchemeSrv {
		url = schemeSrv + url
	}
	return url
}

func (config *configOptions) readClientCert() (*tls.Certificate, error) {
	x509Settings := config.MongoX509Settings
	// Read in the PEM encoded X509 certificate.
	clientCertPEM, err := ioutil.ReadFile(x509Settings.ClientCertPemFile)
	if err != nil {
		return nil, err
	}
	// Read in the PEM encoded private key.
	clientKeyPEM, err := ioutil.ReadFile(x509Settings.ClientKeyPemFile)
	if err != nil {
		return nil, err
	}
	// Parse the private key, and the public key contained within the
	// certificate.
	clientCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		return nil, err
	}
	// Parse the actual certificate data
	clientCert.Leaf, err = x509.ParseCertificate(clientCert.Certificate[0])
	if err != nil {
		return nil, err
	}
	return &clientCert, nil
}

func (config *configOptions) timeoutConnection(inURL string, mongoOk chan bool) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer signal.Stop(sigs)
	deadline := time.Duration(config.MongoDialSettings.Timeout) * time.Second
	connT := time.NewTicker(deadline)
	defer connT.Stop()
	select {
	case <-mongoOk:
		return
	case <-sigs:
		os.Exit(exitStatus)
	case <-connT.C:
		errorLog.Fatalf("Unable to connect to MongoDB using URL %s: timed out after %d seconds",
			cleanMongoURL(inURL), config.MongoDialSettings.Timeout)
	}
}

func (config *configOptions) dialMongo(inURL string) (*mgo.Session, error) {
	var x509Cert *x509.Certificate
	dialInfo, err := mgo.ParseURL(inURL)
	if err != nil {
		return nil, err
	}
	if mongoDialInfo == nil {
		// save the initial dial info so that it can be reused
		// if connecting to shards
		mongoDialInfo = dialInfo.Copy()
	} else {
		// copy the initial auth info when connecting to shards
		dialInfo.Username = mongoDialInfo.Username
		dialInfo.Password = mongoDialInfo.Password
		dialInfo.Source = mongoDialInfo.Source
		dialInfo.Mechanism = mongoDialInfo.Mechanism
	}
	dialInfo.AppName = "monstache"
	dialInfo.Timeout = time.Duration(0)
	dialInfo.ReadTimeout = time.Duration(config.MongoDialSettings.ReadTimeout) * time.Second
	dialInfo.WriteTimeout = time.Duration(config.MongoDialSettings.WriteTimeout) * time.Second
	ssl := config.MongoDialSettings.Ssl || config.MongoPemFile != ""
	if ssl {
		tlsConfig := &tls.Config{}
		// Check to see if we don't need to validate the PEM
		if config.MongoValidatePemFile == false {
			// Turn off validation
			tlsConfig.InsecureSkipVerify = true
		}
		certs := x509.NewCertPool()
		if config.MongoPemFile != "" {
			if ca, err := ioutil.ReadFile(config.MongoPemFile); err == nil {
				if ok := certs.AppendCertsFromPEM(ca); !ok {
					errorLog.Printf("No certs parsed successfully from %s", config.MongoPemFile)
				}
			} else {
				return nil, err
			}
			tlsConfig.RootCAs = certs
		}
		if config.MongoX509Settings.enabled() {
			clientCert, err := config.readClientCert()
			if err != nil {
				return nil, err
			}
			x509Cert = clientCert.Leaf
			if tlsConfig.InsecureSkipVerify {
				certs.AddCert(x509Cert)
				tlsConfig.RootCAs = certs
			} else {
				tlsConfig.Certificates = []tls.Certificate{*clientCert}
			}
		}
		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
			if err != nil {
				errorLog.Printf("Unable to dial MongoDB: %s", err)
			}
			return conn, err
		}
	}
	mongoOk := make(chan bool)
	if config.MongoDialSettings.Timeout != 0 {
		go config.timeoutConnection(inURL, mongoOk)
	}
	session, err := mgo.DialWithInfo(dialInfo)
	close(mongoOk)
	if err == nil {
		session.SetSyncTimeout(time.Duration(config.MongoSessionSettings.SyncTimeout) * time.Second)
	}
	if x509Cert != nil {
		cred := &mgo.Credential{Mechanism: "MONGODB-X509", Certificate: x509Cert}
		if err := session.Login(cred); err != nil {
			return nil, err
		}
	}
	return session, err
}

func (config *configOptions) NewHTTPClient() (client *http.Client, err error) {
	tlsConfig := &tls.Config{}
	if config.ElasticPemFile != "" {
		var ca []byte
		certs := x509.NewCertPool()
		if ca, err = ioutil.ReadFile(config.ElasticPemFile); err == nil {
			if ok := certs.AppendCertsFromPEM(ca); !ok {
				errorLog.Printf("No certs parsed successfully from %s", config.ElasticPemFile)
			}
			tlsConfig.RootCAs = certs
		} else {
			return client, err
		}
	}
	if config.ElasticValidatePemFile == false {
		// Turn off validation
		tlsConfig.InsecureSkipVerify = true
	}
	transport := &http.Transport{
		DisableCompression:  !config.Gzip,
		TLSHandshakeTimeout: time.Duration(30) * time.Second,
		TLSClientConfig:     tlsConfig,
	}
	client = &http.Client{
		Timeout:   time.Duration(config.ElasticClientTimeout) * time.Second,
		Transport: transport,
	}
	if config.AWSConnect.enabled() {
		client = aws.NewV4SigningClientWithHTTPClient(credentials.NewStaticCredentials(
			config.AWSConnect.AccessKey,
			config.AWSConnect.SecretKey,
			"",
		), config.AWSConnect.Region, client)
	}
	return client, err
}

func doDrop(mongo *mgo.Session, elastic *elastic.Client, op *gtm.Op, config *configOptions) (err error) {
	if db, drop := op.IsDropDatabase(); drop {
		if config.DroppedDatabases {
			if err = deleteIndexes(elastic, db, config); err == nil {
				if e := dropDBMeta(mongo, db, config); e != nil {
					errorLog.Printf("Unable to delete metadata for db: %s", e)
				}
			}
		}
	} else if col, drop := op.IsDropCollection(); drop {
		if config.DroppedCollections {
			if err = deleteIndex(elastic, op.GetDatabase()+"."+col, config); err == nil {
				if e := dropCollectionMeta(mongo, op.GetDatabase()+"."+col, config); e != nil {
					errorLog.Printf("Unable to delete metadata for collection: %s", e)
				}
			}
		}
	}
	return
}

func hasFileContent(op *gtm.Op, config *configOptions) (ingest bool) {
	if !config.IndexFiles {
		return
	}
	return fileNamespaces[op.Namespace]
}

func addPatch(config *configOptions, client *elastic.Client, op *gtm.Op,
	objectID string, indexType *indexTypeMapping, meta *indexingMeta) (err error) {
	var merges []interface{}
	var toJSON []byte
	if op.IsSourceDirect() {
		return nil
	}
	if op.Timestamp == 0 {
		return nil
	}
	if op.IsUpdate() {
		ctx := context.Background()
		service := client.Get()
		service.Id(objectID)
		service.Index(indexType.Index)
		service.Type(indexType.Type)
		if meta.ID != "" {
			service.Id(meta.ID)
		}
		if meta.Index != "" {
			service.Index(meta.Index)
		}
		if meta.Type != "" {
			service.Type(meta.Type)
		}
		if meta.Routing != "" {
			service.Routing(meta.Routing)
		}
		if meta.Parent != "" {
			service.Parent(meta.Parent)
		}
		var resp *elastic.GetResult
		if resp, err = service.Do(ctx); err == nil {
			if resp.Found {
				var src map[string]interface{}
				if err = json.Unmarshal(*resp.Source, &src); err == nil {
					if val, ok := src[config.MergePatchAttr]; ok {
						merges = val.([]interface{})
						for _, m := range merges {
							entry := m.(map[string]interface{})
							entry["ts"] = int(entry["ts"].(float64))
							entry["v"] = int(entry["v"].(float64))
						}
					}
					delete(src, config.MergePatchAttr)
					var fromJSON, mergeDoc []byte
					if fromJSON, err = json.Marshal(src); err == nil {
						if toJSON, err = json.Marshal(op.Data); err == nil {
							if mergeDoc, err = jsonpatch.CreateMergePatch(fromJSON, toJSON); err == nil {
								merge := make(map[string]interface{})
								merge["ts"] = op.Timestamp >> 32
								merge["p"] = string(mergeDoc)
								merge["v"] = len(merges) + 1
								merges = append(merges, merge)
								op.Data[config.MergePatchAttr] = merges
							}
						}
					}
				}
			} else {
				err = errors.New("Last document revision not found")
			}

		}
	} else {
		if _, found := op.Data[config.MergePatchAttr]; !found {
			if toJSON, err = json.Marshal(op.Data); err == nil {
				merge := make(map[string]interface{})
				merge["v"] = 1
				merge["ts"] = op.Timestamp >> 32
				merge["p"] = string(toJSON)
				merges = append(merges, merge)
				op.Data[config.MergePatchAttr] = merges
			}
		}
	}
	return
}

func doIndexing(config *configOptions, mongo *mgo.Session, bulk *elastic.BulkProcessor, client *elastic.Client, op *gtm.Op) (err error) {
	meta := parseIndexMeta(op)
	if meta.Skip {
		return
	}
	prepareDataForIndexing(config, op)
	objectID, indexType := opIDToString(op), mapIndexType(config, op)
	if config.EnablePatches {
		if patchNamespaces[op.Namespace] {
			if e := addPatch(config, client, op, objectID, indexType, meta); e != nil {
				errorLog.Printf("Unable to save json-patch info: %s", e)
			}
		}
	}
	ingestAttachment := false
	if hasFileContent(op, config) {
		ingestAttachment = op.Data["file"] != nil
	}
	if config.IndexAsUpdate && meta.Pipeline == "" && ingestAttachment == false {
		req := elastic.NewBulkUpdateRequest()
		req.UseEasyJSON(config.EnableEasyJSON)
		req.Id(objectID)
		req.Index(indexType.Index)
		req.Type(indexType.Type)
		req.Doc(op.Data)
		req.DocAsUpsert(true)
		if meta.ID != "" {
			req.Id(meta.ID)
		}
		if meta.Index != "" {
			req.Index(meta.Index)
		}
		if meta.Type != "" {
			req.Type(meta.Type)
		}
		if meta.Routing != "" {
			req.Routing(meta.Routing)
		}
		if meta.Parent != "" {
			req.Parent(meta.Parent)
		}
		if meta.RetryOnConflict != 0 {
			req.RetryOnConflict(meta.RetryOnConflict)
		}
		if _, err = req.Source(); err == nil {
			bulk.Add(req)
		}
	} else {
		req := elastic.NewBulkIndexRequest()
		req.UseEasyJSON(config.EnableEasyJSON)
		req.Id(objectID)
		req.Index(indexType.Index)
		req.Type(indexType.Type)
		req.Doc(op.Data)
		if meta.ID != "" {
			req.Id(meta.ID)
		}
		if meta.Index != "" {
			req.Index(meta.Index)
		}
		if meta.Type != "" {
			req.Type(meta.Type)
		}
		if meta.Routing != "" {
			req.Routing(meta.Routing)
		}
		if meta.Parent != "" {
			req.Parent(meta.Parent)
		}
		if meta.Version != 0 {
			req.Version(meta.Version)
		}
		if meta.VersionType != "" {
			req.VersionType(meta.VersionType)
		}
		if meta.Pipeline != "" {
			req.Pipeline(meta.Pipeline)
		}
		if meta.RetryOnConflict != 0 {
			req.RetryOnConflict(meta.RetryOnConflict)
		}
		if ingestAttachment {
			req.Pipeline("attachment")
		}
		if _, err = req.Source(); err == nil {
			bulk.Add(req)
		}
	}

	if meta.shouldSave(config) {
		if e := setIndexMeta(mongo, op.Namespace, objectID, meta, config); e != nil {
			errorLog.Printf("Unable to save routing info: %s", e)
		}
	}

	if tmNamespaces[op.Namespace] {
		if op.IsSourceOplog() || config.TimeMachineDirectReads {
			t := time.Now().UTC()
			tmIndex := func(idx string) string {
				pre, suf := config.TimeMachineIndexPrefix, config.TimeMachineIndexSuffix
				tmFormat := strings.Join([]string{pre, idx, t.Format(suf)}, ".")
				return strings.ToLower(tmFormat)
			}
			data := make(map[string]interface{})
			for k, v := range op.Data {
				data[k] = v
			}
			data["_source_id"] = objectID
			if config.IndexOplogTime == false {
				secs := int64(op.Timestamp >> 32)
				t := time.Unix(secs, 0).UTC()
				data[config.OplogTsFieldName] = op.Timestamp
				data[config.OplogDateFieldName] = t.Format(config.OplogDateFieldFormat)
			}
			req := elastic.NewBulkIndexRequest()
			req.UseEasyJSON(config.EnableEasyJSON)
			req.Index(tmIndex(indexType.Index))
			req.Type(indexType.Type)
			req.Routing(objectID)
			req.Doc(data)
			if meta.Index != "" {
				req.Index(tmIndex(meta.Index))
			}
			if meta.Type != "" {
				req.Type(meta.Type)
			}
			if meta.Pipeline != "" {
				req.Pipeline(meta.Pipeline)
			}
			if ingestAttachment {
				req.Pipeline("attachment")
			}
			if _, err = req.Source(); err == nil {
				bulk.Add(req)
			}
		}
	}
	return
}

func doIndex(config *configOptions, mongo *mgo.Session, bulk *elastic.BulkProcessor, client *elastic.Client, op *gtm.Op) (err error) {
	if err = mapData(mongo, config, op); err == nil {
		if op.Data != nil {
			err = doIndexing(config, mongo, bulk, client, op)
		} else if op.IsUpdate() {
			doDelete(config, client, mongo, bulk, op)
		}
	}
	return
}

func runProcessor(mongo *mgo.Session, bulk *elastic.BulkProcessor, client *elastic.Client, op *gtm.Op) (err error) {
	session := mongo.Copy()
	defer session.Close()
	input := &monstachemap.ProcessPluginInput{
		ElasticClient:        client,
		ElasticBulkProcessor: bulk,
		Timestamp:            op.Timestamp,
	}
	input.Document = op.Data
	if op.IsDelete() {
		input.Document = map[string]interface{}{
			"_id": op.Id,
		}
	}
	input.Namespace = op.Namespace
	input.Database = op.GetDatabase()
	input.Collection = op.GetCollection()
	input.Operation = op.Operation
	input.Session = session
	input.UpdateDescription = op.UpdateDescription
	err = processPlugin(input)
	return
}

func routeOp(config *configOptions, mongo *mgo.Session, bulk *elastic.BulkProcessor, client *elastic.Client, op *gtm.Op, out *outputChans) (err error) {
	if processPlugin != nil {
		rop := &gtm.Op{
			Id:                op.Id,
			Operation:         op.Operation,
			Namespace:         op.Namespace,
			Source:            op.Source,
			Timestamp:         op.Timestamp,
			UpdateDescription: op.UpdateDescription,
		}
		if op.Data != nil {
			var data []byte
			data, err = bson.Marshal(op.Data)
			if err == nil {
				var m map[string]interface{}
				err = bson.Unmarshal(data, &m)
				if err == nil {
					rop.Data = m
				}
			}
		}
		out.processC <- rop
	}
	if op.IsDrop() {
		bulk.Flush()
		err = doDrop(mongo, client, op, config)
	} else if op.IsDelete() {
		if len(config.Relate) > 0 {
			if rs := relates[op.Namespace]; len(rs) != 0 {
				var delData map[string]interface{}
				useFind := false
				for _, r := range rs {
					if r.SrcField != "_id" {
						useFind = true
						break
					}
				}
				if useFind {
					delData = findDeletedSrcDoc(config, client, op)
				} else {
					delData = map[string]interface{}{
						"_id": op.Id,
					}
				}
				if delData != nil {
					rop := &gtm.Op{
						Id:        op.Id,
						Operation: op.Operation,
						Namespace: op.Namespace,
						Source:    op.Source,
						Timestamp: op.Timestamp,
						Data:      delData,
					}
					select {
					case out.relateC <- rop:
					default:
						errorLog.Printf(relateQueueOverloadMsg, rop.Namespace, rop.Id)
					}
				}
			}
		}
		doDelete(config, client, mongo, bulk, op)
	} else if op.Data != nil {
		skip := false
		if op.IsSourceOplog() && len(config.Relate) > 0 {
			if rs := relates[op.Namespace]; len(rs) != 0 {
				skip = true
				for _, r := range rs {
					if r.KeepSrc {
						skip = false
						break
					}
				}
				if skip {
					select {
					case out.relateC <- op:
					default:
						errorLog.Printf(relateQueueOverloadMsg, op.Namespace, op.Id)
					}
				} else {
					rop := &gtm.Op{
						Id:                op.Id,
						Operation:         op.Operation,
						Namespace:         op.Namespace,
						Source:            op.Source,
						Timestamp:         op.Timestamp,
						UpdateDescription: op.UpdateDescription,
					}
					var data []byte
					data, err = bson.Marshal(op.Data)
					if err == nil {
						var m map[string]interface{}
						err = bson.Unmarshal(data, &m)
						if err == nil {
							rop.Data = m
						}
					}
					select {
					case out.relateC <- rop:
					default:
						errorLog.Printf(relateQueueOverloadMsg, rop.Namespace, rop.Id)
					}
				}
			}
		}
		if !skip {
			if hasFileContent(op, config) {
				out.fileC <- op
			} else {
				out.indexC <- op
			}
		}
	}
	return
}

func processErr(err error, config *configOptions) {
	mux.Lock()
	defer mux.Unlock()
	exitStatus = 1
	errorLog.Println(err)
	if config.FailFast {
		os.Exit(exitStatus)
	}
}

func doIndexStats(config *configOptions, bulkStats *elastic.BulkProcessor, stats elastic.BulkProcessorStats) (err error) {
	var hostname string
	doc := make(map[string]interface{})
	t := time.Now().UTC()
	doc["Timestamp"] = t.Format("2006-01-02T15:04:05")
	hostname, err = os.Hostname()
	if err == nil {
		doc["Host"] = hostname
	}
	doc["Pid"] = os.Getpid()
	doc["Stats"] = stats
	index := strings.ToLower(t.Format(config.StatsIndexFormat))
	typeName := "stats"
	if config.useTypeFromFuture() {
		typeName = typeFromFuture
	}
	req := elastic.NewBulkIndexRequest().Index(index).Type(typeName)
	req.UseEasyJSON(config.EnableEasyJSON)
	req.Doc(doc)
	bulkStats.Add(req)
	return
}

func dropDBMeta(session *mgo.Session, db string, config *configOptions) (err error) {
	if config.DeleteStrategy == statefulDeleteStrategy {
		col := session.DB(config.ConfigDatabaseName).C("meta")
		q := bson.M{"db": db}
		_, err = col.RemoveAll(q)
	}
	return
}

func dropCollectionMeta(session *mgo.Session, namespace string, config *configOptions) (err error) {
	if config.DeleteStrategy == statefulDeleteStrategy {
		col := session.DB(config.ConfigDatabaseName).C("meta")
		q := bson.M{"namespace": namespace}
		_, err = col.RemoveAll(q)
	}
	return
}

func (meta *indexingMeta) load(metaAttrs map[string]interface{}) {
	var v interface{}
	var ok bool
	var s string
	if _, ok = metaAttrs["skip"]; ok {
		meta.Skip = true
	}
	if v, ok = metaAttrs["routing"]; ok {
		meta.Routing = fmt.Sprintf("%v", v)
	}
	if v, ok = metaAttrs["index"]; ok {
		meta.Index = fmt.Sprintf("%v", v)
	}
	if v, ok = metaAttrs["id"]; ok {
		op := &gtm.Op{
			Id: v,
		}
		meta.ID = opIDToString(op)
	}
	if v, ok = metaAttrs["type"]; ok {
		meta.Type = fmt.Sprintf("%v", v)
	}
	if v, ok = metaAttrs["parent"]; ok {
		meta.Parent = fmt.Sprintf("%v", v)
	}
	if v, ok = metaAttrs["version"]; ok {
		s = fmt.Sprintf("%v", v)
		if version, err := strconv.ParseInt(s, 10, 64); err == nil {
			meta.Version = version
		}
	}
	if v, ok = metaAttrs["versionType"]; ok {
		meta.VersionType = fmt.Sprintf("%v", v)
	}
	if v, ok = metaAttrs["pipeline"]; ok {
		meta.Pipeline = fmt.Sprintf("%v", v)
	}
	if v, ok = metaAttrs["retryOnConflict"]; ok {
		s = fmt.Sprintf("%v", v)
		if roc, err := strconv.Atoi(s); err == nil {
			meta.RetryOnConflict = roc
		}
	}
}

func (meta *indexingMeta) shouldSave(config *configOptions) bool {
	if config.DeleteStrategy == statefulDeleteStrategy {
		return (meta.Routing != "" ||
			meta.Index != "" ||
			meta.Type != "" ||
			meta.Parent != "" ||
			meta.Pipeline != "")
	}
	return false
}

func setIndexMeta(session *mgo.Session, namespace, id string, meta *indexingMeta, config *configOptions) error {
	col := session.DB(config.ConfigDatabaseName).C("meta")
	metaID := fmt.Sprintf("%s.%s", namespace, id)
	doc := make(map[string]interface{})
	doc["id"] = meta.ID
	doc["routing"] = meta.Routing
	doc["index"] = meta.Index
	doc["type"] = meta.Type
	doc["parent"] = meta.Parent
	doc["pipeline"] = meta.Pipeline
	doc["db"] = strings.SplitN(namespace, ".", 2)[0]
	doc["namespace"] = namespace
	_, err := col.UpsertId(metaID, bson.M{"$set": doc})
	return err
}

func getIndexMeta(session *mgo.Session, namespace, id string, config *configOptions) (meta *indexingMeta) {
	meta = &indexingMeta{}
	col := session.DB(config.ConfigDatabaseName).C("meta")
	doc := make(map[string]interface{})
	metaID := fmt.Sprintf("%s.%s", namespace, id)
	col.FindId(metaID).One(doc)
	if doc["id"] != nil {
		meta.ID = doc["id"].(string)
	}
	if doc["routing"] != nil {
		meta.Routing = doc["routing"].(string)
	}
	if doc["index"] != nil {
		meta.Index = strings.ToLower(doc["index"].(string))
	}
	if doc["type"] != nil {
		meta.Type = doc["type"].(string)
	}
	if doc["parent"] != nil {
		meta.Parent = doc["parent"].(string)
	}
	if doc["pipeline"] != nil {
		meta.Pipeline = doc["pipeline"].(string)
	}
	col.RemoveId(metaID)
	return
}

func loadBuiltinFunctions(s *mgo.Session, config *configOptions) {
	for ns, env := range mapEnvs {
		var fa *findConf
		fa = &findConf{
			session: s,
			name:    "findId",
			vm:      env.VM,
			ns:      ns,
			byId:    true,
		}
		if err := env.VM.Set(fa.name, makeFind(fa)); err != nil {
			panic(err)
		}
		fa = &findConf{
			session: s,
			name:    "findOne",
			vm:      env.VM,
			ns:      ns,
		}
		if err := env.VM.Set(fa.name, makeFind(fa)); err != nil {
			panic(err)
		}
		fa = &findConf{
			session: s,
			name:    "find",
			vm:      env.VM,
			ns:      ns,
			multi:   true,
		}
		if err := env.VM.Set(fa.name, makeFind(fa)); err != nil {
			panic(err)
		}
		fa = &findConf{
			session:       s,
			name:          "pipe",
			vm:            env.VM,
			ns:            ns,
			multi:         true,
			pipe:          true,
			pipeAllowDisk: config.PipeAllowDisk,
		}
		if err := env.VM.Set(fa.name, makeFind(fa)); err != nil {
			panic(err)
		}
	}
}

func (fc *findCall) setDatabase(topts map[string]interface{}) (err error) {
	if ov, ok := topts["database"]; ok {
		if ovs, ok := ov.(string); ok {
			fc.db = ovs
		} else {
			err = errors.New("Invalid database option value")
		}
	}
	return
}

func (fc *findCall) setCollection(topts map[string]interface{}) (err error) {
	if ov, ok := topts["collection"]; ok {
		if ovs, ok := ov.(string); ok {
			fc.col = ovs
		} else {
			err = errors.New("Invalid collection option value")
		}
	}
	return
}

func (fc *findCall) setSelect(topts map[string]interface{}) (err error) {
	if ov, ok := topts["select"]; ok {
		if ovsel, ok := ov.(map[string]interface{}); ok {
			for k, v := range ovsel {
				if vi, ok := v.(int64); ok {
					fc.sel[k] = int(vi)
				}
			}
		} else {
			err = errors.New("Invalid select option value")
		}
	}
	return
}

func (fc *findCall) setSort(topts map[string]interface{}) (err error) {
	if ov, ok := topts["sort"]; ok {
		if ovs, ok := ov.([]string); ok {
			fc.sort = ovs
		} else {
			err = errors.New("Invalid sort option value")
		}
	}
	return
}

func (fc *findCall) setLimit(topts map[string]interface{}) (err error) {
	if ov, ok := topts["limit"]; ok {
		if ovl, ok := ov.(int64); ok {
			fc.limit = int(ovl)
		} else {
			err = errors.New("Invalid limit option value")
		}
	}
	return
}

func (fc *findCall) setQuery(v otto.Value) (err error) {
	var q interface{}
	if q, err = v.Export(); err == nil {
		fc.query = fc.restoreIds(deepExportValue(q))
	}
	return
}

func (fc *findCall) setOptions(v otto.Value) (err error) {
	var opts interface{}
	if opts, err = v.Export(); err == nil {
		switch topts := opts.(type) {
		case map[string]interface{}:
			if err = fc.setDatabase(topts); err != nil {
				return
			}
			if err = fc.setCollection(topts); err != nil {
				return
			}
			if err = fc.setSelect(topts); err != nil {
				return
			}
			if fc.isMulti() {
				if err = fc.setSort(topts); err != nil {
					return
				}
				if err = fc.setLimit(topts); err != nil {
					return
				}
			}
		default:
			err = errors.New("Invalid options argument")
			return
		}
	} else {
		err = errors.New("Invalid options argument")
	}
	return
}

func (fc *findCall) setDefaults() {
	if fc.config.ns != "" {
		ns := strings.SplitN(fc.config.ns, ".", 2)
		fc.db = ns[0]
		fc.col = ns[1]
	}
}

func (fc *findCall) getCollection() *mgo.Collection {
	return fc.session.DB(fc.db).C(fc.col)
}

func (fc *findCall) getVM() *otto.Otto {
	return fc.config.vm
}

func (fc *findCall) getFunctionName() string {
	return fc.config.name
}

func (fc *findCall) isMulti() bool {
	return fc.config.multi
}

func (fc *findCall) isPipe() bool {
	return fc.config.pipe
}

func (fc *findCall) pipeAllowDisk() bool {
	return fc.config.pipeAllowDisk
}

func (fc *findCall) logError(err error) {
	errorLog.Printf("Error in function %s: %s\n", fc.getFunctionName(), err)
}

func (fc *findCall) restoreIds(v interface{}) (r interface{}) {
	switch vt := v.(type) {
	case string:
		if bson.IsObjectIdHex(vt) {
			r = bson.ObjectIdHex(vt)
		} else {
			r = v
		}
	case []map[string]interface{}:
		var avs []interface{}
		for _, av := range vt {
			mvs := make(map[string]interface{})
			for k, v := range av {
				mvs[k] = fc.restoreIds(v)
			}
			avs = append(avs, mvs)
		}
		r = avs
	case []interface{}:
		var avs []interface{}
		for _, av := range vt {
			avs = append(avs, fc.restoreIds(av))
		}
		r = avs
	case map[string]interface{}:
		mvs := make(map[string]interface{})
		for k, v := range vt {
			mvs[k] = fc.restoreIds(v)
		}
		r = mvs
	default:
		r = v
	}
	return
}

func (fc *findCall) execute() (r otto.Value, err error) {
	var q *mgo.Query
	col := fc.getCollection()
	if fc.isMulti() {
		if fc.isPipe() {
			pipe := col.Pipe(fc.query)
			if fc.pipeAllowDisk() {
				pipe = pipe.AllowDiskUse()
			}
			var docs []map[string]interface{}
			if err = pipe.All(&docs); err == nil {
				var rdocs []map[string]interface{}
				for _, doc := range docs {
					rdocs = append(rdocs, convertMapJavascript(doc))
				}
				r, err = fc.getVM().ToValue(rdocs)
			}
		} else {
			q = col.Find(fc.query)
			if fc.limit > 0 {
				q.Limit(fc.limit)
			}
			if len(fc.sort) > 0 {
				q.Sort(fc.sort...)
			}
			if len(fc.sel) > 0 {
				q.Select(fc.sel)
			}
			var docs []map[string]interface{}
			if err = q.All(&docs); err == nil {
				var rdocs []map[string]interface{}
				for _, doc := range docs {
					rdocs = append(rdocs, convertMapJavascript(doc))
				}
				r, err = fc.getVM().ToValue(rdocs)
			}
		}
	} else {
		if fc.config.byId {
			q = col.FindId(fc.query)
		} else {
			q = col.Find(fc.query)
		}
		if len(fc.sel) > 0 {
			q.Select(fc.sel)
		}
		doc := make(map[string]interface{})
		if err = q.One(doc); err == nil {
			rdoc := convertMapJavascript(doc)
			r, err = fc.getVM().ToValue(rdoc)
		}
	}
	return
}

func makeFind(fa *findConf) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) (r otto.Value) {
		var err error
		fc := &findCall{
			config:  fa,
			session: fa.session.Copy(),
			sel:     make(map[string]int),
		}
		defer fc.session.Close()
		fc.setDefaults()
		args := call.ArgumentList
		argLen := len(args)
		r = otto.NullValue()
		if argLen >= 1 {
			if argLen >= 2 {
				if err = fc.setOptions(call.Argument(1)); err != nil {
					fc.logError(err)
					return
				}
			}
			if fc.db == "" || fc.col == "" {
				fc.logError(errors.New("Find call must specify db and collection"))
				return
			}
			if err = fc.setQuery(call.Argument(0)); err == nil {
				var result otto.Value
				if result, err = fc.execute(); err == nil {
					r = result
				} else if err != mgo.ErrNotFound {
					fc.logError(err)
				}
			} else {
				fc.logError(err)
			}
		} else {
			fc.logError(errors.New("At least one argument is required"))
		}
		return
	}
}

func findDeletedSrcDoc(config *configOptions, client *elastic.Client, op *gtm.Op) map[string]interface{} {
	objectID := opIDToString(op)
	termQuery := elastic.NewTermQuery("_id", objectID)
	search := client.Search()
	searchResult, err := search.Size(1).Index(config.DeleteIndexPattern).Query(termQuery).Do(context.Background())
	if err != nil {
		errorLog.Printf("Unable to find deleted document %s: %s", objectID, err)
		return nil
	}
	if searchResult.Hits == nil {
		errorLog.Printf("Unable to find deleted document %s", objectID)
		return nil
	}
	if searchResult.Hits.TotalHits == 0 {
		errorLog.Printf("Found no hits for deleted document %s", objectID)
		return nil
	}
	if searchResult.Hits.TotalHits > 1 {
		errorLog.Printf("Found multiple hits for deleted document %s", objectID)
		return nil
	}
	hit := searchResult.Hits.Hits[0]
	if hit.Source == nil {
		errorLog.Printf("Source unavailable for deleted document %s", objectID)
		return nil
	}
	var src map[string]interface{}
	if err = json.Unmarshal(*hit.Source, &src); err == nil {
		src["_id"] = op.Id
		return src
	}
	errorLog.Printf("Unable to unmarshal deleted document %s: %s", objectID, err)
	return nil
}

func doDelete(config *configOptions, client *elastic.Client, mongo *mgo.Session, bulk *elastic.BulkProcessor, op *gtm.Op) {
	req := elastic.NewBulkDeleteRequest()
	req.UseEasyJSON(config.EnableEasyJSON)
	if config.DeleteStrategy == ignoreDeleteStrategy {
		return
	}
	objectID, indexType, meta := opIDToString(op), mapIndexType(config, op), &indexingMeta{}
	req.Id(objectID)
	if config.IndexAsUpdate == false {
		req.Version(int64(op.Timestamp))
		req.VersionType("external")
	}
	if config.DeleteStrategy == statefulDeleteStrategy {
		if routingNamespaces[""] || routingNamespaces[op.Namespace] {
			meta = getIndexMeta(mongo, op.Namespace, objectID, config)
		}
		req.Index(indexType.Index)
		req.Type(indexType.Type)
		if meta.Index != "" {
			req.Index(meta.Index)
		}
		if meta.Type != "" {
			req.Type(meta.Type)
		}
		if meta.Routing != "" {
			req.Routing(meta.Routing)
		}
		if meta.Parent != "" {
			req.Parent(meta.Parent)
		}
	} else if config.DeleteStrategy == statelessDeleteStrategy {
		if routingNamespaces[""] || routingNamespaces[op.Namespace] {
			termQuery := elastic.NewTermQuery("_id", objectID)
			searchResult, err := client.Search().FetchSource(false).Size(1).Index(config.DeleteIndexPattern).Query(termQuery).Do(context.Background())
			if err != nil {
				errorLog.Printf("Unable to delete document %s: %s", objectID, err)
				return
			}
			if searchResult.Hits != nil && searchResult.Hits.TotalHits == 1 {
				hit := searchResult.Hits.Hits[0]
				req.Index(hit.Index)
				req.Type(hit.Type)
				if hit.Routing != "" {
					req.Routing(hit.Routing)
				}
				if hit.Parent != "" {
					req.Parent(hit.Parent)
				}
			} else {
				errorLog.Printf("Failed to find unique document %s for deletion using index pattern %s", objectID, config.DeleteIndexPattern)
				return
			}
		} else {
			req.Index(indexType.Index)
			req.Type(indexType.Type)
		}
	} else {
		return
	}
	bulk.Add(req)
	return
}

func gtmDefaultSettings() gtmSettings {
	return gtmSettings{
		ChannelSize:    gtmChannelSizeDefault,
		BufferSize:     32,
		BufferDuration: "75ms",
	}
}

func notifySdFailed(config *configOptions, err error) {
	if err != nil {
		errorLog.Printf("Systemd notification failed: %s", err)
	} else {
		if config.Verbose {
			warnLog.Println("Systemd notification not supported (i.e. NOTIFY_SOCKET is unset)")
		}
	}
}

func watchdogSdFailed(config *configOptions, err error) {
	if err != nil {
		errorLog.Printf("Error determining systemd WATCHDOG interval: %s", err)
	} else {
		if config.Verbose {
			warnLog.Println("Systemd WATCHDOG not enabled")
		}
	}
}

func (ctx *httpServerCtx) serveHttp() {
	s := ctx.httpServer
	if ctx.config.Verbose {
		infoLog.Printf("Starting http server at %s", s.Addr)
	}
	ctx.started = time.Now()
	err := s.ListenAndServe()
	if !ctx.shutdown {
		panic(fmt.Sprintf("Unable to serve http at address %s: %s", s.Addr, err))
	}
}

func (ctx *httpServerCtx) buildServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/started", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		data := (time.Now().Sub(ctx.started)).String()
		w.Write([]byte(data))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})
	if ctx.config.Stats {
		mux.HandleFunc("/stats", func(w http.ResponseWriter, req *http.Request) {
			stats, err := json.MarshalIndent(ctx.bulk.Stats(), "", "    ")
			if err == nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				w.Write(stats)
			} else {
				w.WriteHeader(500)
				fmt.Fprintf(w, "Unable to print statistics: %s", err)
			}
		})
	}
	mux.HandleFunc("/instance", func(w http.ResponseWriter, req *http.Request) {
		hostname, err := os.Hostname()
		if err != nil {
			w.WriteHeader(500)
			fmt.Fprintf(w, "Unable to get hostname for instance info: %s", err)
			return
		}
		status := instanceStatus{
			Pid:         os.Getpid(),
			Hostname:    hostname,
			ResumeName:  ctx.config.ResumeName,
			ClusterName: ctx.config.ClusterName,
		}
		respC := make(chan *statusResponse)
		statusReq := &statusRequest{
			responseC: respC,
		}
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case statusReqC <- statusReq:
			srsp := <-respC
			if srsp != nil {
				status.Enabled = srsp.enabled
				status.LastTs = srsp.lastTs
				if srsp.lastTs != 0 {
					status.LastTsFormat = time.Unix(int64(srsp.lastTs>>32), 0).Format("2006-01-02T15:04:05")
				}
			}
			data, err := json.Marshal(status)
			if err != nil {
				w.WriteHeader(500)
				fmt.Fprintf(w, "Unable to print instance info: %s", err)
				break
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			w.Write(data)
			fmt.Fprintln(w)
			break
		case <-timer.C:
			w.WriteHeader(500)
			fmt.Fprintf(w, "Timeout getting instance info")
			break
		}
	})
	if ctx.config.Pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	s := &http.Server{
		Addr:     ctx.config.HTTPServerAddr,
		Handler:  mux,
		ErrorLog: errorLog,
	}
	ctx.httpServer = s
}

func notifySd(config *configOptions) {
	var interval time.Duration
	if config.Verbose {
		infoLog.Println("Sending systemd READY=1")
	}
	sent, err := daemon.SdNotify(false, "READY=1")
	if sent {
		if config.Verbose {
			infoLog.Println("READY=1 successfully sent to systemd")
		}
	} else {
		notifySdFailed(config, err)
		return
	}
	interval, err = daemon.SdWatchdogEnabled(false)
	if err != nil || interval == 0 {
		watchdogSdFailed(config, err)
		return
	}
	for {
		if config.Verbose {
			infoLog.Println("Sending systemd WATCHDOG=1")
		}
		sent, err = daemon.SdNotify(false, "WATCHDOG=1")
		if sent {
			if config.Verbose {
				infoLog.Println("WATCHDOG=1 successfully sent to systemd")
			}
		} else {
			notifySdFailed(config, err)
			return
		}
		time.Sleep(interval / 2)
	}
}

func (config *configOptions) makeShardInsertHandler() gtm.ShardInsertHandler {
	return func(shardInfo *gtm.ShardInfo) (*mgo.Session, error) {
		shardURL := shardInfo.GetURL()
		infoLog.Printf("Adding shard found at %s\n", cleanMongoURL(shardURL))
		return config.dialMongo(shardURL)
	}
}

func buildPipe(config *configOptions) func(string, bool) ([]interface{}, error) {
	if pipePlugin != nil {
		return pipePlugin
	} else if len(pipeEnvs) > 0 {
		return func(ns string, changeEvent bool) ([]interface{}, error) {
			mux.Lock()
			defer mux.Unlock()
			nss := []string{"", ns}
			for _, ns := range nss {
				if env := pipeEnvs[ns]; env != nil {
					env.lock.Lock()
					defer env.lock.Unlock()
					val, err := env.VM.Call("module.exports", ns, ns, changeEvent)
					if err != nil {
						return nil, err
					}
					if strings.ToLower(val.Class()) == "array" {
						data, err := val.Export()
						if err != nil {
							return nil, err
						} else if data == val {
							return nil, errors.New("Exported pipeline function must return an array")
						} else {
							switch data.(type) {
							case []map[string]interface{}:
								ds := data.([]map[string]interface{})
								var is []interface{} = make([]interface{}, len(ds))
								for i, d := range ds {
									is[i] = deepExportValue(d)
								}
								return is, nil
							case []interface{}:
								ds := data.([]interface{})
								if len(ds) > 0 {
									panic("Pipeline function must return an array of objects")
								}
								return nil, nil
							default:
								panic("Pipeline function must return an array of objects")
							}
						}
					} else {
						return nil, errors.New("Exported pipeline function must return an array")
					}
				}
			}
			return nil, nil
		}
	}
	return nil
}

func shutdown(timeout int, hsc *httpServerCtx, bulk *elastic.BulkProcessor, bulkStats *elastic.BulkProcessor, mongo *mgo.Session, config *configOptions) {
	infoLog.Println("Shutting down")
	closeC := make(chan bool)
	go func() {
		if mongo != nil && config.ClusterName != "" {
			resetClusterState(mongo, config)
		}
		if hsc != nil {
			hsc.shutdown = true
			hsc.httpServer.Shutdown(context.Background())
		}
		if bulk != nil {
			bulk.Stop()
		}
		if bulkStats != nil {
			bulkStats.Stop()
		}
		close(closeC)
	}()
	doneC := make(chan bool)
	go func() {
		closeT := time.NewTicker(time.Duration(timeout) * time.Second)
		done := false
		for !done {
			select {
			case <-closeC:
				done = true
				close(doneC)
			case <-closeT.C:
				done = true
				close(doneC)
			}
		}
	}()
	<-doneC
	os.Exit(exitStatus)
}

func handlePanic() {
	if r := recover(); r != nil {
		errorLog.Println(r)
		infoLog.Println("Shutting down with exit status 1 after panic.")
		time.Sleep(3 * time.Second)
		os.Exit(1)
	}
}

func saveTimestampFromReplStatus(session *mgo.Session, config *configOptions) {
	if rs, err := gtm.GetReplStatus(session); err == nil {
		var ts bson.MongoTimestamp
		if ts, err = rs.GetLastCommitted(); err == nil {
			if err = saveTimestamp(session, ts, config); err != nil {
				processErr(err, config)
			}
		} else {
			processErr(err, config)
		}
	} else {
		processErr(err, config)
	}
}

func main() {
	enabled := true
	defer handlePanic()
	config := &configOptions{
		MongoValidatePemFile: true,
		MongoDialSettings:    mongoDialSettings{Timeout: -1, ReadTimeout: -1, WriteTimeout: -1},
		MongoSessionSettings: mongoSessionSettings{SocketTimeout: -1, SyncTimeout: -1},
		GtmSettings:          gtmDefaultSettings(),
	}
	config.parseCommandLineFlags()
	if config.Version {
		fmt.Println(version)
		os.Exit(0)
	}
	config.loadEnvironment()
	config.loadTimeMachineNamespaces()
	config.loadRoutingNamespaces()
	config.loadPatchNamespaces()
	config.loadGridFsConfig()
	config.loadConfigFile()
	config.loadPlugins()
	config.setDefaults()
	if config.Print {
		config.dump()
		os.Exit(0)
	}
	config.setupLogging()
	config.validate()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	mongo, err := config.dialMongo(config.MongoURL)
	if err != nil {
		panic(fmt.Sprintf("Unable to connect to MongoDB using URL %s: %s", cleanMongoURL(config.MongoURL), err))
	}
	infoLog.Printf("Started monstache version %s", version)
	if mongoInfo, err := mongo.BuildInfo(); err == nil {
		infoLog.Printf("Successfully connected to MongoDB version %s", mongoInfo.Version)
	} else {
		infoLog.Println("Successfully connected to MongoDB")
	}
	defer mongo.Close()
	loadBuiltinFunctions(mongo, config)

	elasticClient, err := config.newElasticClient()
	if err != nil {
		panic(fmt.Sprintf("Unable to create Elasticsearch client: %s", err))
	}
	if config.ElasticVersion == "" {
		if err := config.testElasticsearchConn(elasticClient); err != nil {
			panic(fmt.Sprintf("Unable to validate connection to Elasticsearch: %s", err))
		}
	} else {
		if err := config.parseElasticsearchVersion(config.ElasticVersion); err != nil {
			panic(fmt.Sprintf("Elasticsearch version must conform to major.minor.fix: %s", err))
		}
	}
	bulk, err := config.newBulkProcessor(elasticClient)
	if err != nil {
		panic(fmt.Sprintf("Unable to start bulk processor: %s", err))
	}
	defer bulk.Stop()
	var bulkStats *elastic.BulkProcessor
	if config.IndexStats {
		bulkStats, err = config.newStatsBulkProcessor(elasticClient)
		if err != nil {
			panic(fmt.Sprintf("Unable to start stats bulk processor: %s", err))
		}
		defer bulkStats.Stop()
	}

	go notifySd(config)
	var hsc *httpServerCtx
	if config.EnableHTTPServer {
		hsc = &httpServerCtx{
			bulk:   bulk,
			config: config,
		}
		hsc.buildServer()
		go hsc.serveHttp()
	}

	eventLoopC := make(chan bool)
	go func() {
		select {
		case <-eventLoopC:
			return
		case <-sigs:
			os.Exit(exitStatus)
			break
		}
	}()

	var after gtm.TimestampGenerator
	if config.Replay {
		after = func(session *mgo.Session, options *gtm.Options) bson.MongoTimestamp {
			return bson.MongoTimestamp(0)
		}
	} else if config.ResumeFromTimestamp != 0 {
		after = func(session *mgo.Session, options *gtm.Options) bson.MongoTimestamp {
			return bson.MongoTimestamp(config.ResumeFromTimestamp)
		}
	} else if config.Resume {
		after = func(session *mgo.Session, options *gtm.Options) bson.MongoTimestamp {
			var ts bson.MongoTimestamp
			collection := session.DB(config.ConfigDatabaseName).C("monstache")
			doc := make(map[string]interface{})
			collection.FindId(config.ResumeName).One(doc)
			if doc["ts"] != nil {
				ts = doc["ts"].(bson.MongoTimestamp)
			} else {
				ts = gtm.LastOpTimestamp(session, options)
			}
			return ts
		}
	}

	if config.IndexFiles {
		if len(config.FileNamespaces) == 0 {
			errorLog.Fatalln("File indexing is ON but no file namespaces are configured")
		}
		if err := ensureFileMapping(elasticClient); err != nil {
			panic(err)
		}
	}

	var nsFilter, filter, directReadFilter, pluginFilter gtm.OpFilter
	filterChain := []gtm.OpFilter{notMonstache(config), notSystem, notChunks}
	filterArray := []gtm.OpFilter{}
	if config.readShards() {
		filterChain = append(filterChain, notConfig)
	}
	if config.NsRegex != "" {
		filterChain = append(filterChain, filterWithRegex(config.NsRegex))
	}
	if config.NsDropRegex != "" {
		filterChain = append(filterChain, filterDropWithRegex(config.NsDropRegex))
	}
	if config.NsExcludeRegex != "" {
		filterChain = append(filterChain, filterInverseWithRegex(config.NsExcludeRegex))
	}
	if config.NsDropExcludeRegex != "" {
		filterChain = append(filterChain, filterDropInverseWithRegex(config.NsDropExcludeRegex))
	}
	if config.Worker != "" {
		workerFilter, err := consistent.ConsistentHashFilter(config.Worker, config.Workers)
		if err != nil {
			panic(err)
		}
		filterArray = append(filterArray, workerFilter)
	} else if config.Workers != nil {
		panic("Workers configured but this worker is undefined. worker must be set to one of the workers.")
	}
	if filterPlugin != nil {
		pluginFilter = filterWithPlugin()
		filterArray = append(filterArray, pluginFilter)
	} else if len(filterEnvs) > 0 {
		pluginFilter = filterWithScript()
		filterArray = append(filterArray, pluginFilter)
	}
	nsFilter = gtm.ChainOpFilters(filterChain...)
	filter = gtm.ChainOpFilters(filterArray...)
	directReadFilter = gtm.ChainOpFilters(filterArray...)
	var oplogDatabaseName, oplogCollectionName *string
	if config.MongoOpLogDatabaseName != "" {
		oplogDatabaseName = &config.MongoOpLogDatabaseName
	}
	if config.MongoOpLogCollectionName != "" {
		oplogCollectionName = &config.MongoOpLogCollectionName
	}
	if config.ClusterName != "" {
		if err = ensureClusterTTL(mongo, config); err == nil {
			infoLog.Printf("Joined cluster %s", config.ClusterName)
		} else {
			panic(fmt.Sprintf("Unable to enable cluster mode: %s", err))
		}
		enabled, err = enableProcess(mongo, config)
		if err != nil {
			panic(fmt.Sprintf("Unable to determine enabled cluster process: %s", err))
		}
	}
	gtmBufferDuration, err := time.ParseDuration(config.GtmSettings.BufferDuration)
	if err != nil {
		panic(fmt.Sprintf("Unable to parse gtm buffer duration %s: %s", config.GtmSettings.BufferDuration, err))
	}
	var mongos []*mgo.Session
	var configSession *mgo.Session
	if config.readShards() {
		// if we have a config server URL then we are running in a sharded cluster
		configSession, err = config.dialMongo(config.MongoConfigURL)
		if err != nil {
			panic(fmt.Sprintf("Unable to connect to mongodb config server using URL %s: %s", cleanMongoURL(config.MongoConfigURL), err))
		}
		// get the list of shard servers
		shardInfos := gtm.GetShards(configSession)
		if len(shardInfos) == 0 {
			errorLog.Fatalln("Shards enabled but none found in config.shards collection")
		}
		// add each shard server to the sync list
		for _, shardInfo := range shardInfos {
			shardURL := shardInfo.GetURL()
			infoLog.Printf("Adding shard found at %s\n", cleanMongoURL(shardURL))
			shard, err := config.dialMongo(shardURL)
			if err != nil {
				panic(fmt.Sprintf("Unable to connect to mongodb shard using URL %s: %s", cleanMongoURL(shardURL), err))
			}
			defer shard.Close()
			mongos = append(mongos, shard)
		}
	} else {
		mongos = append(mongos, mongo)
	}

	changeStreamNs := config.ChangeStreamNs
	if config.DisableChangeEvents {
		changeStreamNs = []string{}
	}

	gtmOpts := &gtm.Options{
		After:               after,
		Filter:              filter,
		NamespaceFilter:     nsFilter,
		OpLogDisabled:       config.DisableChangeEvents || len(config.ChangeStreamNs) > 0,
		OpLogDatabaseName:   oplogDatabaseName,
		OpLogCollectionName: oplogCollectionName,
		ChannelSize:         config.GtmSettings.ChannelSize,
		Ordering:            gtm.AnyOrder,
		WorkerCount:         10,
		BufferDuration:      gtmBufferDuration,
		BufferSize:          config.GtmSettings.BufferSize,
		DirectReadNs:        config.DirectReadNs,
		DirectReadSplitMax:  config.DirectReadSplitMax,
		DirectReadConcur:    config.DirectReadConcur,
		DirectReadNoTimeout: config.DirectReadNoTimeout,
		DirectReadFilter:    directReadFilter,
		Log:                 infoLog,
		Pipe:                buildPipe(config),
		PipeAllowDisk:       config.PipeAllowDisk,
		ChangeStreamNs:      changeStreamNs,
	}

	heartBeat := time.NewTicker(10 * time.Second)
	if config.ClusterName != "" {
		if enabled {
			infoLog.Printf("Starting work for cluster %s", config.ClusterName)
		} else {
			infoLog.Printf("Pausing work for cluster %s", config.ClusterName)
			bulk.Stop()
			wait := true
			for wait {
				select {
				case req := <-statusReqC:
					req.responseC <- nil
				case <-heartBeat.C:
					enabled, err = enableProcess(mongo, config)
					if enabled {
						wait = false
						infoLog.Printf("Resuming work for cluster %s", config.ClusterName)
						bulk.Start(context.Background())
						break
					}
				}
			}
		}
	} else {
		heartBeat.Stop()
	}

	gtmCtx := gtm.StartMulti(mongos, gtmOpts)

	if config.readShards() && !config.DisableChangeEvents {
		gtmCtx.AddShardListener(configSession, gtmOpts, config.makeShardInsertHandler())
	}
	timestampTicker := time.NewTicker(10 * time.Second)
	if config.Resume == false {
		timestampTicker.Stop()
	}
	statsTimeout := time.Duration(30) * time.Second
	if config.StatsDuration != "" {
		statsTimeout, err = time.ParseDuration(config.StatsDuration)
		if err != nil {
			panic(fmt.Sprintf("Unable to parse stats duration: %s", err))
		}
	}
	printStats := time.NewTicker(statsTimeout)
	if config.Stats == false {
		printStats.Stop()
	}
	var lastTimestamp, lastSavedTimestamp bson.MongoTimestamp
	var allOpsVisited bool
	var fileWg, indexWg, processWg, relateWg sync.WaitGroup
	doneC := make(chan int)
	opsConsumed := make(chan bool)
	outputChs := &outputChans{
		indexC:   make(chan *gtm.Op),
		processC: make(chan *gtm.Op),
		fileC:    make(chan *gtm.Op),
		relateC:  make(chan *gtm.Op, config.RelateBuffer),
		filter:   pluginFilter,
	}
	if len(config.Relate) > 0 {
		for i := 0; i < config.RelateThreads; i++ {
			relateWg.Add(1)
			go func() {
				defer relateWg.Done()
				for op := range outputChs.relateC {
					if err := processRelated(mongo, bulk, elasticClient, config, op, outputChs); err != nil {
						processErr(err, config)
					}
				}
			}()
		}
	}
	for i := 0; i < 5; i++ {
		indexWg.Add(1)
		go func() {
			defer indexWg.Done()
			for op := range outputChs.indexC {
				if err := doIndex(config, mongo, bulk, elasticClient, op); err != nil {
					processErr(err, config)
				}
			}
		}()
	}
	for i := 0; i < config.FileDownloaders; i++ {
		fileWg.Add(1)
		go func() {
			defer fileWg.Done()
			for op := range outputChs.fileC {
				err := addFileContent(mongo, op, config)
				if err != nil {
					processErr(err, config)
				}
				outputChs.indexC <- op
			}
		}()
	}
	for i := 0; i < config.PostProcessors; i++ {
		processWg.Add(1)
		go func() {
			defer processWg.Done()
			for op := range outputChs.processC {
				if err := runProcessor(mongo, bulk, elasticClient, op); err != nil {
					processErr(err, config)
				}
			}
		}()
	}
	var tearDown = func() {
		infoLog.Println("Stopping all workers")
		gtmCtx.Stop()
		<-opsConsumed
		close(outputChs.relateC)
		relateWg.Wait()
		close(outputChs.fileC)
		fileWg.Wait()
		close(outputChs.indexC)
		indexWg.Wait()
		close(outputChs.processC)
		processWg.Wait()
		doneC <- 30
	}
	if len(config.DirectReadNs) > 0 {
		go func() {
			gtmCtx.DirectReadWg.Wait()
			infoLog.Println("Direct reads completed")
			if config.Resume {
				saveTimestampFromReplStatus(mongo, config)
			}
			if config.ExitAfterDirectReads {
				tearDown()
			}
		}()
	}
	go func() {
		close(eventLoopC)
		<-sigs
		go func() {
			<-sigs
			os.Exit(exitStatus)
		}()
		tearDown()
	}()
	infoLog.Println("Listening for events")
	for {
		select {
		case timeout := <-doneC:
			if enabled {
				enabled = false
				shutdown(timeout, hsc, bulk, bulkStats, mongo, config)
			} else {
				shutdown(timeout, hsc, nil, nil, nil, config)
			}
			return
		case <-timestampTicker.C:
			if !enabled {
				break
			}
			if lastTimestamp > lastSavedTimestamp {
				bulk.Flush()
				if saveTimestamp(mongo, lastTimestamp, config); err == nil {
					lastSavedTimestamp = lastTimestamp
				} else {
					processErr(err, config)
				}
			}
		case <-heartBeat.C:
			if config.ClusterName == "" {
				break
			}
			if enabled {
				enabled, err = ensureEnabled(mongo, config)
				if !enabled {
					infoLog.Printf("Pausing work for cluster %s", config.ClusterName)
					gtmCtx.Pause()
					bulk.Stop()
					wait := true
					for wait {
						select {
						case req := <-statusReqC:
							req.responseC <- nil
						case <-heartBeat.C:
							enabled, err = enableProcess(mongo, config)
							if enabled {
								wait = false
								infoLog.Printf("Resuming work for cluster %s", config.ClusterName)
								bulk.Start(context.Background())
								resumeWork(gtmCtx, mongo, config)
								break
							}
						}
					}
				}
			} else {
				enabled, err = enableProcess(mongo, config)
				if enabled {
					infoLog.Printf("Resuming work for cluster %s", config.ClusterName)
					bulk.Start(context.Background())
					resumeWork(gtmCtx, mongo, config)
				}
			}
			if err != nil {
				processErr(err, config)
			}
		case <-printStats.C:
			if !enabled {
				break
			}
			if config.IndexStats {
				if err := doIndexStats(config, bulkStats, bulk.Stats()); err != nil {
					errorLog.Printf("Error indexing statistics: %s", err)
				}
			} else {
				stats, err := json.Marshal(bulk.Stats())
				if err != nil {
					errorLog.Printf("Unable to log statistics: %s", err)
				} else {
					statsLog.Println(string(stats))
				}
			}
		case req := <-statusReqC:
			e, l := enabled, lastTimestamp
			statusResp := &statusResponse{
				enabled: e,
				lastTs:  l,
			}
			req.responseC <- statusResp
		case err = <-gtmCtx.ErrC:
			if err == nil {
				break
			}
			processErr(err, config)
		case op, open := <-gtmCtx.OpC:
			if !enabled {
				break
			}
			if op == nil {
				if !open && !allOpsVisited {
					allOpsVisited = true
					opsConsumed <- true
				}
				break
			}
			if op.IsSourceOplog() {
				lastTimestamp = op.Timestamp
			}
			if err = routeOp(config, mongo, bulk, elasticClient, op, outputChs); err != nil {
				processErr(err, config)
			}
		}
	}
}
