package main

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/evanphx/json-patch"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
	elastigo "github.com/rwynn/elastigo/lib"
	"github.com/rwynn/gtm"
	"github.com/rwynn/gtm/consistent"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var gridByteBuffer bytes.Buffer
var infoLog *log.Logger = log.New(os.Stdout, "INFO ", log.Flags())

var mapEnvs map[string]*executionEnv
var mapIndexTypes map[string]*indexTypeMapping
var fileNamespaces map[string]bool
var patchNamespaces map[string]bool

var chunksRegex = regexp.MustCompile("\\.chunks$")
var systemsRegex = regexp.MustCompile("system\\..+$")

const Version = "2.11.2"
const mongoUrlDefault string = "localhost"
const resumeNameDefault string = "default"
const elasticMaxConnsDefault int = 10
const gtmChannelSizeDefault int = 100

type executionEnv struct {
	Vm      *otto.Otto
	Script  string
	Routing bool
}

type javascript struct {
	Namespace string
	Script    string
	Routing   bool
}

type indexTypeMapping struct {
	Namespace string
	Index     string
	Type      string
}

type indexingMeta struct {
	Routing string
	Index   string
	Type    string
}

type mongoDialSettings struct {
	Timeout int
	Ssl     bool
}

type mongoSessionSettings struct {
	SocketTimeout int `toml:"socket-timeout"`
	SyncTimeout   int `toml:"sync-timeout"`
}

type gtmSettings struct {
	ChannelSize    int    `toml:"channel-size"`
	BufferSize     int    `toml:"buffer-size"`
	BufferDuration string `toml:"buffer-duration"`
	WorkerCount    int    `toml:"worker-count"`
	Ordering       int
}

type configOptions struct {
	MongoUrl                 string               `toml:"mongo-url"`
	MongoPemFile             string               `toml:"mongo-pem-file"`
	MongoValidatePemFile     bool                 `toml:"mongo-validate-pem-file"`
	MongoOpLogDatabaseName   string               `toml:"mongo-oplog-database-name"`
	MongoOpLogCollectionName string               `toml:"mongo-oplog-collection-name"`
	MongoCursorTimeout       string               `toml:"mongo-cursor-timeout"`
	MongoDialSettings        mongoDialSettings    `toml:"mongo-dial-settings"`
	MongoSessionSettings     mongoSessionSettings `toml:"mongo-session-settings"`
	GtmSettings              gtmSettings          `toml:"gtm-settings"`
	ElasticUrl               string               `toml:"elasticsearch-url"`
	ElasticPemFile           string               `toml:"elasticsearch-pem-file"`
	ElasticValidatePemFile   bool                 `toml:"elasticsearch-validate-pem-file`
	ElasticVersion           string               `toml:"elasticsearch-version"`
	ResumeName               string               `toml:"resume-name"`
	NsRegex                  string               `toml:"namespace-regex"`
	NsExcludeRegex           string               `toml:"namespace-exclude-regex"`
	ClusterName              string               `toml:"cluster-name"`
	Version                  bool
	Gzip                     bool
	Verbose                  bool
	Resume                   bool
	ResumeWriteUnsafe        bool  `toml:"resume-write-unsafe"`
	ResumeFromTimestamp      int64 `toml:"resume-from-timestamp"`
	Replay                   bool
	DroppedDatabases         bool     `toml:"dropped-databases"`
	DroppedCollections       bool     `toml:"dropped-collections"`
	IndexFiles               bool     `toml:"index-files"`
	FileHighlighting         bool     `toml:"file-highlighting"`
	EnablePatches            bool     `toml:"enable-patches"`
	FailFast                 bool     `toml:"fail-fast"`
	IndexOplogTime           bool     `toml:"index-oplog-time"`
	MergePatchAttr           string   `toml:"merge-patch-attribute"`
	ElasticMaxConns          int      `toml:"elasticsearch-max-conns"`
	ElasticRetrySeconds      int      `toml:"elasticsearch-retry-seconds"`
	ElasticMaxDocs           int      `toml:"elasticsearch-max-docs"`
	ElasticMaxBytes          int      `toml:"elasticsearch-max-bytes"`
	ElasticMaxSeconds        int      `toml:"elasticsearch-max-seconds"`
	ElasticHosts             []string `toml:"elasticsearch-hosts"`
	ElasticMajorVersion      int
	MaxFileSize              int64 `toml:"max-file-size"`
	ConfigFile               string
	Script                   []javascript
	Mapping                  []indexTypeMapping
	FileNamespaces           []string `toml:"file-namespaces"`
	PatchNamespaces          []string `toml:"patch-namespaces"`
	Workers                  []string
	Worker                   string
}

func (this *configOptions) ParseElasticSearchVersion(number string) (err error) {
	if number == "" {
		err = errors.New("Elasticsearch version cannot be blank")
	} else {
		versionParts := strings.Split(number, ".")
		var majorVersion int
		majorVersion, err = strconv.Atoi(versionParts[0])
		if err == nil {
			this.ElasticMajorVersion = majorVersion
			if majorVersion == 0 {
				err = errors.New("Invalid elasticsearch major version 0")
			}
		}
	}
	return
}

func TestElasticSearchConn(conn *elastigo.Conn, config *configOptions) (err error) {
	var result map[string]interface{}
	body, err := conn.DoCommand("GET", "/", nil, nil)
	if err != nil {
		return
	}
	err = json.Unmarshal(body, &result)
	if err == nil {
		version := result["version"].(map[string]interface{})
		if version == nil {
			err = errors.New("Unable to determine elasticsearch version")
		} else {
			number := version["number"].(string)
			if config.Verbose {
				infoLog.Printf("Successfully connected to elasticsearch version %s", number)
			}
			err = config.ParseElasticSearchVersion(number)
		}
	}
	return
}

func NormalizeIndexName(name string) (normal string) {
	normal = strings.ToLower(strings.TrimPrefix(name, "_"))
	return
}

func NormalizeTypeName(name string) (normal string) {
	normal = strings.TrimPrefix(name, "_")
	return
}

func NormalizeEsId(id string) (normal string) {
	normal = strings.TrimPrefix(id, "_")
	return
}

func DeleteIndexes(conn *elastigo.Conn, db string, config *configOptions) (err error) {
	for ns, m := range mapIndexTypes {
		parts := strings.SplitN(ns, ".", 2)
		if parts[0] == db {
			if _, err = conn.DeleteIndex(m.Index + "*"); err != nil {
				return
			}
		}
	}
	_, err = conn.DeleteIndex(NormalizeIndexName(db) + "*")
	return
}

func DeleteIndex(conn *elastigo.Conn, namespace string, config *configOptions) (err error) {
	esIndex := NormalizeIndexName(namespace)
	if m := mapIndexTypes[namespace]; m != nil {
		esIndex = m.Index
	}
	_, err = conn.DeleteIndex(esIndex)
	return err
}

func IngestAttachment(conn *elastigo.Conn, esIndex string, esType string, esId string, data map[string]interface{}, meta *indexingMeta) (err error) {
	var body []byte
	args := map[string]interface{}{
		"pipeline": "attachment",
	}
	if meta.Routing != "" {
		args["routing"] = meta.Routing
	}
	body, err = json.Marshal(data)
	if err == nil {
		_, err = conn.DoCommand("PUT", fmt.Sprintf("/%s/%s/%s", esIndex, esType, esId), args, string(body))
	}
	return err
}

func EnsureFileMapping(conn *elastigo.Conn, namespace string, config *configOptions) (err error) {
	if config.ElasticMajorVersion < 5 {
		return EnsureFileMappingMapperAttachment(conn, namespace, config)
	} else {
		return EnsureFileMappingIngestAttachment(conn, namespace, config)
	}
}

func EnsureFileMappingIngestAttachment(conn *elastigo.Conn, namespace string, config *configOptions) (err error) {
	var body []byte
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
	body, err = json.Marshal(pipeline)
	if err == nil {
		_, err = conn.DoCommand("PUT", "/_ingest/pipeline/attachment", nil, string(body))
	}
	return err
}

func EnsureFileMappingMapperAttachment(conn *elastigo.Conn, namespace string, config *configOptions) (err error) {
	var body []byte
	parts := strings.SplitN(namespace, ".", 2)
	esIndex, esType := NormalizeIndexName(namespace), NormalizeTypeName(parts[1])
	if m := mapIndexTypes[namespace]; m != nil {
		esIndex, esType = m.Index, m.Type
	}
	props := map[string]interface{}{
		"properties": map[string]interface{}{
			"file": map[string]interface{}{
				"type": "attachment",
			},
		},
	}
	file := props["properties"].(map[string]interface{})["file"].(map[string]interface{})
	types := map[string]interface{}{
		esType: props,
	}
	mappings := map[string]interface{}{
		"mappings": types,
	}
	if config.FileHighlighting {
		file["fields"] = map[string]interface{}{
			"content": map[string]interface{}{
				"type":        "string",
				"term_vector": "with_positions_offsets",
				"store":       true,
			},
		}
	}
	if exists, _ := conn.ExistsIndex(esIndex, "", nil); exists {
		body, err = json.Marshal(types)
		if err != nil {
			return err
		}
		_, err = conn.DoCommand("PUT", fmt.Sprintf("/%s/%s/_mapping", esIndex, esType), nil, string(body))
	} else {
		body, err = json.Marshal(mappings)
		if err != nil {
			return err
		}
		_, err = conn.DoCommand("PUT", fmt.Sprintf("/%s", esIndex), nil, string(body))
	}
	return err
}

func DefaultIndexTypeMapping(op *gtm.Op) *indexTypeMapping {
	return &indexTypeMapping{
		Namespace: op.Namespace,
		Index:     NormalizeIndexName(op.Namespace),
		Type:      NormalizeTypeName(op.GetCollection()),
	}
}

func IndexTypeMapping(op *gtm.Op) *indexTypeMapping {
	mapping := DefaultIndexTypeMapping(op)
	if mapIndexTypes != nil {
		if m := mapIndexTypes[op.Namespace]; m != nil {
			mapping = m
		}
	}
	return mapping
}

func OpIdToString(op *gtm.Op) string {
	var opIdStr string
	switch op.Id.(type) {
	case bson.ObjectId:
		opIdStr = op.Id.(bson.ObjectId).Hex()
	case float64:
		intId := int(op.Id.(float64))
		if op.Id.(float64) == float64(intId) {
			opIdStr = fmt.Sprintf("%v", intId)
		} else {
			opIdStr = fmt.Sprintf("%v", op.Id)
		}
	case float32:
		intId := int(op.Id.(float32))
		if op.Id.(float32) == float32(intId) {
			opIdStr = fmt.Sprintf("%v", intId)
		} else {
			opIdStr = fmt.Sprintf("%v", op.Id)
		}
	default:
		opIdStr = NormalizeEsId(fmt.Sprintf("%v", op.Id))
	}
	return opIdStr
}

func MapData(op *gtm.Op) error {
	if mapEnvs == nil {
		return nil
	}
	if env := mapEnvs[op.Namespace]; env != nil {
		val, err := env.Vm.Call("module.exports", op.Data, op.Data)
		if err != nil {
			return err
		}
		if strings.ToLower(val.Class()) == "object" {
			data, err := val.Export()
			if err != nil {
				return err
			} else if data == val {
				return errors.New("exported function must return an object")
			} else {
				op.Data = data.(map[string]interface{})
			}
		} else {
			indexed, err := val.ToBoolean()
			if err != nil {
				return err
			} else if !indexed {
				op.Data = nil
			}
		}
	}
	return nil
}

func PrepareDataForIndexing(config *configOptions, op *gtm.Op) {
	data := op.Data
	if config.IndexOplogTime {
		secs := int64(op.Timestamp >> 32)
		t := time.Unix(secs, 0).UTC()
		data["_oplog_ts"] = op.Timestamp
		data["_oplog_date"] = t.Format("2006/01/02 15:04:05")
	}
	delete(data, "_id")
	delete(data, "_type")
	delete(data, "_index")
	delete(data, "_score")
	delete(data, "_source")
	delete(data, "_meta_monstache")
}

func ParseIndexMeta(data map[string]interface{}) (meta *indexingMeta) {
	meta = &indexingMeta{}
	if m, ok := data["_meta_monstache"]; ok {
		switch m.(type) {
		case map[string]interface{}:
			metaAttrs := m.(map[string]interface{})
			if r, ok := metaAttrs["routing"]; ok {
				meta.Routing = fmt.Sprintf("%v", r)
			}
			if i, ok := metaAttrs["index"]; ok {
				meta.Index = fmt.Sprintf("%v", i)
			}
			if t, ok := metaAttrs["type"]; ok {
				meta.Type = fmt.Sprintf("%v", t)
			}
		case otto.Value:
			ex, err := m.(otto.Value).Export()
			if err == nil && ex != m {
				switch ex.(type) {
				case map[string]interface{}:
					metaAttrs := ex.(map[string]interface{})
					if r, ok := metaAttrs["routing"]; ok {
						meta.Routing = fmt.Sprintf("%v", r)
					}
					if i, ok := metaAttrs["index"]; ok {
						meta.Index = fmt.Sprintf("%v", i)
					}
					if t, ok := metaAttrs["type"]; ok {
						meta.Type = fmt.Sprintf("%v", t)
					}
				default:
					log.Println("invalid indexing metadata")
				}
			}
		default:
			log.Println("invalid indexing metadata")
		}
	}
	return meta
}

func AddFileContent(session *mgo.Session, op *gtm.Op, config *configOptions) (err error) {
	op.Data["file"] = ""
	gridByteBuffer.Reset()
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
			infoLog.Printf("file %s md5(%s) exceeds max file size. file content omitted.",
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

func NotMonstache(op *gtm.Op) bool {
	return op.GetDatabase() != "monstache"
}

func NotChunks(op *gtm.Op) bool {
	return !chunksRegex.MatchString(op.GetCollection())
}

func NotSystem(op *gtm.Op) bool {
	return !systemsRegex.MatchString(op.GetCollection())
}

func FilterWithRegex(regex string) gtm.OpFilter {
	var validNameSpace = regexp.MustCompile(regex)
	return func(op *gtm.Op) bool {
		return validNameSpace.MatchString(op.Namespace)
	}
}

func FilterInverseWithRegex(regex string) gtm.OpFilter {
	var invalidNameSpace = regexp.MustCompile(regex)
	return func(op *gtm.Op) bool {
		return !invalidNameSpace.MatchString(op.Namespace)
	}
}

func EnsureClusterTTL(session *mgo.Session) error {
	col := session.DB("monstache").C("cluster")
	return col.EnsureIndex(mgo.Index{
		Key:         []string{"expireAt"},
		Background:  true,
		ExpireAfter: time.Duration(30) * time.Second,
	})
}

func IsEnabledProcess(session *mgo.Session, config *configOptions) (bool, error) {
	col := session.DB("monstache").C("cluster")
	doc := make(map[string]interface{})
	doc["_id"] = config.ResumeName
	doc["expireAt"] = time.Now().UTC()
	doc["pid"] = os.Getpid()
	if host, err := os.Hostname(); err == nil {
		doc["host"] = host
	} else {
		return false, err
	}
	err := col.Insert(doc)
	if err == nil {
		return true, nil
	}
	lastError := err.(*mgo.LastError)
	if lastError.Code == 11000 {
		return false, nil
	} else {
		return false, err
	}
}

func ResetClusterState(session *mgo.Session, config *configOptions) error {
	col := session.DB("monstache").C("cluster")
	return col.RemoveId(config.ResumeName)
}

func IsEnabledProcessId(session *mgo.Session, config *configOptions) bool {
	col := session.DB("monstache").C("cluster")
	doc := make(map[string]interface{})
	col.FindId(config.ResumeName).One(doc)
	if doc["pid"] != nil && doc["host"] != nil {
		pid := doc["pid"].(int)
		host := doc["host"].(string)
		hostname, err := os.Hostname()
		if err != nil {
			log.Println(err)
			return false
		}
		enabled := pid == os.Getpid() && host == hostname
		if enabled {
			col.UpdateId(config.ResumeName,
				bson.M{"$set": bson.M{"expireAt": time.Now().UTC()}})
		}
		return enabled
	}
	return false
}

func ResumeWork(session *mgo.Session, config *configOptions) {
	col := session.DB("monstache").C("monstache")
	doc := make(map[string]interface{})
	col.FindId(config.ResumeName).One(doc)
	if doc["ts"] != nil {
		ts := doc["ts"].(bson.MongoTimestamp)
		gtm.Since(ts)
	}
	gtm.Resume()
}

func SaveTimestamp(session *mgo.Session, op *gtm.Op, resumeName string) error {
	col := session.DB("monstache").C("monstache")
	doc := make(map[string]interface{})
	doc["ts"] = op.Timestamp
	_, err := col.UpsertId(resumeName, bson.M{"$set": doc})
	return err
}

func (config *configOptions) ParseCommandLineFlags() *configOptions {
	flag.StringVar(&config.MongoUrl, "mongo-url", "", "MongoDB connection URL")
	flag.StringVar(&config.MongoPemFile, "mongo-pem-file", "", "Path to a PEM file for secure connections to MongoDB")
	flag.BoolVar(&config.MongoValidatePemFile, "mongo-validate-pem-file", true, "Set to boolean false to not validate the MongoDB PEM file")
	flag.StringVar(&config.MongoOpLogDatabaseName, "mongo-oplog-database-name", "", "Override the database name which contains the mongodb oplog")
	flag.StringVar(&config.MongoOpLogCollectionName, "mongo-oplog-collection-name", "", "Override the collection name which contains the mongodb oplog")
	flag.StringVar(&config.MongoCursorTimeout, "mongo-cursor-timeout", "", "Override the duration before a cursor timeout occurs when tailing the oplog")
	flag.StringVar(&config.ElasticVersion, "elasticsearch-version", "", "Specify elasticsearch version directly instead of getting it from the server")
	flag.StringVar(&config.ElasticUrl, "elasticsearch-url", "", "ElasticSearch connection URL")
	flag.StringVar(&config.ElasticPemFile, "elasticsearch-pem-file", "", "Path to a PEM file for secure connections to elasticsearch")
	flag.BoolVar(&config.ElasticValidatePemFile, "elasticsearch-validate-pem-file", true, "Set to boolean false to not validate the ElasticSearch PEM file")
	flag.IntVar(&config.ElasticMaxConns, "elasticsearch-max-conns", 0, "ElasticSearch max connections")
	flag.IntVar(&config.ElasticRetrySeconds, "elasticsearch-retry-seconds", 0, "Number of seconds before retrying ElasticSearch requests")
	flag.IntVar(&config.ElasticMaxDocs, "elasticsearch-max-docs", 0, "Number of docs to hold before flushing to ElasticSearch")
	flag.IntVar(&config.ElasticMaxBytes, "elasticsearch-max-bytes", 0, "Number of bytes to hold before flushing to ElasticSearch")
	flag.IntVar(&config.ElasticMaxSeconds, "elasticsearch-max-seconds", 0, "Number of seconds before flushing to ElasticSearch")
	flag.Int64Var(&config.MaxFileSize, "max-file-size", 0, "GridFs file content exceeding this limit in bytes will not be indexed in ElasticSearch")
	flag.StringVar(&config.ConfigFile, "f", "", "Location of configuration file")
	flag.BoolVar(&config.DroppedDatabases, "dropped-databases", true, "True to delete indexes from dropped databases")
	flag.BoolVar(&config.DroppedCollections, "dropped-collections", true, "True to delete indexes from dropped collections")
	flag.BoolVar(&config.Version, "v", false, "True to print the version number")
	flag.BoolVar(&config.Gzip, "gzip", false, "True to use gzip for requests to elasticsearch")
	flag.BoolVar(&config.Verbose, "verbose", false, "True to output verbose messages")
	flag.BoolVar(&config.Resume, "resume", false, "True to capture the last timestamp of this run and resume on a subsequent run")
	flag.Int64Var(&config.ResumeFromTimestamp, "resume-from-timestamp", 0, "Timestamp to resume syncing from")
	flag.BoolVar(&config.ResumeWriteUnsafe, "resume-write-unsafe", false, "True to speedup writes of the last timestamp synched for resuming at the cost of error checking")
	flag.BoolVar(&config.Replay, "replay", false, "True to replay all events from the oplog and index them in elasticsearch")
	flag.BoolVar(&config.IndexFiles, "index-files", false, "True to index gridfs files into elasticsearch. Requires the elasticsearch mapper-attachments (deprecated) or ingest-attachment plugin")
	flag.BoolVar(&config.FileHighlighting, "file-highlighting", false, "True to enable the ability to highlight search times for a file query")
	flag.BoolVar(&config.EnablePatches, "enable-patches", false, "True to include an json-patch field on updates")
	flag.BoolVar(&config.FailFast, "fail-fast", false, "True to exit if a single _bulk request fails")
	flag.BoolVar(&config.IndexOplogTime, "index-oplog-time", false, "True to add date/time information from the oplog to each document when indexing")
	flag.StringVar(&config.MergePatchAttr, "merge-patch-attribute", "", "Attribute to store json-patch values under")
	flag.StringVar(&config.ResumeName, "resume-name", "", "Name under which to load/store the resume state. Defaults to 'default'")
	flag.StringVar(&config.ClusterName, "cluster-name", "", "Name of the monstache process cluster")
	flag.StringVar(&config.Worker, "worker", "", "The name of this worker in a multi-worker configuration")
	flag.StringVar(&config.NsRegex, "namespace-regex", "", "A regex which is matched against an operation's namespace (<database>.<collection>).  Only operations which match are synched to elasticsearch")
	flag.StringVar(&config.NsExcludeRegex, "namespace-exclude-regex", "", "A regex which is matched against an operation's namespace (<database>.<collection>).  Only operations which do not match are synched to elasticsearch")
	flag.Parse()
	return config
}

func (config *configOptions) LoadIndexTypes() {
	if config.Mapping != nil {
		mapIndexTypes = make(map[string]*indexTypeMapping)
		for _, m := range config.Mapping {
			if m.Namespace != "" && m.Index != "" && m.Type != "" {
				mapIndexTypes[m.Namespace] = &indexTypeMapping{
					Namespace: m.Namespace,
					Index:     NormalizeIndexName(m.Index),
					Type:      NormalizeTypeName(m.Type),
				}
			} else {
				panic("mappings must specify namespace, index, and type attributes")
			}
		}
	}
}

func (config *configOptions) LoadScripts() {
	if config.Script != nil {
		mapEnvs = make(map[string]*executionEnv)
		for _, s := range config.Script {
			if s.Namespace != "" && s.Script != "" {
				env := &executionEnv{
					Vm:      otto.New(),
					Script:  s.Script,
					Routing: s.Routing,
				}
				if err := env.Vm.Set("module", make(map[string]interface{})); err != nil {
					panic(err)
				}
				if _, err := env.Vm.Run(env.Script); err != nil {
					panic(err)
				}
				val, err := env.Vm.Run("module.exports")
				if err != nil {
					panic(err)
				} else if !val.IsFunction() {
					panic("module.exports must be a function")

				}
				mapEnvs[s.Namespace] = env
			} else {
				panic("scripts must specify namespace and script attributes")
			}
		}
	}
}

func (config *configOptions) LoadConfigFile() *configOptions {
	if config.ConfigFile != "" {
		var tomlConfig configOptions = configOptions{
			DroppedDatabases:     true,
			DroppedCollections:   true,
			MongoDialSettings:    mongoDialSettings{Timeout: -1},
			MongoSessionSettings: mongoSessionSettings{SocketTimeout: -1, SyncTimeout: -1},
			GtmSettings:          GtmDefaultSettings(),
		}
		if _, err := toml.DecodeFile(config.ConfigFile, &tomlConfig); err != nil {
			panic(err)
		}
		if config.MongoUrl == "" {
			config.MongoUrl = tomlConfig.MongoUrl
		}
		if config.MongoPemFile == "" {
			config.MongoPemFile = tomlConfig.MongoPemFile
		}
		if config.MongoValidatePemFile && !tomlConfig.MongoValidatePemFile {
			config.MongoValidatePemFile = false
		}
		if config.MongoOpLogDatabaseName == "" {
			config.MongoOpLogDatabaseName = tomlConfig.MongoOpLogDatabaseName
		}
		if config.MongoOpLogCollectionName == "" {
			config.MongoOpLogCollectionName = tomlConfig.MongoOpLogCollectionName
		}
		if config.MongoCursorTimeout == "" {
			config.MongoCursorTimeout = tomlConfig.MongoCursorTimeout
		}
		if config.ElasticPemFile == "" {
			config.ElasticPemFile = tomlConfig.ElasticPemFile
		}
		if config.ElasticValidatePemFile && !tomlConfig.ElasticValidatePemFile {
			config.ElasticValidatePemFile = false
		}
		if config.ElasticUrl == "" {
			config.ElasticUrl = tomlConfig.ElasticUrl
		}
		if config.ElasticVersion == "" {
			config.ElasticVersion = tomlConfig.ElasticVersion
		}
		if config.ElasticMaxConns == 0 {
			config.ElasticMaxConns = tomlConfig.ElasticMaxConns
		}
		if config.ElasticRetrySeconds == 0 {
			config.ElasticRetrySeconds = tomlConfig.ElasticRetrySeconds
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
		if config.MaxFileSize == 0 {
			config.MaxFileSize = tomlConfig.MaxFileSize
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
		if !config.IndexFiles && tomlConfig.IndexFiles {
			config.IndexFiles = true
		}
		if !config.FileHighlighting && tomlConfig.FileHighlighting {
			config.FileHighlighting = true
		}
		if !config.EnablePatches && tomlConfig.EnablePatches {
			config.EnablePatches = true
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
		if config.Resume && config.ResumeName == "" {
			config.ResumeName = tomlConfig.ResumeName
		}
		if config.ClusterName == "" {
			config.ClusterName = tomlConfig.ClusterName
		}
		if config.NsRegex == "" {
			config.NsRegex = tomlConfig.NsRegex
		}
		if config.NsExcludeRegex == "" {
			config.NsExcludeRegex = tomlConfig.NsExcludeRegex
		}
		if config.IndexFiles {
			config.FileNamespaces = tomlConfig.FileNamespaces
			config.LoadGridFsConfig()
		}
		if config.Worker == "" {
			config.Worker = tomlConfig.Worker
		}
		if config.EnablePatches {
			config.PatchNamespaces = tomlConfig.PatchNamespaces
			config.LoadPatchNamespaces()
		}
		config.Workers = tomlConfig.Workers
		config.ElasticHosts = tomlConfig.ElasticHosts
		config.MongoDialSettings = tomlConfig.MongoDialSettings
		config.MongoSessionSettings = tomlConfig.MongoSessionSettings
		config.GtmSettings = tomlConfig.GtmSettings
		tomlConfig.LoadScripts()
		tomlConfig.LoadIndexTypes()
	}
	return config
}

func (config *configOptions) LoadPatchNamespaces() *configOptions {
	patchNamespaces = make(map[string]bool)
	for _, namespace := range config.PatchNamespaces {
		patchNamespaces[namespace] = true
	}
	return config
}

func (config *configOptions) LoadGridFsConfig() *configOptions {
	fileNamespaces = make(map[string]bool)
	for _, namespace := range config.FileNamespaces {
		fileNamespaces[namespace] = true
	}
	return config
}

func (config *configOptions) SetDefaults() *configOptions {
	if config.MongoUrl == "" {
		config.MongoUrl = mongoUrlDefault
	}
	if config.ClusterName != "" {
		if config.ClusterName != "" && config.Worker != "" {
			config.ResumeName = fmt.Sprintf("%s:%s", config.ClusterName, config.Worker)
		} else {
			config.ResumeName = config.ClusterName
		}
		config.Resume = true
	} else if config.ResumeName == "" {
		if config.Worker != "" {
			config.ResumeName = config.Worker
		} else {
			config.ResumeName = resumeNameDefault
		}
	}
	if config.ElasticMaxConns == 0 {
		config.ElasticMaxConns = elasticMaxConnsDefault
	}
	if config.MergePatchAttr == "" {
		config.MergePatchAttr = "json-merge-patches"
	}
	if config.ElasticMaxSeconds == 0 {
		config.ElasticMaxSeconds = 2
	}
	return config
}

func (config *configOptions) DialMongo() (*mgo.Session, error) {
	ssl := config.MongoDialSettings.Ssl || config.MongoPemFile != ""
	if ssl {
		tlsConfig := &tls.Config{}
		if config.MongoPemFile != "" {
			certs := x509.NewCertPool()
			if ca, err := ioutil.ReadFile(config.MongoPemFile); err == nil {
				certs.AppendCertsFromPEM(ca)
			} else {
				return nil, err
			}
			tlsConfig.RootCAs = certs
		}
		// Check to see if we don't need to validate the PEM
		if config.MongoValidatePemFile == false {
			// Turn off validation
			tlsConfig.InsecureSkipVerify = true
		}
		dialInfo, err := mgo.ParseURL(config.MongoUrl)
		if err != nil {
			return nil, err
		} else {
			dialInfo.Timeout = time.Duration(10) * time.Second
			if config.MongoDialSettings.Timeout != -1 {
				dialInfo.Timeout = time.Duration(config.MongoDialSettings.Timeout) * time.Second
			}
			dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
				conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
				if err != nil {
					log.Printf("Unable to dial mongodb: %s", err)
				}
				return conn, err
			}
			session, err := mgo.DialWithInfo(dialInfo)
			if err == nil {
				session.SetSyncTimeout(1 * time.Minute)
				session.SetSocketTimeout(1 * time.Minute)
			}
			return session, err
		}
	} else {
		if config.MongoDialSettings.Timeout != -1 {
			return mgo.DialWithTimeout(config.MongoUrl,
				time.Duration(config.MongoDialSettings.Timeout)*time.Second)
		} else {
			return mgo.Dial(config.MongoUrl)
		}
	}
}

func (config *configOptions) ConfigHttpTransport() error {
	if config.ElasticPemFile != "" {
		certs := x509.NewCertPool()
		if ca, err := ioutil.ReadFile(config.ElasticPemFile); err == nil {
			certs.AppendCertsFromPEM(ca)
		} else {
			return err
		}
		tlsConfig := &tls.Config{RootCAs: certs}
		// Check to see if we don't need to validate the PEM
		if config.ElasticValidatePemFile == false {
			// Turn off validation
			tlsConfig.InsecureSkipVerify = true
		}
		http.DefaultTransport.(*http.Transport).TLSClientConfig = tlsConfig
	}
	return nil
}

func TraceRequest(method, url, body string) {
	infoLog.Printf("%s request sent to %s", method, url)
	if body != "" {
		ba := []byte(body)
		if len(ba) > 1 && ba[0] == 0x1f && ba[1] == 0x8b {
			buff := bytes.NewBuffer(ba)
			reader, err := gzip.NewReader(buff)
			if err != nil {
				return
			}
			defer reader.Close()
			if unzipped, err := ioutil.ReadAll(reader); err == nil {
				infoLog.Printf("request body: %s", unzipped)
			} else {
				log.Printf("unable to unzip request: %s", err)
			}
		} else {
			infoLog.Printf("request body: %s", body)
		}
	}
}

func DoDrop(mongo *mgo.Session, elastic *elastigo.Conn, op *gtm.Op, config *configOptions) (indexed bool, err error) {
	if db, drop := op.IsDropDatabase(); drop {
		if config.DroppedDatabases {
			if err = DeleteIndexes(elastic, db, config); err == nil {
				indexed = true
				if e := DropDBMeta(mongo, db); e != nil {
					log.Printf("unable to delete meta for db: %s", e)
				}
			}
		} else {
			indexed = true
		}
	} else if col, drop := op.IsDropCollection(); drop {
		if config.DroppedCollections {
			if err = DeleteIndex(elastic, op.GetDatabase()+"."+col, config); err == nil {
				indexed = true
				if e := DropCollectionMeta(mongo, op.GetDatabase()+"."+col); e != nil {
					log.Printf("unable to delete meta for collection: %s", e)
				}
			}
		} else {
			indexed = true
		}
	}
	return
}

func DoFileContent(mongo *mgo.Session, op *gtm.Op, config *configOptions) (ingestAttachment bool, err error) {
	if !config.IndexFiles {
		return
	}
	if fileNamespaces[op.Namespace] {
		err = AddFileContent(mongo, op, config)
		if config.ElasticMajorVersion >= 5 {
			if op.Data["file"] != "" {
				ingestAttachment = true
			}
		}
	}
	return
}

func DoResume(mongo *mgo.Session, op *gtm.Op, config *configOptions) (err error) {
	if config.Resume {
		err = SaveTimestamp(mongo, op, config.ResumeName)
	}
	return
}

func AddPatch(config *configOptions, elastic *elastigo.Conn, op *gtm.Op,
	objectId string, indexType *indexTypeMapping, meta *indexingMeta) (err error) {
	var merges []interface{}
	var toJson []byte
	if op.IsUpdate() {
		var params = make(map[string]interface{})
		if meta.Routing != "" {
			params["routing"] = meta.Routing
		}
		var resp elastigo.BaseResponse
		if resp, err = elastic.Get(indexType.Index, indexType.Type, objectId, params); err == nil {
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
					var fromJson, mergeDoc []byte
					if fromJson, err = json.Marshal(src); err == nil {
						if toJson, err = json.Marshal(op.Data); err == nil {
							if mergeDoc, err = jsonpatch.CreateMergePatch(fromJson, toJson); err == nil {
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
				err = errors.New("last document revision not found")
			}
		}
	} else {
		if toJson, err = json.Marshal(op.Data); err == nil {
			merge := make(map[string]interface{})
			merge["v"] = 1
			merge["ts"] = op.Timestamp >> 32
			merge["p"] = string(toJson)
			merges = append(merges, merge)
			op.Data[config.MergePatchAttr] = merges
		}
	}
	return
}

func DoIndexing(config *configOptions, mongo *mgo.Session, indexer *elastigo.BulkIndexer, elastic *elastigo.Conn, op *gtm.Op, ingestAttachment bool) (indexed bool, err error) {
	meta := ParseIndexMeta(op.Data)
	PrepareDataForIndexing(config, op)
	objectId, indexType := OpIdToString(op), IndexTypeMapping(op)
	if config.EnablePatches {
		if patchNamespaces[op.Namespace] {
			if e := AddPatch(config, elastic, op, objectId, indexType, meta); e != nil {
				log.Printf("unable to save json-patch info: %s", e)
			}
		}
	}
	if ingestAttachment {
		if err = IngestAttachment(elastic, indexType.Index, indexType.Type, objectId, op.Data, meta); err == nil {
			indexed = true
		}
	} else {
		if err = indexer.Index(indexType.Index, indexType.Type, objectId, "", meta.Routing, "", nil, op.Data); err == nil {
			indexed = true
		}
	}
	if meta.Empty() == false {
		if e := SetIndexMeta(mongo, op.Namespace, objectId, meta); e != nil {
			log.Printf("unable to save routing info: %s", e)
		}
	}
	return
}

func DoIndex(config *configOptions, mongo *mgo.Session, indexer *elastigo.BulkIndexer, elastic *elastigo.Conn, op *gtm.Op, ingestAttachment bool) (indexed bool, err error) {
	if err = MapData(op); err == nil {
		if op.Data != nil {
			indexed, err = DoIndexing(config, mongo, indexer, elastic, op, ingestAttachment)
		} else if op.IsUpdate() {
			indexed = DoDelete(mongo, indexer, op)
		} else {
			indexed = true
		}
	}
	return
}

func DropDBMeta(session *mgo.Session, db string) (err error) {
	col := session.DB("monstache").C("meta")
	q := bson.M{"db": db}
	_, err = col.RemoveAll(q)
	return
}

func DropCollectionMeta(session *mgo.Session, namespace string) (err error) {
	col := session.DB("monstache").C("meta")
	q := bson.M{"namespace": namespace}
	_, err = col.RemoveAll(q)
	return
}

func (meta *indexingMeta) Empty() bool {
	return meta.Routing == "" && meta.Index == "" && meta.Type == ""
}

func SetIndexMeta(session *mgo.Session, namespace, id string, meta *indexingMeta) error {
	col := session.DB("monstache").C("meta")
	metaId := fmt.Sprintf("%s.%s", namespace, id)
	doc := make(map[string]interface{})
	doc["routing"] = meta.Routing
	doc["index"] = meta.Index
	doc["type"] = meta.Type
	doc["db"] = strings.SplitN(namespace, ".", 2)
	doc["namespace"] = namespace
	_, err := col.UpsertId(metaId, bson.M{"$set": doc})
	return err
}

func GetIndexMeta(session *mgo.Session, namespace, id string) (meta *indexingMeta) {
	meta = &indexingMeta{}
	col := session.DB("monstache").C("meta")
	doc := make(map[string]interface{})
	metaId := fmt.Sprintf("%s.%s", namespace, id)
	col.FindId(metaId).One(doc)
	if doc["routing"] != nil {
		meta.Routing = doc["routing"].(string)
	}
	if doc["index"] != nil {
		meta.Index = doc["index"].(string)
	}
	if doc["type"] != nil {
		meta.Type = doc["type"].(string)
	}
	col.RemoveId(metaId)
	return
}

func DoDelete(mongo *mgo.Session, indexer *elastigo.BulkIndexer, op *gtm.Op) (indexed bool) {
	objectId, indexType, meta := OpIdToString(op), IndexTypeMapping(op), &indexingMeta{}
	if mapEnvs != nil {
		if env := mapEnvs[op.Namespace]; env != nil && env.Routing {
			meta = GetIndexMeta(mongo, op.Namespace, objectId)
		}
	}
	if meta.Index != "" {
		indexType.Index = meta.Index
	}
	if meta.Type != "" {
		indexType.Type = meta.Type
	}
	indexer.Delete(indexType.Index, indexType.Type, "", meta.Routing, objectId)
	indexed = true
	return
}

func GtmDefaultSettings() gtmSettings {
	return gtmSettings{
		ChannelSize:    gtmChannelSizeDefault,
		BufferSize:     32,
		BufferDuration: "750ms",
		WorkerCount:    8,
		Ordering:       int(gtm.Document),
	}
}

func main() {
	log.SetPrefix("ERROR ")
	enabled := true
	config := &configOptions{
		MongoDialSettings:    mongoDialSettings{Timeout: -1},
		MongoSessionSettings: mongoSessionSettings{SocketTimeout: -1, SyncTimeout: -1},
		GtmSettings:          GtmDefaultSettings(),
	}
	config.ParseCommandLineFlags()
	if config.Version {
		fmt.Println(Version)
		os.Exit(0)
	}
	config.LoadConfigFile().SetDefaults()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	if err := config.ConfigHttpTransport(); err != nil {
		log.Panicf("Unable to configure HTTP transport: %s", err)
	}
	mongo, err := config.DialMongo()
	if err != nil {
		log.Panicf("Unable to connect to mongodb using URL %s: %s", config.MongoUrl, err)
	}
	defer mongo.Close()
	if config.Resume && config.ResumeWriteUnsafe {
		if config.ClusterName == "" {
			mongo.SetSafe(nil)
		}
	}
	if config.MongoSessionSettings.SocketTimeout != -1 {
		timeOut := time.Duration(config.MongoSessionSettings.SocketTimeout) * time.Second
		mongo.SetSocketTimeout(timeOut)
	}
	if config.MongoSessionSettings.SyncTimeout != -1 {
		timeOut := time.Duration(config.MongoSessionSettings.SyncTimeout) * time.Second
		mongo.SetSyncTimeout(timeOut)
	}
	elastic := elastigo.NewConn()
	if config.ElasticUrl != "" {
		elastic.SetFromUrl(config.ElasticUrl)
	}
	if config.ElasticHosts != nil {
		elastic.SetHosts(config.ElasticHosts)
	}
	if config.Verbose {
		elastic.RequestTracer = TraceRequest
	}
	if config.Gzip {
		elastic.Gzip = true
	}
	if config.ElasticVersion == "" {
		if err := TestElasticSearchConn(elastic, config); err != nil {
			host := elastic.Domain
			if len(config.ElasticHosts) > 0 {
				host = config.ElasticHosts[0]
			}
			log.Panicf("Unable to validate connection to elasticsearch using %s://%s:%s: %s",
				elastic.Protocol, host, elastic.Port, err)
		}
	} else {
		if err := config.ParseElasticSearchVersion(config.ElasticVersion); err != nil {
			log.Panicf("Elasticsearch version must conform to major.minor.fix: %s", err)
		}
	}
	indexer := elastic.NewBulkIndexerErrors(config.ElasticMaxConns, config.ElasticRetrySeconds)
	if config.ElasticMaxDocs != 0 {
		indexer.BulkMaxDocs = config.ElasticMaxDocs
	}
	if config.ElasticMaxBytes != 0 {
		indexer.BulkMaxBuffer = config.ElasticMaxBytes
	}
	indexer.BufferDelayMax = time.Duration(config.ElasticMaxSeconds) * time.Second
	indexer.Start()
	defer indexer.Stop()

	go func(mongo *mgo.Session, indexer *elastigo.BulkIndexer, config *configOptions) {
		<-sigs
		if config.ClusterName != "" {
			ResetClusterState(mongo, config)
		}
		mongo.Close()
		indexer.Flush()
		indexer.Stop()
		done <- true
	}(mongo, indexer, config)

	var after gtm.TimestampGenerator = nil
	if config.Resume {
		after = func(session *mgo.Session, options *gtm.Options) bson.MongoTimestamp {
			ts := gtm.LastOpTimestamp(session, options)
			if config.Replay {
				ts = 0
			} else if config.ResumeFromTimestamp != 0 {
				ts = bson.MongoTimestamp(config.ResumeFromTimestamp)
			} else {
				collection := session.DB("monstache").C("monstache")
				doc := make(map[string]interface{})
				collection.FindId(config.ResumeName).One(doc)
				if doc["ts"] != nil {
					ts = doc["ts"].(bson.MongoTimestamp)
				}
			}
			return ts
		}
	} else if config.Replay {
		after = func(session *mgo.Session, options *gtm.Options) bson.MongoTimestamp {
			return 0
		}
	}

	if config.IndexFiles {
		if len(config.FileNamespaces) == 0 {
			log.Fatalln("File indexing is ON but no file namespaces are configured")
		}
		for _, namespace := range config.FileNamespaces {
			if err := EnsureFileMapping(elastic, namespace, config); err != nil {
				panic(err)
			}
			if config.ElasticMajorVersion >= 5 {
				break
			}
		}
	}

	var filter gtm.OpFilter = nil
	filterChain := []gtm.OpFilter{NotMonstache, NotSystem, NotChunks}
	if config.NsRegex != "" {
		filterChain = append(filterChain, FilterWithRegex(config.NsRegex))
	}
	if config.NsExcludeRegex != "" {
		filterChain = append(filterChain, FilterInverseWithRegex(config.NsExcludeRegex))
	}
	if config.Worker != "" {
		workerFilter, err := consistent.ConsistentHashFilter(config.Worker, config.Workers)
		if err != nil {
			panic(err)
		}
		filterChain = append(filterChain, workerFilter)
	} else if config.Workers != nil {
		panic("workers configured but this worker is undefined. worker must be set to one of the workers.")
	}
	filter = gtm.ChainOpFilters(filterChain...)
	var oplogDatabaseName, oplogCollectionName, cursorTimeout *string
	if config.MongoOpLogDatabaseName != "" {
		oplogDatabaseName = &config.MongoOpLogDatabaseName
	}
	if config.MongoOpLogCollectionName != "" {
		oplogCollectionName = &config.MongoOpLogCollectionName
	}
	if config.MongoCursorTimeout != "" {
		cursorTimeout = &config.MongoCursorTimeout
	}
	if config.ClusterName != "" {
		if err = EnsureClusterTTL(mongo); err == nil {
			infoLog.Printf("Joined cluster %s", config.ClusterName)
		} else {
			log.Panicf("Unable to enable cluster mode: %s", err)
		}
		enabled, err = IsEnabledProcess(mongo, config)
		if err != nil {
			log.Panicf("Unable to determine enabled cluster process: %s", err)
		}
		if enabled {
			infoLog.Printf("Starting work for cluster %s", config.ClusterName)
		} else {
			infoLog.Printf("Pausing work for cluster %s", config.ClusterName)
			gtm.Pause()
		}
	}
	gtmBufferDuration, err := time.ParseDuration(config.GtmSettings.BufferDuration)
	if err != nil {
		log.Panicf("Unable to parse gtm buffer duration %s", config.GtmSettings.BufferDuration)
	}
	ops, errs := gtm.Tail(mongo, &gtm.Options{
		After:               after,
		Filter:              filter,
		OpLogDatabaseName:   oplogDatabaseName,
		OpLogCollectionName: oplogCollectionName,
		CursorTimeout:       cursorTimeout,
		ChannelSize:         config.GtmSettings.ChannelSize,
		Ordering:            gtm.OrderingGuarantee(config.GtmSettings.Ordering),
		WorkerCount:         config.GtmSettings.WorkerCount,
		BufferDuration:      gtmBufferDuration,
		BufferSize:          config.GtmSettings.BufferSize,
	})
	heartBeat := time.NewTicker(10 * time.Second)
	if config.ClusterName == "" {
		heartBeat.Stop()
	}
	exitStatus := 0
	for {
		select {
		case <-done:
			os.Exit(exitStatus)
		case <-heartBeat.C:
			if enabled {
				enabled = IsEnabledProcessId(mongo, config)
				if !enabled {
					infoLog.Printf("Pausing work for cluster %s", config.ClusterName)
					gtm.Pause()
				}
			} else {
				if enabled, err = IsEnabledProcess(mongo, config); err == nil {
					if enabled {
						infoLog.Printf("Resuming work for cluster %s", config.ClusterName)
						ResumeWork(mongo, config)
					}
				} else {
					errs <- err
				}
			}
		case err = <-errs:
			exitStatus = 1
			log.Println(err)
			if config.FailFast {
				os.Exit(exitStatus)
			}
		case indexErr := <-indexer.ErrorChannel:
			if indexErr.Buf != nil {
				errs <- fmt.Errorf("%s. Failed Request Body : %s", indexErr.Err, indexErr.Buf)
			} else {
				errs <- indexErr.Err
			}
		case op := <-ops:
			if !enabled {
				break
			}
			ingestAttachment, indexed := false, false
			if op.IsDrop() {
				if indexed, err = DoDrop(mongo, elastic, op, config); err != nil {
					errs <- err
				}
			} else if op.IsDelete() {
				indexed = DoDelete(mongo, indexer, op)
			} else if op.Data != nil {
				if ingestAttachment, err = DoFileContent(mongo, op, config); err != nil {
					errs <- err
				}
				if indexed, err = DoIndex(config, mongo, indexer, elastic, op, ingestAttachment); err != nil {
					errs <- err
				}
			}
			if indexed {
				if err = DoResume(mongo, op, config); err != nil {
					errs <- err
				}
			}
		}
	}
}
