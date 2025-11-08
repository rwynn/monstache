module github.com/rwynn/monstache/v6

go 1.25.0

require (
	github.com/BurntSushi/toml v1.2.1
	github.com/aws/aws-sdk-go v1.55.6
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf
	github.com/evanphx/json-patch v5.6.0+incompatible
	github.com/fsnotify/fsnotify v1.5.1
	github.com/olivere/elastic/v7 v7.0.31
	github.com/robertkrimen/otto v0.0.0-20211024170158-b87d35c0b86f
	github.com/rwynn/gtm/v2 v2.1.5
	go.mongodb.org/mongo-driver v1.17.4
	gopkg.in/Graylog2/go-gelf.v2 v2.0.0-20191017102106-1550ee647df0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/serialx/hashring v0.0.0-20200727003509-22c0c7ab6b1b // indirect
	github.com/stretchr/testify v1.8.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	golang.org/x/crypto v0.35.0 // indirect
	golang.org/x/mod v0.22.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/telemetry v0.0.0-20240522233618-39ace7a40ae7 // indirect
	golang.org/x/text v0.22.0 // indirect
	golang.org/x/tools v0.29.0 // indirect
	golang.org/x/vuln v1.1.4 // indirect
	gopkg.in/sourcemap.v1 v1.0.5 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

tool golang.org/x/vuln/cmd/govulncheck

ignore ./docker
