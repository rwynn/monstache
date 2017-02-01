GB =go build
BUILD_DIR =build

ENV_LINUX =env GOOS=linux GOARCH=amd64
ENV_WIN =env GOOS=windows GOARCH=amd64
ENV_DARWIN =env GOOS=darwin GOARCH=amd64

OUT_LINUX =-o $(BUILD_DIR)/linux-amd64/monstache
OUT_WIN =-o $(BUILD_DIR)/windows-amd64/monstache.exe
OUT_DARWIN =-o $(BUILD_DIR)/darwin-amd64/monstache

GIT_TAG =$(shell git rev-parse --short HEAD)

LDFLAGS =-ldflags="-s -w"

TARGET = monstache

all: $(TARGET)

$(TARGET): $(TARGET).go
	$(ENV_LINUX) $(GB) $(LDFLAGS) -v $(OUT_LINUX)
	$(ENV_WIN) $(GB) $(LDFLAGS) -v $(OUT_WIN)
	$(ENV_DARWIN) $(GB) $(LDFLAGS) -v $(OUT_DARWIN)
	zip -r $(BUILD_DIR)/$(TARGET)-$(GIT_TAG).zip $(BUILD_DIR)
clean:
	$(RM) -R $(BUILD_DIR)
