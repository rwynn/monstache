GB =go build
BUILD_DIR =build

ENV_LINUX =env GOOS=linux GOARCH=amd64
ENV_WIN =env GOOS=windows GOARCH=amd64
ENV_DARWIN =env GOOS=darwin GOARCH=amd64

OUT_LINUX_DIR =$(BUILD_DIR)/linux-amd64
OUT_WIN_DIR =$(BUILD_DIR)/windows-amd64
OUT_DARWIN_DIR =$(BUILD_DIR)/darwin-amd64

OUT_LINUX =$(OUT_LINUX_DIR)/monstache
OUT_WIN =$(OUT_WIN_DIR)/monstache.exe
OUT_DARWIN =$(OUT_DARWIN_DIR)/monstache

SHA256_LINUX =$(OUT_LINUX_DIR)/sha256.txt
SHA256_WIN =$(OUT_WIN_DIR)/sha256.txt
SHA256_DARWIN =$(OUT_DARWIN_DIR)/sha256.txt

MD5_LINUX =$(OUT_LINUX_DIR)/md5.txt
MD5_WIN =$(OUT_WIN_DIR)/md5.txt
MD5_DARWIN =$(OUT_DARWIN_DIR)/md5.txt

GIT_TAG =$(shell git rev-parse --short HEAD)

LDFLAGS =-ldflags="-s -w"

TARGET = monstache

all: $(TARGET)

release: $(TARGET).go
	$(ENV_LINUX) $(GB) $(LDFLAGS) -v -o $(OUT_LINUX)

$(TARGET): $(TARGET).go
	$(ENV_LINUX) $(GB) $(LDFLAGS) -v -o $(OUT_LINUX)
	$(ENV_WIN) $(GB) $(LDFLAGS) -v -o $(OUT_WIN)
	$(ENV_DARWIN) $(GB) $(LDFLAGS) -v -o $(OUT_DARWIN)
	sha256sum $(OUT_LINUX) > $(SHA256_LINUX)
	sha256sum $(OUT_WIN) > $(SHA256_WIN)
	sha256sum $(OUT_DARWIN) > $(SHA256_DARWIN)
	md5sum $(OUT_LINUX) > $(MD5_LINUX)
	md5sum $(OUT_WIN) > $(MD5_WIN)
	md5sum $(OUT_DARWIN) > $(MD5_DARWIN)
	zip -r $(BUILD_DIR)/$(TARGET)-$(GIT_TAG).zip $(BUILD_DIR)
clean:
	$(RM) -R $(BUILD_DIR)
