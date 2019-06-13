package monstachemap

import (
	"encoding/hex"
	"errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

const timeJsonFormat = "2006-01-02T15:04:05.000Z07:00"

type Time struct {
	time.Time
}

type Binary struct {
	primitive.Binary
}

func (t Time) MarshalJSON() ([]byte, error) {
	if y := t.Year(); y < 0 || y >= 10000 {
		return nil, errors.New("Time.MarshalJSON: year outside of range [0,9999]")
	}
	b := make([]byte, 0, len(timeJsonFormat)+2)
	b = append(b, '"')
	b = t.AppendFormat(b, timeJsonFormat)
	b = append(b, '"')
	return b, nil
}

func (bi Binary) MarshalJSON() ([]byte, error) {
	hexStr := hex.EncodeToString(bi.Data)
	b := make([]byte, 0, len(hexStr)+2)
	b = append(b, '"')
	b = append(b, []byte(hexStr)...)
	b = append(b, '"')
	return b, nil
}

func ConvertSliceForJSON(a []interface{}) []interface{} {
	var avs []interface{}
	for _, av := range a {
		var avc interface{}
		switch achild := av.(type) {
		case map[string]interface{}:
			avc = ConvertMapForJSON(achild)
		case []interface{}:
			avc = ConvertSliceForJSON(achild)
		case primitive.Binary:
			avc = Binary{achild}
		case time.Time:
			avc = Time{achild}
		default:
			avc = av
		}
		avs = append(avs, avc)
	}
	return avs
}

func ConvertMapForJSON(m map[string]interface{}) map[string]interface{} {
	o := map[string]interface{}{}
	for k, v := range m {
		switch child := v.(type) {
		case map[string]interface{}:
			o[k] = ConvertMapForJSON(child)
		case []interface{}:
			o[k] = ConvertSliceForJSON(child)
		case primitive.Binary:
			o[k] = Binary{child}
		case time.Time:
			o[k] = Time{child}
		default:
			o[k] = v
		}
	}
	return o
}
