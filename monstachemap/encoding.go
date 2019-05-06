package monstachemap

import (
    "time"
    "errors"
)

const timeJsonFormat = "2006-01-02T15:04:05.000Z07:00"

type Time struct {
	time.Time
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

/*func (tw *Time) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	t, err := time.Parse(`"`+timeJsonFormat+`"`, string(data))
	*tw = mtime{t}
	return err
}*/

func ConvertSliceForJSON(a []interface{}) []interface{} {
    var avs []interface{}
	for _, av := range a {
		var avc interface{}
		switch achild := av.(type) {
		case map[string]interface{}:
			avc = ConvertMapForJSON(achild)
		case []interface{}:
			avc = ConvertSliceForJSON(achild)
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
        case time.Time:
            o[k] = Time{child}
		default:
			o[k] = v
		}
	}
	return o
}
