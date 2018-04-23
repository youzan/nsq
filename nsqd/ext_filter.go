package nsqd

import (
	"errors"
	"regexp"

	"github.com/gobwas/glob"
	"github.com/tidwall/gjson"
	"github.com/youzan/nsq/internal/ext"
)

// {
//	"ver":1,
//	"filter_ext_key":"xx",
//	"filter_data":"filterA",
// }
// {
//	"ver":2,
//	"filter_ext_key":"xx",
//	"filter_data":"regexp string",
// }
// {
//	"ver":3,
//	"filter_ext_key":"xx",
//	"filter_data":"glob rule",
// }
// ver is used to extend other filter type
// currently support equal, regexp, glob
// TODO: maybe support multi ext key rule chains
// such as, filter if match_rule1(ext_key1) and match_rule2(ext_key2) or match_rule3(ext_key3)
var (
	ErrNotSupportedFilter = errors.New("the filter type not supported")
	ErrInvalidFilter      = errors.New("invalid filter rule")
)

type ExtFilterData struct {
	Type           int               `json:"type,omitempty"`
	Inverse        bool              `json:"inverse,omitempty"`
	FilterExtKey   string            `json:"filter_ext_key,omitempty"`
	FilterData     string            `json:"filter_data,omitempty"`
	FilterDataList []MultiFilterData `json:"filter_data_list,omitempty"`
}

type MultiFilterData struct {
	FilterExtKey string `json:"filter_ext_key,omitempty"`
	FilterData   string `json:"filter_data,omitempty"`
}

type IExtFilter interface {
	Match(msg *Message) bool
}

func getMsgExtFilterStr(msg *Message, key string) string {
	filterData := gjson.GetBytes(msg.ExtBytes, key)
	var msgExtFilterHeader string
	if filterData.Type == gjson.String {
		msgExtFilterHeader = filterData.String()
	}
	return msgExtFilterHeader
}

func isMatchedJsonValue(filterData gjson.Result, match interface{}) bool {
	switch t := match.(type) {
	case string:
		if filterData.Type == gjson.String && t == filterData.String() {
			return true
		}
		if t == "" {
			// empty rule
			return true
		}
	case []byte:
		if filterData.Type == gjson.String && string(t) == filterData.String() {
			return true
		}
		if len(t) == 0 {
			// empty rule
			return true
		}
	}
	return false
}

type extReFilter struct {
	re     *regexp.Regexp
	extKey string
}

func (crf *extReFilter) Match(msg *Message) bool {
	if msg.ExtVer != ext.JSON_HEADER_EXT_VER {
		return false
	}
	d := getMsgExtFilterStr(msg, crf.extKey)
	return crf.re.MatchString(d)
}

type extExactlyFilter struct {
	match  string
	extKey string
}

func (cef *extExactlyFilter) Match(msg *Message) bool {
	if msg.ExtVer != ext.JSON_HEADER_EXT_VER {
		return false
	}
	filterData := gjson.GetBytes(msg.ExtBytes, cef.extKey)
	return isMatchedJsonValue(filterData, cef.match)
}

type extGlobFilter struct {
	globF  glob.Glob
	extKey string
}

func (cgf *extGlobFilter) Match(msg *Message) bool {
	if msg.ExtVer != ext.JSON_HEADER_EXT_VER {
		return false
	}
	d := getMsgExtFilterStr(msg, cgf.extKey)
	return cgf.globF.Match(d)
}

type extMultiFilter struct {
	chainFilters []IExtFilter
	relation     string
}

func (f *extMultiFilter) Match(msg *Message) bool {
	for _, r := range f.chainFilters {
		if r.Match(msg) {
			if f.relation == "any" {
				return true
			}
		} else {
			if f.relation == "all" {
				return false
			}
		}
	}
	if f.relation == "all" {
		return true
	}
	return false
}

func NewExtFilter(filter ExtFilterData) (IExtFilter, error) {
	var cf IExtFilter
	var err error
	if filter.FilterExtKey == "" {
		return nil, ErrInvalidFilter
	}
	if filter.FilterData == "" {
		if filter.Type != 4 {
			return nil, ErrInvalidFilter
		}
	}

	if filter.Type == 1 {
		cef := &extExactlyFilter{}
		cef.match = filter.FilterData
		cef.extKey = filter.FilterExtKey
		cf = cef
	} else if filter.Type == 2 {
		crf := &extReFilter{}
		crf.re, err = regexp.Compile(filter.FilterData)
		if err != nil {
			return nil, err
		}
		crf.extKey = filter.FilterExtKey
		cf = crf
	} else if filter.Type == 3 {
		cgf := &extGlobFilter{}
		cgf.globF, err = glob.Compile(filter.FilterData)
		if err != nil {
			return nil, err
		}
		cgf.extKey = filter.FilterExtKey
		cf = cgf
	} else if filter.Type == 4 {
		mf := &extMultiFilter{}
		filters := filter.FilterDataList
		if len(filters) == 0 {
			return nil, ErrInvalidFilter
		}
		mf.relation = filter.FilterExtKey
		for _, f := range filters {
			ef := &extExactlyFilter{}
			if f.FilterExtKey == "" || f.FilterData == "" {
				return nil, ErrInvalidFilter
			}
			ef.match = f.FilterData
			ef.extKey = f.FilterExtKey
			mf.chainFilters = append(mf.chainFilters, ef)
		}
		cf = mf
	} else {
		return nil, ErrNotSupportedFilter
	}
	return cf, nil
}
