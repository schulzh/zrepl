package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"time"
)

type Level int

func (l Level) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.String())
}

const (
	Debug Level = iota
	Info
	Warn
	Error
)

func (l Level) Short() string {
	switch l {
	case Debug:
		return "DEBG"
	case Info:
		return "INFO"
	case Warn:
		return "WARN"
	case Error:
		return "ERRO"
	default:
		return fmt.Sprintf("%s", l)
	}
}

func (l Level) String() string {
	switch l {
	case Debug:
		return "debug"
	case Info:
		return "info"
	case Warn:
		return "warn"
	case Error:
		return "error"
	default:
		return fmt.Sprintf("%s", string(l))
	}
}

func ParseLevel(s string) (l Level, err error) {
	for _, l := range AllLevels {
		if s == l.String() {
			return l, nil
		}
	}
	return -1, errors.Errorf("unknown level '%s'", s)
}

// Levels ordered least severe to most severe
var AllLevels []Level = []Level{Debug, Info, Warn, Error}

type Fields map[string]interface{}

type Entry struct {
	Level   Level
	Message string
	Time    time.Time
	Fields  Fields
}

type Outlet interface {
	// Note: os.Stderr is also used by logger.Logger for reporting errors returned by outlets
	//       => you probably don't want to log there
	WriteEntry(ctx context.Context, entry Entry) error
}

type Outlets struct {
	reg map[Level][]Outlet
	e   Outlet
}

func NewOutlets(loggerErrorOutlet Outlet) *Outlets {
	return &Outlets{
		make(map[Level][]Outlet, len(AllLevels)),
		loggerErrorOutlet,
	}
}

func (os *Outlets) Add(outlet Outlet, minLevel Level) {
	for _, l := range AllLevels[minLevel:] {
		os.reg[l] = append(os.reg[l], outlet)
	}
}

func (os *Outlets) Get(level Level) []Outlet {
	return os.reg[level]
}

func (os *Outlets) GetLoggerErrorOutlet() Outlet {
	return os.e
}
