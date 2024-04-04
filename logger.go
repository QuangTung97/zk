package zk

import (
	"log"
)

// Logger ...
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type defaultLoggerImpl struct {
}

func (*defaultLoggerImpl) Infof(format string, args ...any) {
	log.Printf("[INFO] [ZK] "+format, args...)
}

func (*defaultLoggerImpl) Warnf(format string, args ...any) {
	log.Printf("[WARN] [ZK] "+format, args...)
}

func (*defaultLoggerImpl) Errorf(format string, args ...any) {
	log.Printf("[ERROR] [ZK] "+format, args...)
}
