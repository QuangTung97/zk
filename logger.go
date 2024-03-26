package zk

import (
	"log"
)

type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
}

type defaultLoggerImpl struct {
}

func (*defaultLoggerImpl) Infof(format string, args ...any) {
	log.Printf("[INFO] [ZK] "+format, args...)
}

func (*defaultLoggerImpl) Warnf(format string, args ...any) {
	log.Printf("[WARN] [ZK] "+format, args...)
}
