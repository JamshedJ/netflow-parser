package stopwatch

import (
	"fmt"
	"time"
)

type Stopwatch struct {
	name        string
	startedAt   time.Time
	restartedAt time.Time
}

func New(name string) *Stopwatch {
	return &Stopwatch{
		name:        name,
		startedAt:   time.Now(),
		restartedAt: time.Now(),
	}
}

func (s *Stopwatch) Mark(action string) {
	fmt.Printf("| %-10s | %-30s | duration: %15s | overall: %15s |\n",
		s.name, action, time.Since(s.restartedAt), time.Since(s.startedAt))
	s.restartedAt = time.Now()
	return
}
