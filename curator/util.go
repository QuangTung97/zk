package curator

type parallelRunnerImpl struct {
	runners []SessionRunner
}

// NewParallelRunner creates a SessionRunner that calls methods of all input runners
func NewParallelRunner(runners ...SessionRunner) SessionRunner {
	return &parallelRunnerImpl{
		runners: runners,
	}
}

func (r *parallelRunnerImpl) Begin(client Client) {
	for _, runner := range r.runners {
		runner.Begin(client)
	}
}

func (r *parallelRunnerImpl) Retry() {
	for _, runner := range r.runners {
		runner.Retry()
	}
}

func (r *parallelRunnerImpl) End() {
	for _, runner := range r.runners {
		runner.End()
	}
}
