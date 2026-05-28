//go:build !windows

package shardsim

// On non-Windows there is no Job Object; responders are stopped via Spawner.Stop
// (best-effort kill). newJob never fails so the spawn path stays uniform.
type job struct{}

func newJob() (*job, error)     { return &job{}, nil }
func (j *job) assign(int) error { return nil }
func (j *job) close()           {}
