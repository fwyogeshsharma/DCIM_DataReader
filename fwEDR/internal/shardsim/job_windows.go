//go:build windows

package shardsim

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

// job wraps a Windows Job Object configured with KILL_ON_JOB_CLOSE: when EDR
// exits (and the handle closes), the OS terminates every assigned responder.
type job struct{ h windows.Handle }

func newJob() (*job, error) {
	h, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, err
	}
	var info windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(
		h,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	); err != nil {
		windows.CloseHandle(h)
		return nil, err
	}
	return &job{h: h}, nil
}

func (j *job) assign(pid int) error {
	ph, err := windows.OpenProcess(windows.PROCESS_ALL_ACCESS, false, uint32(pid))
	if err != nil {
		return err
	}
	defer windows.CloseHandle(ph)
	return windows.AssignProcessToJobObject(j.h, ph)
}

func (j *job) close() {
	if j != nil && j.h != 0 {
		windows.CloseHandle(j.h)
		j.h = 0
	}
}
