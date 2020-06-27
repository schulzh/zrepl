package job

import (
	"github.com/pkg/errors"
	"github.com/zrepl/zrepl/config"
	"github.com/zrepl/zrepl/daemon/filters"
	"github.com/zrepl/zrepl/endpoint"
	"github.com/zrepl/zrepl/zfs"
)

type SendingJobConfig interface {
	GetFilesystems() config.FilesystemsFilter
	GetSendOptions() *config.SendOptions // must not be nil
}

func buildSenderConfig(in SendingJobConfig, jobID endpoint.JobID) (*endpoint.SenderConfig, error) {

	fsf, err := filters.DatasetMapFilterFromConfig(in.GetFilesystems())
	if err != nil {
		return nil, errors.Wrap(err, "cannot build filesystem filter")
	}

	incrementalStepProtectionStrategyKind, err := endpoint.StepProtectionStrategyKindString(in.GetSendOptions().IncrementalStepProtection)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse incremental step protection setting")
	}

	return &endpoint.SenderConfig{
		FSF:                                   fsf,
		Encrypt:                               &zfs.NilBool{B: in.GetSendOptions().Encrypted},
		IncrementalStepProtectionStrategyKind: incrementalStepProtectionStrategyKind,
		JobID:                                 jobID,
	}, nil
}
