package endpoint

import (
	"context"
	"fmt"
	"regexp"

	"github.com/pkg/errors"

	"github.com/zrepl/zrepl/zfs"
)

//go:generate enumer -type=StepProtectionStrategyKind -json -transform=snake -trimprefix=StepProtectionStrategyKind
type StepProtectionStrategyKind int

const (
	StepProtectionStrategyKindHolds StepProtectionStrategyKind = 1 << iota
	StepProtectionStrategyKindBookmarks
	StepProtectionStrategyKindNone
)

type StepProtectionStrategy interface {
	Kind() StepProtectionStrategyKind
	PreSend(ctx context.Context, jid JobID, sendArgs *zfs.ZFSSendArgsValidated) (keep []Abstraction, err error)
}

func StepProtectionStrategyFromKind(k StepProtectionStrategyKind) StepProtectionStrategy {
	switch k {
	case StepProtectionStrategyKindNone:
		return StepProtectionStrategyNone{}
	case StepProtectionStrategyKindBookmarks:
		return StepProtectionStrategyBookmarks{}
	case StepProtectionStrategyKindHolds:
		return StepProtectionStrategyHolds{}
	default:
		panic(fmt.Sprintf("unreachable: %q %T", k, k))
	}
}

type StepProtectionStrategyNone struct{}

func (s StepProtectionStrategyNone) Kind() StepProtectionStrategyKind {
	return StepProtectionStrategyKindNone
}

func (s StepProtectionStrategyNone) PreSend(ctx context.Context, jid JobID, sendArgs *zfs.ZFSSendArgsValidated) (keep []Abstraction, err error) {
	return nil, nil
}

type StepProtectionStrategyBookmarks struct{}

func (s StepProtectionStrategyBookmarks) Kind() StepProtectionStrategyKind {
	return StepProtectionStrategyKindBookmarks
}

func (s StepProtectionStrategyBookmarks) PreSend(ctx context.Context, jid JobID, sendArgs *zfs.ZFSSendArgsValidated) (keep []Abstraction, err error) {

	if sendArgs.FromVersion != nil {
		from, err := BookmarkStep(ctx, sendArgs.FS, *sendArgs.FromVersion, jid)
		if err != nil {
			if err == zfs.ErrBookmarkCloningNotSupported {
				getLogger(ctx).WithField("step_protection_strategy", s).
					WithField("bookmark", sendArgs.From.FullPath(sendArgs.FS)).
					Info("bookmark cloning is not supported, speculating that `from` will not be destroyed until step is done")
			} else {
				return nil, err
			}
		}
		keep = append(keep, from)
	}
	to, err := BookmarkStep(ctx, sendArgs.FS, sendArgs.ToVersion, jid)
	if err != nil {
		return nil, err
	}
	keep = append(keep, to)

	return keep, nil
}

type StepProtectionStrategyHolds struct{}

func (s StepProtectionStrategyHolds) Kind() StepProtectionStrategyKind {
	return StepProtectionStrategyKindHolds
}

func (s StepProtectionStrategyHolds) PreSend(ctx context.Context, jid JobID, sendArgs *zfs.ZFSSendArgsValidated) (keep []Abstraction, err error) {

	if sendArgs.FromVersion != nil {
		if sendArgs.FromVersion.Type == zfs.Bookmark {
			getLogger(ctx).WithField("step_protection_strategy", s).WithField("fromVersion", sendArgs.FromVersion.FullPath(sendArgs.FS)).
				Debug("cannot hold a bookmark, trying to fall back to a step bookmark")
			from, err := BookmarkStep(ctx, sendArgs.FS, *sendArgs.FromVersion, jid)
			if err != nil {
				if err == zfs.ErrBookmarkCloningNotSupported {
					getLogger(ctx).WithField("step_protection_strategy", s).
						WithField("bookmark", sendArgs.From.FullPath(sendArgs.FS)).
						Info("bookmark cloning is not supported, speculating that `from` will not be destroyed until step is done")
				} else {
					return nil, err
				}
			}
			keep = append(keep, from)
		} else {
			from, err := HoldStep(ctx, sendArgs.FS, *sendArgs.FromVersion, jid)
			if err != nil {
				return nil, err
			}
			keep = append(keep, from)
		}
		// fallthrough
	}

	to, err := HoldStep(ctx, sendArgs.FS, sendArgs.ToVersion, jid)
	if err != nil {
		return nil, err
	}
	keep = append(keep, to)

	return keep, nil
}

var stepHoldTagRE = regexp.MustCompile("^zrepl_STEP_J_(.+)")

func StepHoldTag(jobid JobID) (string, error) {
	return stepHoldTagImpl(jobid.String())
}

func stepHoldTagImpl(jobid string) (string, error) {
	t := fmt.Sprintf("zrepl_STEP_J_%s", jobid)
	if err := zfs.ValidHoldTag(t); err != nil {
		return "", err
	}
	return t, nil
}

// err != nil always means that the bookmark is not a step bookmark
func ParseStepHoldTag(tag string) (JobID, error) {
	match := stepHoldTagRE.FindStringSubmatch(tag)
	if match == nil {
		return JobID{}, fmt.Errorf("parse hold tag: match regex %q", stepHoldTagRE)
	}
	jobID, err := MakeJobID(match[1])
	if err != nil {
		return JobID{}, errors.Wrap(err, "parse hold tag: invalid job id field")
	}
	return jobID, nil
}

const stepBookmarkNamePrefix = "zrepl_STEP"

// v must be validated by caller
func StepBookmarkName(fs string, guid uint64, id JobID) (string, error) {
	return stepBookmarkNameImpl(fs, guid, id.String())
}

func stepBookmarkNameImpl(fs string, guid uint64, jobid string) (string, error) {
	return makeJobAndGuidBookmarkName(stepBookmarkNamePrefix, fs, guid, jobid)
}

// name is the full bookmark name, including dataset path
//
// err != nil always means that the bookmark is not a step bookmark
func ParseStepBookmarkName(fullname string) (guid uint64, jobID JobID, err error) {
	guid, jobID, err = parseJobAndGuidBookmarkName(fullname, stepBookmarkNamePrefix)
	if err != nil {
		err = errors.Wrap(err, "parse step bookmark name") // no shadow!
	}
	return guid, jobID, err
}

// idempotently hold `version`
func HoldStep(ctx context.Context, fs string, v zfs.FilesystemVersion, jobID JobID) (Abstraction, error) {
	if !v.IsSnapshot() {
		panic(fmt.Sprintf("version must be a snapshot got %#v", v))
	}

	tag, err := StepHoldTag(jobID)
	if err != nil {
		return nil, errors.Wrap(err, "step hold tag")
	}

	if err := zfs.ZFSHold(ctx, fs, v, tag); err != nil {
		return nil, errors.Wrap(err, "step hold: zfs")
	}

	return &holdBasedAbstraction{
		Type:              AbstractionStepHold,
		FS:                fs,
		Tag:               tag,
		JobID:             jobID,
		FilesystemVersion: v,
	}, nil

}

// returns ErrBookmarkCloningNotSupported if version is a bookmark and bookmarking bookmarks is not supported by ZFS
func BookmarkStep(ctx context.Context, fs string, v zfs.FilesystemVersion, jobID JobID) (Abstraction, error) {

	bmname, err := StepBookmarkName(fs, v.Guid, jobID)
	if err != nil {
		return nil, errors.Wrap(err, "create step bookmark: determine bookmark name")
	}
	// idempotently create bookmark
	stepBookmark, err := zfs.ZFSBookmark(ctx, fs, v, bmname)
	if err != nil {
		if err == zfs.ErrBookmarkCloningNotSupported {
			// TODO we could actually try to find a local snapshot that has the requested GUID
			// 		however, the replication algorithm prefers snapshots anyways, so this quest
			// 		is most likely not going to be successful. Also, there's the possibility that
			//      the caller might want to filter what snapshots are eligibile, and this would
			//      complicate things even further.
			return nil, err // TODO go1.13 use wrapping
		}
		return nil, errors.Wrap(err, "create step bookmark: zfs")
	}
	return &bookmarkBasedAbstraction{
		Type:              AbstractionStepBookmark,
		FS:                fs,
		FilesystemVersion: stepBookmark,
		JobID:             jobID,
	}, nil
}

var _ BookmarkExtractor = StepBookmarkExtractor

func StepBookmarkExtractor(fs *zfs.DatasetPath, v zfs.FilesystemVersion) (_ Abstraction) {
	if v.Type != zfs.Bookmark {
		panic("impl error")
	}

	fullname := v.ToAbsPath(fs)

	guid, jobid, err := ParseStepBookmarkName(fullname)
	if guid != v.Guid {
		// TODO log this possibly tinkered-with bookmark
		return nil
	}
	if err == nil {
		bm := &bookmarkBasedAbstraction{
			Type:              AbstractionStepBookmark,
			FS:                fs.ToString(),
			FilesystemVersion: v,
			JobID:             jobid,
		}
		return bm
	}
	return nil
}

var _ HoldExtractor = StepHoldExtractor

func StepHoldExtractor(fs *zfs.DatasetPath, v zfs.FilesystemVersion, holdTag string) Abstraction {
	if v.Type != zfs.Snapshot {
		panic("impl error")
	}

	jobID, err := ParseStepHoldTag(holdTag)
	if err == nil {
		return &holdBasedAbstraction{
			Type:              AbstractionStepHold,
			FS:                fs.ToString(),
			Tag:               holdTag,
			FilesystemVersion: v,
			JobID:             jobID,
		}
	}
	return nil
}
