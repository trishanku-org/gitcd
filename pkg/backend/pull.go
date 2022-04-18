package backend

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"
	"github.com/trishanku/gitcd/pkg/git"
	"github.com/trishanku/gitcd/pkg/git/tree/mutation"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type PullOptionFunc func(*puller) error

func NewPull(optFns ...PullOptionFunc) (err error) {
	var p = &puller{}

	for _, fn := range optFns {
		if err = fn(p); err != nil {
			return
		}
	}

	return
}

type pullOpts struct{}

var PullOptions = pullOpts{}

func (pullOpts) WithBackend(kvs etcdserverpb.KVServer) PullOptionFunc {
	return func(p *puller) (err error) {
		var b *backend

		if b, err = checkIsBackend(kvs); err != nil {
			return
		}

		p.backend = b
		p.merger = b.repo.Merger()
		return
	}
}

type mergerOptionFunc func(git.Merger) error

func (pullOpts) withMergerOption(mFn mergerOptionFunc) PullOptionFunc {
	return func(p *puller) error {
		if p.merger == nil {
			return errors.New("error configuring merger before it has been initialized")
		}

		return mFn(p.merger)
	}
}

func (opts pullOpts) WithMergeConfictResolution(conflictResolution git.MergeConfictResolution) PullOptionFunc {
	return opts.withMergerOption(func(merger git.Merger) error {
		merger.SetConflictResolution(conflictResolution)
		return nil
	})
}

func (opts pullOpts) WithMergeRetentionPolicy(retentionPolicy git.MergeRetentionPolicy) PullOptionFunc {
	return opts.withMergerOption(func(merger git.Merger) error {
		merger.SetRetentionPolicy(retentionPolicy)
		return nil
	})
}

func (pullOpts) WithRemoteName(remoteName git.RemoteName) PullOptionFunc {
	return func(p *puller) error {
		p.remoteName = remoteName
		return nil
	}
}

func (pullOpts) WithRemoteDataRefName(remoteDataRefName git.ReferenceName) PullOptionFunc {
	return func(p *puller) error {
		p.remoteDataRefName = remoteDataRefName
		return nil
	}
}

func (pullOpts) WithRemoteMetaRefName(remoteMetaRefName git.ReferenceName) PullOptionFunc {
	return func(p *puller) error {
		p.remoteMetaRefName = remoteMetaRefName
		return nil
	}
}

func (pullOpts) WithNoFastForward(noFastForward bool) PullOptionFunc {
	return func(p *puller) error {
		p.noFastForward = noFastForward
		return nil
	}
}

func (pullOpts) WithTicker(ticker <-chan time.Time) PullOptionFunc {
	return func(p *puller) error {
		p.ticker = ticker
		return nil
	}
}

func (pullOpts) WithLogger(log logr.Logger) PullOptionFunc {
	return func(p *puller) error {
		p.log = log
		return nil
	}
}

func (pullOpts) WithContext(ctx context.Context) PullOptionFunc {
	return func(p *puller) error {
		go func() {
			if err := p.Run(ctx); err != nil {
				p.log.Error(err, "Error running. Terminating...")
			}
		}()
		return nil
	}
}

func (pullOpts) WithOnce(ctx context.Context) PullOptionFunc {
	return func(p *puller) error { return p.pull(ctx) }
}

// puller helps pull changes from a configured remote Git repository into the backend.
type puller struct {
	backend *backend
	merger  git.Merger

	remoteName        git.RemoteName
	remoteDataRefName git.ReferenceName
	remoteMetaRefName git.ReferenceName

	noFastForward bool

	ticker <-chan time.Time
	log    logr.Logger
}

func (p *puller) getRemote(ctx context.Context) (r git.Remote, err error) {
	var (
		b  = p.backend
		rc git.RemoteCollection
	)

	if rc, err = b.repo.Remotes(); err != nil {
		return
	}

	defer rc.Close()

	r, err = rc.Get(ctx, p.remoteName)
	return
}

func (p *puller) fetch(ctx context.Context) (err error) {
	var remote git.Remote

	if remote, err = p.getRemote(ctx); err != nil {
		return
	}

	defer remote.Close()

	err = remote.Fetch(ctx, nil)
	return
}

func (p *puller) getRemoteDataHead(ctx context.Context) (c git.Commit, err error) {
	return p.backend.getHead(ctx, p.remoteDataRefName)
}

func (p *puller) getRemoteMetadataHead(ctx context.Context) (c git.Commit, err error) {
	return p.backend.getHead(ctx, p.remoteMetaRefName)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}

	return b
}

func (p *puller) createMergeCommit(ctx context.Context, message string, mergeTreeID git.ObjectID, parents ...git.Commit) (newHeadID git.ObjectID, err error) {
	newHeadID, err = p.backend.createCommit(
		ctx,
		message,
		mergeTreeID,
		func(ctx context.Context, cb git.CommitBuilder) (err error) {
			for _, p := range parents {
				if p == nil {
					continue
				}

				if err = cb.AddParents(p); err != nil {
					return
				}
			}

			return
		},
	)
	return
}

func (p *puller) mutateTree(
	ctx context.Context,
	t git.Tree,
	contents map[string][]byte,
) (
	mutated bool,
	newTreeID git.ObjectID,
	err error,
) {
	var (
		b  = p.backend
		tm *mutation.TreeMutation
	)

	for p, content := range contents {
		if tm, err = mutation.AddMutation(
			tm,
			p,
			func(ctx context.Context, tb git.TreeBuilder, entryName string, te git.TreeEntry) (mutated bool, err error) {
				mutated, err = b.addOrReplaceTreeEntry(ctx, tb, entryName, content, te)
				return
			},
		); err != nil {
			return
		}
	}

	mutated, newTreeID, err = mutation.MutateTree(ctx, b.repo, t, tm, false)
	return
}

func (p *puller) merge(ctx context.Context) (err error) {
	var (
		b      = p.backend
		merger = b.repo.Merger()

		oursDataC, theirsDataC, oursMetaC, theirsMetaC                 git.Commit
		revision, oursRevision, theirsRevision                         int64
		dataMutated, metaMutated, dataFastForward, metaFastForward     bool
		dataMergeTreeID, metaMergeTreeID, newDataHeadID, newMetaHeadID git.ObjectID
		message                                                        string
	)

	defer merger.Close()

	for _, s := range []struct {
		ptrC           *git.Commit
		headFn         func(context.Context) (git.Commit, error)
		ignoreNotFound bool
	}{
		{ptrC: &theirsDataC, headFn: p.getRemoteDataHead, ignoreNotFound: false},
		{ptrC: &theirsMetaC, headFn: p.getRemoteMetadataHead, ignoreNotFound: false},
		{ptrC: &oursDataC, headFn: b.getDataHead, ignoreNotFound: true},
		{ptrC: &oursMetaC, headFn: b.getMetadataHead, ignoreNotFound: true},
	} {
		if *s.ptrC, err = s.headFn(ctx); err != nil {
			if !b.errors.IsNotFound(err) {
				return
			} else if !s.ignoreNotFound {
				return
			}
		}

		defer (*s.ptrC).Close()
	}

	for _, s := range []struct {
		ptrRevision *int64
		metaP       git.Peelable
	}{
		{ptrRevision: &oursRevision, metaP: oursMetaC},
		{ptrRevision: &theirsRevision, metaP: theirsMetaC},
	} {
		if s.metaP == nil {
			continue
		}

		if *s.ptrRevision, err = b.readRevisionFromMetaPeelable(ctx, s.metaP); err != nil {
			return
		}
	}

	revision = maxInt64(oursRevision, theirsRevision)

	for _, s := range []struct {
		oursC, theirsC       git.Commit
		mutated, fastForward *bool
		mergeTreeID          *git.ObjectID
	}{
		{oursC: oursDataC, theirsC: theirsDataC, mutated: &dataMutated, mergeTreeID: &dataMergeTreeID, fastForward: &dataFastForward},
		{oursC: oursMetaC, theirsC: theirsMetaC, mutated: &metaMutated, mergeTreeID: &metaMergeTreeID, fastForward: &metaFastForward},
	} {
		if *s.mutated, *s.mergeTreeID, *s.fastForward, err = merger.MergeTreesFromCommits(ctx, s.oursC, s.theirsC); err != nil {
			return
		}
	}

	if !dataMutated && !metaMutated {
		return
	}

	if dataFastForward = dataFastForward && !p.noFastForward; dataFastForward {
		newDataHeadID = theirsDataC.ID()
	}

	if metaFastForward = metaFastForward && dataFastForward && !p.noFastForward; metaFastForward {
		newMetaHeadID = theirsMetaC.ID()
	} else {
		revision++ // Merge commit increments revision.
	}

	message = revisionToString(revision)

	if dataMutated && !dataFastForward {
		if newDataHeadID, err = p.createMergeCommit(ctx, message, dataMergeTreeID, oursDataC, theirsDataC); err != nil {
			return
		}
	}

	if !metaMutated {
		if oursMetaC != nil {
			metaMergeTreeID = oursMetaC.TreeID()
			metaMutated = true
		} else if theirsMetaC != nil {
			// Should not happen.
			metaMergeTreeID = theirsMetaC.TreeID()
			metaMutated = true
		}
	}

	if !metaFastForward {
		var (
			metaMergeT         git.Tree
			newMetaMutated     bool
			newMetaMergeTreeID git.ObjectID
			contents           = map[string][]byte{metadataPathRevision: []byte(revisionToString(revision))}
		)

		if metaMergeT, err = b.repo.ObjectGetter().GetTree(ctx, metaMergeTreeID); err != nil {
			return
		}

		defer metaMergeT.Close()

		if dataMutated {
			contents[metadataPathData] = []byte(newDataHeadID.String())
		}

		if newMetaMutated, newMetaMergeTreeID, err = p.mutateTree(ctx, metaMergeT, contents); err != nil {
			return
		} else if !newMetaMutated {
			newMetaMergeTreeID = metaMergeTreeID
		}

		if newMetaHeadID, err = p.createMergeCommit(ctx, message, newMetaMergeTreeID, oursMetaC, theirsMetaC); err != nil {
			return
		}
	}

	return b.advanceReferences(ctx, metaMutated, newMetaHeadID, dataMutated, newDataHeadID, revision)
}

func (p *puller) pull(ctx context.Context) (err error) {
	for _, fn := range []func(context.Context) error{p.fetch, p.merge} {
		if err = fn(ctx); err != nil {
			return
		}
	}

	return
}

func (p *puller) Run(ctx context.Context) (err error) {
	p.log.V(-1).Info("Running")
	defer func() { p.log.V(-1).Info("Stopping", "error", err) }()

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-p.ticker:
			if !ok {
				return
			}

			if err = p.pull(ctx); err != nil {
				p.log.Error(err, "Error pulling watch queue", "remoteName", p.remoteName)
			}
		}
	}
}
