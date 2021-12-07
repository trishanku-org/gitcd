package backend

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/trishanku/gitcd/pkg/git"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

type WatchOptionFunc func(etcdserverpb.WatchServer) error

func NewWatchServer(optFns ...WatchOptionFunc) (ws etcdserverpb.WatchServer, err error) {
	var wm = &watchManager{}

	for _, optFn := range optFns {
		if err = optFn(wm); err != nil {
			return
		}
	}

	ws = wm
	return
}

type wmOptionFunc func(*watchManager) error

func checkIsWatchManager(i interface{}) (*watchManager, error) {
	if b, ok := i.(*watchManager); ok {
		return b, nil
	}

	return nil, fmt.Errorf("unsupported watch backend type %T", i)
}

type watchOpts struct{}

var WatchOptions = watchOpts{}

func (watchOpts) doIt(fn wmOptionFunc) WatchOptionFunc {
	return func(ws etcdserverpb.WatchServer) (err error) {
		var wm *watchManager

		if wm, err = checkIsWatchManager(ws); err != nil {
			return
		}

		// Updating options will probably require restarting the watch server.
		wm.Lock()
		defer wm.Unlock()

		return fn(wm)
	}
}

func (opts watchOpts) WithBackend(kvs etcdserverpb.KVServer) WatchOptionFunc {
	return opts.doIt(func(wm *watchManager) (err error) {
		var b *backend

		if b, err = checkIsBackend(kvs); err != nil {
			return
		}

		wm.backend = b
		return nil
	})
}

func (opts watchOpts) WithTicker(ticker <-chan time.Time) WatchOptionFunc {
	return opts.doIt(func(wm *watchManager) (err error) {
		wm.ticker = ticker
		return nil
	})
}

func (opts watchOpts) WithLogger(log logr.Logger) WatchOptionFunc {
	return opts.doIt(func(wm *watchManager) (err error) {
		wm.log = log
		return nil
	})
}

func (opts watchOpts) WithContext(ctx context.Context) WatchOptionFunc {
	return opts.doIt(func(wm *watchManager) (err error) {
		go func() {
			if err := wm.Run(ctx); err != nil {
				wm.log.Error(err, "Error running. Terminating...")
			}
		}()
		return nil
	})
}

// watch defines the interface to interact with individual watches.
type watch interface {
	Context() context.Context
	Cancel(*etcdserverpb.ResponseHeader, error) error
	ProgressNotify() bool
	PrevKv() bool
	WatchId() int64
	Fragment() bool
	FilterAndSend(*etcdserverpb.ResponseHeader, []*mvccpb.Event) error
}

// revisionWatcher extracts and dispatches events for a given revision.
type revisionWatcher struct {
	backend     *backend
	revision    int64
	changesOnly bool
	interval    *closedOpenInterval
	watches     []watch

	// These are computed automatically based on the interval and changes.
	nrun           int
	startPathSlice pathSlice
	events         []*mvccpb.Event
}

func (rw *revisionWatcher) next() (n *revisionWatcher) {
	if rw == nil {
		return
	}

	n = &revisionWatcher{backend: rw.backend, revision: rw.revision + 1, changesOnly: true, interval: rw.interval}
	n.addWatches(rw.watches...)

	return
}

func (rw *revisionWatcher) addWatches(watches ...watch) *revisionWatcher {
	for _, w := range watches {
		if ctx := w.Context(); ctx.Err() == nil {
			rw.watches = append(rw.watches, w)
		}
	}

	return rw
}

func (rw *revisionWatcher) cancelAllWatches(ctx context.Context, err error) *revisionWatcher {
	if rw == nil {
		return rw
	}

	var header = rw.backend.newResponseHeader(ctx)
	if header == nil {
		header = rw.backend.newResponseHeaderWithRevision(0)
	}

	for _, w := range rw.watches {
		w.Cancel(header, err)
	}

	return rw
}

func (rw *revisionWatcher) getStartPathSlice() pathSlice {
	if rw == nil {
		return nil
	}

	if rw.startPathSlice == nil {
		rw.startPathSlice = pathSlice(splitPath(rw.backend.getPathForKey(rw.interval.GetStartInclusive().String())))
	}

	return rw.startPathSlice
}

func (rw *revisionWatcher) getRevisionAndPredecessorMetadata(ctx context.Context) (metaRoot, metaPredecessorRoot git.Tree, err error) {
	var (
		b                     = rw.backend
		metaRef               git.Reference
		metaHead              git.Commit
		metaP                 git.Peelable
		metaCommit            git.Commit
		metaPredecessorTreeID git.ObjectID
		predecessorRevision   int64
	)

	if metaRef, err = b.getMetadataReference(ctx); err != nil {
		return
	}

	defer metaRef.Close()

	if metaHead, err = b.repo.Peeler().PeelToCommit(ctx, metaRef); err != nil {
		return
	}

	defer metaHead.Close()

	if metaP, err = b.getMetadataPeelableForRevision(ctx, metaHead, rw.revision); err != nil {
		return
	}

	defer metaP.Close()

	if metaCommit, err = b.repo.Peeler().PeelToCommit(ctx, metaP); err != nil {
		return
	}

	defer metaCommit.Close()

	if metaRoot, err = b.repo.Peeler().PeelToTree(ctx, metaCommit); err != nil {
		return
	}

	if !rw.changesOnly {
		return
	}

	if err = metaCommit.ForEachParent(ctx, func(ctx context.Context, c git.Commit) (done bool, err error) {
		var (
			t        git.Tree
			revision int64
		)

		if t, err = b.repo.Peeler().PeelToTree(ctx, c); err != nil {
			return
		}

		defer t.Close()

		if revision, err = b.readRevision(ctx, t, metadataPathRevision); err != nil {
			return
		}

		if metaPredecessorRoot == nil || (revision < rw.revision && revision > predecessorRevision) {
			metaPredecessorTreeID = t.ID()
			predecessorRevision = revision
		}

		done = revision == rw.revision-1
		return
	}); err != nil {
		return
	}

	if predecessorRevision <= 0 {
		return
	}

	metaPredecessorRoot, err = b.repo.ObjectGetter().GetTree(ctx, metaPredecessorTreeID)
	return
}

func (rw *revisionWatcher) getDataRootForMetadata(ctx context.Context, metaRoot git.Tree) (dataRoot git.Tree, err error) {
	var (
		b = rw.backend
		c git.Commit
	)

	if c, err = b.getDataCommitForMetadata(ctx, metaRoot); err != nil {
		return
	}

	defer c.Close()

	dataRoot, err = b.repo.Peeler().PeelToTree(ctx, c)
	return
}

func (rw *revisionWatcher) Run(ctx context.Context) (err error) {
	var nmt, ndt, omt, odt git.Tree

	defer func() {
		if err != nil && !errors.Is(err, rpctypes.ErrGRPCFutureRev) {
			rw.cancelAllWatches(ctx, err)
		}
	}()

	rw.nrun++

	if nmt, omt, err = rw.getRevisionAndPredecessorMetadata(ctx); err != nil {
		return
	}

	rw.events = nil

	for _, s := range []struct {
		m    git.Tree
		dPtr *git.Tree
	}{
		{m: nmt, dPtr: &ndt},
		{m: omt, dPtr: &odt},
	} {
		if s.m == nil {
			continue
		}

		defer s.m.Close()

		if *s.dPtr, err = rw.getDataRootForMetadata(ctx, s.m); err != nil {
			return
		}

		defer (*s.dPtr).Close()
	}

	if err = rw.loadEvents(ctx, "", nmt, ndt, omt, odt); err != nil {
		return
	}

	err = rw.dispatchEvents(ctx)
	return
}

func (rw *revisionWatcher) checkMetadataEntryRelevance(parentPath string, mte git.TreeEntry) (relevant, done bool) {
	var (
		b               = rw.backend
		ie              = &intervalExplorer{keyPrefix: b.keyPrefix, repo: b.repo, errors: b.errors, tree: nil, interval: rw.interval}
		p               = parentPath
		k               = key(b.getKeyForPath(p))
		startKeyForPath = key(ie.getKeyForPath(rw.getStartPathSlice().getRelevantPathForDepthOf(p)))
	)

	if k.Cmp(startKeyForPath) == cmpResultLess {
		return
	}

	if done = rw.interval.Check(k) == checkResultOutOfRangeRight; done {
		relevant = false
		return
	}

	if mte == nil {
		return
	}

	if mte.EntryType() != git.ObjectTypeTree { // Non-tree entries are only metadata and are not keys.
		return
	}

	p = path.Join(parentPath, mte.EntryName())
	k = key(b.getKeyForPath(p))
	startKeyForPath = key(ie.getKeyForPath(rw.getStartPathSlice().getRelevantPathForDepthOf(p)))

	if k.Cmp(startKeyForPath) == cmpResultLess {
		return
	}

	done = rw.interval.Check(k) == checkResultOutOfRangeRight
	relevant = !done
	return
}

func (rw *revisionWatcher) forEachRelevantEntryInMetadata(ctx context.Context, parentPath string, mt git.Tree, fn git.TreeEntryReceiverFunc) (err error) {
	var relevant bool

	if relevant, _ = rw.checkMetadataEntryRelevance(parentPath, nil); !relevant {
		return
	}

	err = mt.ForEachEntry(ctx, func(ctx context.Context, mte git.TreeEntry) (done bool, err error) {
		if relevant, done = rw.checkMetadataEntryRelevance(parentPath, mte); !relevant || done {
			return
		}

		done, err = fn(ctx, mte)
		return
	})

	return
}

func (rw *revisionWatcher) loaderOfEventsForRelevantMetadataEntry(
	parentPath string,
	ndt, odt git.Tree,
	metaTreeEntriesFn func(ctx context.Context, mte git.TreeEntry) (nmte, omte git.TreeEntry, err error),
) git.TreeEntryReceiverFunc {
	var b = rw.backend

	return func(ctx context.Context, mte git.TreeEntry) (done bool, err error) {
		var (
			entryName              = mte.EntryName()
			nmte, omte, ndte, odte git.TreeEntry
			ndteType, odteType     git.ObjectType
			ignorableErr           error
			nmt, omt               git.Tree
			p                      string
		)

		if nmte, omte, ignorableErr = metaTreeEntriesFn(ctx, mte); ignorableErr != nil {
			return
		}

		for _, s := range []struct {
			check     bool
			fn        func(context.Context, string) (git.TreeEntry, error)
			tePtr     *git.TreeEntry
			teTypePtr *git.ObjectType
		}{
			{check: ndt != nil, tePtr: &ndte, fn: ndt.GetEntryByPath},
			{check: odt != nil, tePtr: &odte, fn: odt.GetEntryByPath},
		} {
			if s.check {
				var ignorableErr error
				if *s.tePtr, ignorableErr = s.fn(ctx, entryName); ignorableErr != nil {
					continue
				}

				if s.teTypePtr != nil {
					*s.teTypePtr = (*s.tePtr).EntryType()
				}
			}
		}

		for _, s := range []struct {
			mte   git.TreeEntry
			mtPtr *git.Tree
		}{
			{mte: nmte, mtPtr: &nmt},
			{mte: omte, mtPtr: &omt},
		} {
			if s.mte != nil && s.mte.EntryType() == git.ObjectTypeTree {
				if *s.mtPtr, err = b.repo.ObjectGetter().GetTree(ctx, s.mte.EntryID()); err != nil {
					return
				}

				defer (*s.mtPtr).Close()
			}
		}

		p = path.Join(parentPath, entryName)

		// load blob key events
		for _, s := range []struct {
			check     bool
			eventType mvccpb.Event_EventType
			dte, odte git.TreeEntry
			mt, omt   git.Tree
			quit      bool
		}{
			{
				check:     (ndte == nil || ndteType == git.ObjectTypeTree) && odte != nil && odteType == git.ObjectTypeBlob,
				eventType: mvccpb.DELETE,
				dte:       odte,
				mt:        omt,
				quit:      ndte == nil,
			},
			{
				check:     (odte == nil || odteType == git.ObjectTypeTree) && ndte != nil && ndteType == git.ObjectTypeBlob,
				eventType: mvccpb.PUT,
				dte:       ndte,
				mt:        nmt,
				quit:      odte == nil,
			},
			{
				check:     ndte != nil && ndteType == git.ObjectTypeBlob && odte != nil && odteType == git.ObjectTypeBlob,
				eventType: mvccpb.PUT,
				dte:       ndte,
				odte:      odte,
				mt:        nmt,
				omt:       omt,
				quit:      true,
			},
		} {
			if !s.check {
				continue
			}

			var (
				k     = key(b.getKeyForPath(p))
				ev    *mvccpb.Event
				v, ov []byte
			)

			switch rw.interval.Check(k) {
			case checkResultOutOfRangeRight:
				done = true
				fallthrough
			case checkResultOutOfRangeLeft:
				return
			}

			if s.mt == nil {
				return // Skip if there is no metadata.
			}

			ev = &mvccpb.Event{Type: s.eventType}

			for _, s := range []struct {
				dte  git.TreeEntry
				vPtr *[]byte
			}{
				{dte: s.dte, vPtr: &v},
				{dte: s.odte, vPtr: &ov},
			} {
				if s.dte != nil {
					if *s.vPtr, err = b.getContentForTreeEntry(ctx, s.dte); err != nil {
						return
					}
				}
			}

			for _, s := range []struct {
				mt     git.Tree
				v      []byte
				kvPPtr **mvccpb.KeyValue
			}{
				{mt: s.mt, v: v, kvPPtr: &ev.Kv},
				{mt: s.omt, v: ov, kvPPtr: &ev.PrevKv},
			} {
				if s.mt != nil {
					if *s.kvPPtr, err = b.loadKeyValue(ctx, s.mt, k, s.v); err != nil {
						return
					}
				}
			}

			rw.events = append(rw.events, ev)

			if s.quit {
				return
			}
		}

		for _, s := range []struct {
			check      bool
			ndte, odte git.TreeEntry
			nmt, omt   git.Tree
		}{
			{ // DELETE
				check: (ndte == nil || ndteType == git.ObjectTypeBlob) && odte != nil && odteType == git.ObjectTypeTree,
				odte:  odte,
				omt:   omt,
			},
			{ // PUT
				check: (odte == nil || odteType == git.ObjectTypeBlob) && ndte != nil && ndteType == git.ObjectTypeTree,
				ndte:  ndte,
				nmt:   nmt,
			},
			{ // PUT and/or DELETE
				check: ndte != nil && ndteType == git.ObjectTypeTree && odte != nil && odteType == git.ObjectTypeTree,
				ndte:  ndte,
				odte:  odte,
				nmt:   nmt,
				omt:   omt,
			},
		} {
			if !s.check {
				continue
			}

			var ndt, odt git.Tree

			for _, s := range []struct {
				dte   git.TreeEntry
				dtPtr *git.Tree
			}{
				{dte: s.ndte, dtPtr: &ndt},
				{dte: s.odte, dtPtr: &odt},
			} {
				if s.dte != nil {
					if *s.dtPtr, err = b.repo.ObjectGetter().GetTree(ctx, s.dte.EntryID()); err != nil {
						return
					}

					defer (*s.dtPtr).Close()
				}
			}

			err = rw.loadEvents(ctx, p, s.nmt, ndt, s.omt, odt)
			return
		}

		return
	}
}

func mergeSortedUniqueEntryNames(a, b []string) (c []string) {
	var ai, bi int

	if len(a) <= 0 {
		return b
	}

	if len(b) <= 0 {
		return a
	}

	for ai, bi = 0, 0; ai < len(a) && bi < len(b); {
		var aName, bName = a[ai], b[bi]

		switch key(aName).Cmp(key(bName)) {
		case cmpResultLess:
			c = append(c, aName)
			ai++
		case cmpResultEqual:
			c = append(c, aName)
			ai++
			bi++
		default:
			c = append(c, bName)
			bi++
		}
	}

	for _, s := range []struct {
		i int
		s []string
	}{
		{i: ai, s: a},
		{i: bi, s: b},
	} {
		if s.i >= len(s.s) {
			continue
		}

		if c[len(c)-1] == s.s[s.i] {
			s.i++ // Skip an entry in s.s if the same is already present in c.
		}

		c = append(c, s.s[s.i:]...)
	}

	return
}

func (rw *revisionWatcher) loadEvents(
	ctx context.Context,
	parentPath string,
	nmt, ndt, omt, odt git.Tree,
) (err error) {
	var (
		nNames, oNames, allNames []string
		nEntries, oEntries       map[string]git.TreeEntry
	)

	if nmt == nil && omt == nil {
		return
	}

	for _, s := range []struct {
		check             bool
		mt                git.Tree
		metaTreeEntriesFn func(ctx context.Context, mte git.TreeEntry) (nmte, omte git.TreeEntry, err error)
	}{
		{ // DELETE
			check:             nmt == nil && omt != nil,
			mt:                omt,
			metaTreeEntriesFn: func(ctx context.Context, mte git.TreeEntry) (nmte, omte git.TreeEntry, err error) { omte = mte; return },
		},
		{ // PUT
			check:             omt == nil && nmt != nil,
			mt:                nmt,
			metaTreeEntriesFn: func(ctx context.Context, mte git.TreeEntry) (nmte, omte git.TreeEntry, err error) { nmte = mte; return },
		},
	} {
		if !s.check {
			continue
		}

		err = rw.forEachRelevantEntryInMetadata(
			ctx,
			parentPath,
			s.mt,
			rw.loaderOfEventsForRelevantMetadataEntry(
				parentPath,
				ndt, odt,
				s.metaTreeEntriesFn,
			),
		)
		return
	}

	nEntries, oEntries = make(map[string]git.TreeEntry), make(map[string]git.TreeEntry)

	for _, s := range []struct {
		m        git.Tree
		pnames   *[]string
		pentries *map[string]git.TreeEntry
	}{
		{m: nmt, pnames: &nNames, pentries: &nEntries},
		{m: omt, pnames: &oNames, pentries: &oEntries},
	} {
		if err = rw.forEachRelevantEntryInMetadata(ctx, parentPath, s.m, func(ctx context.Context, mte git.TreeEntry) (done bool, err error) {
			var entryName = mte.EntryName()

			*s.pnames = append(*s.pnames, entryName)
			(*s.pentries)[entryName] = mte
			return
		}); err != nil {
			return
		}
	}

	allNames = mergeSortedUniqueEntryNames(nNames, oNames)

	for _, entryName := range allNames {
		var nmte, omte = nEntries[entryName], oEntries[entryName]

		if nmte == nil && omte == nil {
			continue
		}

		for _, s := range []struct {
			check bool
			mte   git.TreeEntry
		}{
			{check: nmte == nil && omte != nil, mte: omte}, // DELETE
			{check: omte == nil && nmte != nil, mte: nmte}, // PUT
			{ // PUT and/or DELETE
				check: nmte != nil && omte != nil && !reflect.DeepEqual(nmte.EntryID(), omte.EntryID()),
				mte:   nmte,
			},
		} {
			if s.check {
				_, err = rw.loaderOfEventsForRelevantMetadataEntry(
					parentPath,
					ndt, odt,
					func(ctx context.Context, mte git.TreeEntry) (nmter, omter git.TreeEntry, err error) {
						nmter, omter = nmte, omte
						return
					},
				)(ctx, s.mte)
				return
			}
		}
	}

	return
}

// TODO optimize for performance
func (rw *revisionWatcher) dispatchEvents(ctx context.Context) (err error) {
	if len(rw.events) == 0 || len(rw.watches) == 0 {
		return
	}

	var (
		eventsNoPrevKv []*mvccpb.Event
		header         = rw.backend.newResponseHeaderWithRevision(rw.revision)
	)

	for _, w := range rw.watches {
		if err = ctx.Err(); err != nil {
			return
		}

		if w.Context().Err() != nil {
			continue
		}

		var (
			events = rw.events
			werr   error
		)

		if !w.PrevKv() {
			if len(eventsNoPrevKv) != len(rw.events) {
				for _, ev := range rw.events {
					eventsNoPrevKv = append(eventsNoPrevKv, &mvccpb.Event{Type: ev.Type, Kv: ev.Kv})
				}
			}

			events = eventsNoPrevKv
		}

		if werr = w.FilterAndSend(header, events); werr != nil {
			w.Cancel(header, err)
		}
	}

	return
}

type watchManager struct {
	sync.Mutex

	backend *backend
	ticker  <-chan time.Time
	log     logr.Logger

	ctx      context.Context
	cancelFn context.CancelFunc
	queue    []*revisionWatcher
}

var _ etcdserverpb.WatchServer = (*watchManager)(nil)

func (wm *watchManager) enqueue(rw ...*revisionWatcher) {
	wm.Lock()
	defer wm.Unlock()

	wm.queue = append(wm.queue, rw...)
}

func (wm *watchManager) groupQueue(q []*revisionWatcher) (groupedQ []*revisionWatcher) {
	if q == nil {
		return
	}

	var m = map[int64]map[bool]*revisionWatcher{}

	for _, rw := range q {
		var mr map[bool]*revisionWatcher

		if rw == nil {
			continue
		}

		if mr = m[rw.revision]; mr == nil {
			mr = map[bool]*revisionWatcher{}
			m[rw.revision] = mr
		}

		if mrw := mr[rw.changesOnly]; mrw == nil {
			mr[rw.changesOnly] = rw
		} else {
			mrw.interval.merge(rw.interval)
			mrw.addWatches(rw.watches...)
		}
	}

	for _, mr := range m {
		for _, rw := range mr {
			groupedQ = append(groupedQ, rw)
		}
	}

	return
}

func (wm *watchManager) dispatchQueue(parentCtx context.Context, q []*revisionWatcher) (nextQ []*revisionWatcher, err error) {
	type message struct {
		next *revisionWatcher
		err  error
	}

	var (
		ctx, cancelFn = context.WithCancel(parentCtx)
		ch            = make(chan *message)
		errs          []error
	)

	defer close(ch)
	defer cancelFn()

	q = wm.groupQueue(q)

	for _, rw := range q {
		go func(rw *revisionWatcher) {
			var (
				next *revisionWatcher
				err  error
			)

			defer func() { ch <- &message{next: next, err: err} }()

			if err := rw.Run(ctx); err == nil {
				next = rw.next()
			} else if errors.Is(err, rpctypes.ErrGRPCFutureRev) {
				next = rw // Retry later
			}
		}(rw)
	}

	for i := 0; i < len(q); i++ {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			if msg.next != nil {
				nextQ = append(nextQ, msg.next)
			}

			errs = append(errs, msg.err)
		}
	}

	err = utilerrors.NewAggregate(errs)
	return
}

func (wm *watchManager) Run(ctx context.Context) error {
	func() {
		wm.Lock()
		defer wm.Unlock()

		if wm.cancelFn != nil {
			wm.cancelFn()
		}

		wm.ctx, wm.cancelFn = context.WithCancel(ctx)
		ctx = wm.ctx
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-wm.ticker:
			if !ok {
				return nil
			}

			var q []*revisionWatcher

			func() {
				wm.Lock()
				defer wm.Unlock()

				q = wm.queue
				wm.queue = nil
			}()

			if q == nil {
				continue
			}

			q, _ = wm.dispatchQueue(ctx, q)
			// TODO Log err
			if len(q) > 0 {
				wm.enqueue(q...)
			}
		}
	}
}

func (wm *watchManager) Watch(stream etcdserverpb.Watch_WatchServer) (err error) {
	var (
		ctx, cancelFn = context.WithCancel(stream.Context())
		w             = &watchServer{
			stream: stream,
			mgr:    wm,
		}
	)

	defer cancelFn()

	for err != nil {
		var req *etcdserverpb.WatchRequest

		if req, err = stream.Recv(); err != nil {
			break
		}

		switch {
		case req.GetCreateRequest() != nil:
			err = w.accept(ctx, req.GetCreateRequest())
		case req.GetCancelRequest() != nil:
			err = w.cancel(ctx, req.GetCancelRequest().GetWatchId(), nil)
		default:
			// TODO Handle WatchProgressRequest
		}
	}

	return
}

type watchServer struct {
	sync.Mutex

	stream etcdserverpb.Watch_WatchServer
	mgr    *watchManager

	watches map[int64]watch
	watchID int64
}

// register registers the cancelFn with a new watch ID which can be passed to unregister later.
// The new watch ID is returned.
func (ws *watchServer) registerWatch(ctx context.Context, req *etcdserverpb.WatchCreateRequest) watch {
	ws.Lock()
	defer ws.Unlock()

	req.WatchId = atomic.AddInt64(&ws.watchID, 1)

	var wi = &watchImpl{
		watchServer: ws,
		req:         req,
		stream:      ws.stream,
	}

	wi.ctx, wi.cancelFn = context.WithCancel(ctx)

	ws.watches[req.WatchId] = wi

	return wi
}

func (ws *watchServer) unregisterWatch(w watch) {
	ws.Lock()
	defer ws.Unlock()

	delete(ws.watches, w.WatchId())
}

func isWatchNOP(req *etcdserverpb.WatchCreateRequest) bool {
	var noPut, noDelete bool

	for _, filter := range req.GetFilters() {
		switch filter {
		case etcdserverpb.WatchCreateRequest_NOPUT:
			noPut = true
		case etcdserverpb.WatchCreateRequest_NODELETE:
			noDelete = true
		}
	}

	return noPut && noDelete
}

func (ws *watchServer) checkRevisionValid(ctx context.Context, revision int64) (err error) {
	switch {
	case revision < 0:
		err = fmt.Errorf("unsupported revision %d", revision)
		return
	case revision == 0:
		return
	}

	var (
		b        = ws.mgr.backend
		metaRef  git.Reference
		metaHead git.Commit
		metaP    git.Peelable
	)

	if metaRef, err = b.getMetadataReference(ctx); err != nil {
		return
	}

	defer metaRef.Close()

	if metaHead, err = b.repo.Peeler().PeelToCommit(ctx, metaRef); err != nil {
		return
	}

	defer metaHead.Close()

	metaP, err = ws.mgr.backend.getMetadataPeelableForRevision(ctx, metaHead, revision)

	metaP.Close()

	return
}

func (ws *watchServer) accept(ctx context.Context, req *etcdserverpb.WatchCreateRequest) (err error) {
	var (
		w      = ws.registerWatch(ctx, req)
		b      = ws.mgr.backend
		header *etcdserverpb.ResponseHeader
	)

	defer func() {
		if err != nil {
			// TODO log cancel error
			if header == nil {
				header = b.newResponseHeader(ctx)
			}
			w.Cancel(header, err)
		}
	}()

	if isWatchNOP(req) {
		err = fmt.Errorf("invalid watch request with filters %v", req.Filters)
		return
	}

	if err = ws.checkRevisionValid(ctx, req.StartRevision); err != nil {
		return
	}

	if req.StartRevision == 0 {
		header = b.newResponseHeader(ctx)
	} else {
		header = b.newResponseHeaderWithRevision(req.StartRevision)
	}

	if err = ws.stream.Send(&etcdserverpb.WatchResponse{
		Header:  header,
		Created: true,
		WatchId: w.WatchId(), // TODO CompactVersion
	}); err != nil {
		return
	}

	ws.mgr.enqueue(&revisionWatcher{
		backend:     b,
		revision:    header.Revision,
		changesOnly: true,
		interval:    &closedOpenInterval{start: req.GetKey(), end: req.GetRangeEnd()},
		watches:     []watch{w},
	})

	return
}

func (ws *watchServer) cancel(ctx context.Context, watchID int64, cause error) (err error) {
	var w watch

	func() {
		ws.Mutex.Lock()
		defer ws.Mutex.Unlock()

		w = ws.watches[watchID]
	}()

	if w == nil {
		return
	}

	err = w.Cancel(ws.mgr.backend.newResponseHeader(ctx), cause)
	return
}

type watchImpl struct {
	watchServer *watchServer
	ctx         context.Context
	cancelFn    context.CancelFunc
	req         *etcdserverpb.WatchCreateRequest
	stream      etcdserverpb.Watch_WatchServer
}

var _ watch = (*watchImpl)(nil)

func (w *watchImpl) Context() context.Context {
	return w.ctx
}

func (w *watchImpl) Cancel(header *etcdserverpb.ResponseHeader, err error) error {
	var reason = "watch closed"

	defer w.watchServer.unregisterWatch(w)

	w.cancelFn()

	if err != nil {
		reason = err.Error()
	}

	return w.stream.Send(&etcdserverpb.WatchResponse{Header: header, WatchId: w.req.WatchId, Canceled: true, CancelReason: reason})
}

func (w *watchImpl) PrevKv() bool {
	return w.req.PrevKv
}

func (w *watchImpl) ProgressNotify() bool {
	return w.req.ProgressNotify
}

func (w *watchImpl) WatchId() int64 {
	return w.req.WatchId
}

func (w *watchImpl) Fragment() bool {
	return w.req.Fragment
}

// TODO optimize for performance
func (w *watchImpl) FilterAndSend(header *etcdserverpb.ResponseHeader, allEvents []*mvccpb.Event) (err error) {
	var (
		ctx      = w.Context()
		events   []*mvccpb.Event
		interval = &closedOpenInterval{start: w.req.Key, end: w.req.RangeEnd}
	)

	if err = ctx.Err(); err != nil {
		return
	}

filter:
	for _, ev := range allEvents {
		switch interval.Check(ev.Kv.Key) {
		case checkResultOutOfRangeLeft:
			continue filter
		case checkResultOutOfRangeRight:
			break filter
		default:
			events = append(events, ev)
		}
	}

	if !w.ProgressNotify() && len(events) == 0 {
		return
	}

	// TODO fragment
	err = w.stream.Send(&etcdserverpb.WatchResponse{Header: header, WatchId: w.req.WatchId, Events: events})
	return
}
