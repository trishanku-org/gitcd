package backend

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/onsi/ginkgo"
	"github.com/trishanku/gitcd/pkg/git"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

//TODO Test
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

func (opts watchOpts) WithCancelChannelSize(cancelChannelSize int) WatchOptionFunc {
	return opts.doIt(func(wm *watchManager) (err error) {
		wm.cancelChannelSize = cancelChannelSize
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
	Cancel(*etcdserverpb.ResponseHeader, error)
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
	log         logr.Logger
	watches     []watch

	// These are computed automatically based on the interval and changes.
	nrun   int
	events []*mvccpb.Event
}

func (rw *revisionWatcher) next() (n *revisionWatcher) {
	if rw == nil {
		return
	}

	n = &revisionWatcher{backend: rw.backend, revision: rw.revision + 1, changesOnly: true, interval: rw.interval, log: rw.log}
	n.watches = getActiveWatches(rw.watches)

	return
}

func getActiveWatches(watches []watch) (r []watch) {
	if len(watches) <= 0 {
		return
	}

	for _, w := range watches {
		if ctx := w.Context(); ctx.Err() == nil {
			r = append(r, w)
		}
	}

	return
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

func (rw *revisionWatcher) getRevisionAndPredecessorMetadata(ctx context.Context) (metaRoot, metaPredecessorRoot git.Tree, err error) {
	var (
		b                     = rw.backend
		metaRef               git.Reference
		metaHead              git.Commit
		metaP                 git.Peelable
		metaCommit            git.Commit
		metaPredecessorTreeID git.ObjectID
		predecessorRevision   int64
		commitFilterFn        commitFilterFunc
		log                   = rw.log.WithName("getRevisionAndPredecessorMetadata")

		processParentCommitFn = func(ctx context.Context, c git.Commit) (done bool, err error) {
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
		}
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

	// First try only own lineage. TODO Test
	commitFilterFn = sameAuthorAs(metaHead)

	if err = metaCommit.ForEachParent(ctx, func(ctx context.Context, c git.Commit) (done bool, err error) {
		if commitFilterFn != nil {
			var skip bool

			if skip, err = commitFilterFn(ctx, c); skip || err != nil {
				// Skip other lineages.
				return
			}
		}

		// Own lineage.
		done, err = processParentCommitFn(ctx, c)
		return
	}); err != nil {
		return
	}

	if predecessorRevision <= 0 && commitFilterFn != nil {
		// No suitable predecessor in own lineage. Try, all lineages.
		log.V(-1).Info(
			"No suitable predecessor in own lineage. Trying all lineages.",
			"revision", rw.revision,
			"metaCommitID", metaCommit.ID(),
		)
		if err = metaCommit.ForEachParent(ctx, processParentCommitFn); err != nil {
			return
		}
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
	var (
		nmt, ndt, omt, odt git.Tree
		eventsLoaded       bool
	)

	defer func() {
		// TODO test
		if ctx.Err() != nil || (!eventsLoaded && err != nil && !errors.Is(err, rpctypes.ErrGRPCFutureRev)) {
			// Watches are canceled properly in dispatchEvents.
			rw.cancelAllWatches(ctx, err)
		}
	}()

	rw.backend.RLock()
	defer rw.backend.RUnlock()

	rw.watches = getActiveWatches(rw.watches)

	if len(rw.watches) == 0 {
		return // No active watches. Nothing to do.
	}

	rw.nrun++

	if nmt, omt, err = rw.getRevisionAndPredecessorMetadata(ctx); err != nil {
		return
	}

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

	rw.events = nil

	if err = rw.loadEvents(ctx, nmt, ndt, omt, odt); err != nil {
		return
	}

	eventsLoaded = true

	err = rw.dispatchEvents(ctx)
	return
}

type kvLoaderFunc func() (kv *mvccpb.KeyValue, err error)

func (b *backend) buildEvent(typ mvccpb.Event_EventType, kvFn, prevKVFn kvLoaderFunc, ignorePrevKVError bool) (ev *mvccpb.Event, err error) {
	var kv, prevKV *mvccpb.KeyValue

	if kvFn != nil {
		if kv, err = kvFn(); err != nil {
			return
		}
	}

	if prevKVFn != nil {
		prevKV, err = prevKVFn()

		if ignorePrevKVError {
			err = nil // Forget the PrevKV error.
		}

		if err != nil {
			return
		}
	}

	ev = &mvccpb.Event{Type: typ, Kv: kv, PrevKv: prevKV}
	return
}

func (b *backend) getKeyValueLoader(ctx context.Context, k string, metaRoot, dataRoot git.Tree) kvLoaderFunc {
	return func() (kv *mvccpb.KeyValue, err error) {
		var (
			p = b.getPathForKey(k)
			v []byte
		)

		if v, err = b.getContent(ctx, dataRoot, p); err != nil {
			return
		}

		if kv, err = b.getMetadataFor(ctx, metaRoot, k); err != nil {
			return
		}

		kv.Value = v
		return
	}
}

func (rw *revisionWatcher) getDeletionKeyValueLoader(prevKvFn kvLoaderFunc) kvLoaderFunc {
	return func() (kv *mvccpb.KeyValue, err error) {
		kv, err = prevKvFn()

		if kv == nil {
			return
		}

		kv.ModRevision = rw.revision
		kv.Value = nil
		kv.Lease = 0
		kv.Version = 0
		return
	}
}

func (rw *revisionWatcher) loadEvents(
	ctx context.Context,
	nmt, ndt, omt, odt git.Tree,
) (err error) {
	var (
		b      = rw.backend
		diff   git.Diff
		events []*mvccpb.Event
		log    logr.Logger
	)

	if nmt == nil && omt == nil {
		return
	}

	func() {
		var omtID, nmtID git.ObjectID

		if omt != nil {
			omtID = omt.ID()
		}

		if nmt != nil {
			nmtID = nmt.ID()
		}

		log = rw.log.WithValues(
			"func", "loadEvents",
			"changesOnly", rw.changesOnly,
			"revision", rw.revision,
			"interval", rw.interval,
			"omtID", omtID,
			"nmtID", nmtID,
		)
	}()

	defer func() {
		for _, ev := range rw.events {
			if ev != nil && ev.Kv != nil && ev.Type == mvccpb.PUT && ev.Kv.CreateRevision != ev.Kv.ModRevision && ev.PrevKv == nil {
				log.Info("PrevKV=nil", "ev", ev)
			}
		}
	}()

	if rw.changesOnly {
		diff, err = b.repo.TreeDiff(ctx, omt, nmt)
	} else {
		diff, err = b.repo.TreeDiff(ctx, nil, nmt)
	}

	if err != nil {
		return
	}

	defer diff.Close()

	if err = diff.ForEachDiffChange(ctx, func(ctx context.Context, change git.DiffChange) (done bool, err error) {
		var (
			p  = change.Path()
			k  string
			ev *mvccpb.Event
		)

		if path.Base(p) != etcdserverpb.Compare_MOD.String() {
			return // ModRevision always changes for all events. So, we can ignore other changes in metadata.
		}

		p = path.Dir(p) // Discard the trailing ModRevision key.
		k = b.getKeyForPath(p)

		switch change.Type() {
		case git.DiffChangeTypeAdded:
			if omt == nil {
				// No predecessor revision, no prevKV.
				ev, err = b.buildEvent(mvccpb.PUT, b.getKeyValueLoader(ctx, k, nmt, ndt), nil, false)
			} else {
				// Predecessor revision exists. PrevKV might exist if changeOnly == false.
				ev, err = b.buildEvent(mvccpb.PUT, b.getKeyValueLoader(ctx, k, nmt, ndt), b.getKeyValueLoader(ctx, k, omt, odt), true)
			}
		case git.DiffChangeTypeDeleted:
			{
				var prevKvFn = b.getKeyValueLoader(ctx, k, omt, odt)
				ev, err = b.buildEvent(mvccpb.DELETE, rw.getDeletionKeyValueLoader(prevKvFn), prevKvFn, false)
			}
		default:
			ev, err = b.buildEvent(mvccpb.PUT, b.getKeyValueLoader(ctx, k, nmt, ndt), b.getKeyValueLoader(ctx, k, omt, odt), false)
		}

		if err != nil {
			return
		}

		if ev != nil {
			events = append(events, ev)
		}

		return
	}); err != nil {
		return
	}

	if len(events) > 0 {
		// Sort deletes first to facilitate watchers to re-apply replace (delete, add) changes in the watch event order.
		sort.SliceStable(events, func(i, j int) bool { return events[i].Type > events[j].Type })

		rw.events = append(rw.events, events...)
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
		errs           []error
	)

	defer func() {
		if errs != nil {
			err = utilerrors.NewAggregate(errs)
		}
	}()

	for _, w := range rw.watches {
		if err = ctx.Err(); err != nil {
			errs = append(errs, err)
			return
		}

		if err = w.Context().Err(); err != nil {
			errs = append(errs, err)
			continue
		}

		var events = rw.events

		if !w.PrevKv() {
			if len(eventsNoPrevKv) != len(rw.events) {
				for _, ev := range rw.events {
					eventsNoPrevKv = append(eventsNoPrevKv, &mvccpb.Event{Type: ev.Type, Kv: ev.Kv})
				}
			}

			events = eventsNoPrevKv
		}

		if err = w.FilterAndSend(header, events); err != nil {
			errs = append(errs, err)
			w.Cancel(header, err) // TODO log cancel error.
		}
	}

	return
}

type watchManager struct {
	sync.Mutex

	backend           *backend
	ticker            <-chan time.Time
	log               logr.Logger
	cancelChannelSize int

	ctx      context.Context
	cancelFn context.CancelFunc
	queue    []*revisionWatcher
}

var _ etcdserverpb.WatchServer = (*watchManager)(nil)

func (wm *watchManager) enqueue(rw ...*revisionWatcher) {
	wm.Lock()
	defer wm.Unlock()

	wm.log.V(-1).Info("Enqueuing", "queue", len(rw))
	defer func() { wm.log.V(-1).Info("Enqueued", "queue", len(wm.queue)) }()

	wm.queue = append(wm.queue, rw...)
}

func (wm *watchManager) groupQueue(q []*revisionWatcher) (groupedQ []*revisionWatcher) {
	wm.log.V(-1).Info("Grouping queue", "queue", len(q))
	defer func() { wm.log.V(-1).Info("Grouped queue", "queue", len(groupedQ)) }()

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
			mrw.watches = append(mrw.watches, getActiveWatches(rw.watches)...)
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

	if err = parentCtx.Err(); err != nil {
		return
	}

	var (
		ctx, cancelFn = context.WithCancel(parentCtx)
		ch            = make(chan *message, len(q))
		errs          []error
	)

	defer close(ch)
	defer cancelFn()

	wm.log.V(-1).Info("dispatching queue", "queue", len(q))
	defer func() { wm.log.V(-1).Info("dispatched queue", "next queue", len(nextQ), "error", err) }()

	q = wm.groupQueue(q)

	for _, rw := range q {
		go func(rw *revisionWatcher) {
			var (
				log  = wm.log.WithValues("revision", rw.revision, "interval", rw.interval, "changesOnly", rw.changesOnly, "watches", len(rw.watches))
				next *revisionWatcher
				err  error
			)

			log.V(-1).Info("Running revision watcher")
			defer func() {
				log.V(-1).Info("Ran revision watcher", "error", err)
				if ctx.Err() == nil {
					ch <- &message{next: next, err: err}
				}
			}()

			defer ginkgo.GinkgoRecover()

			if err = rw.Run(ctx); err == nil {
				next = rw.next()
			} else if errors.Is(err, rpctypes.ErrGRPCFutureRev) {
				next = rw // Retry later
			}

			// Watches should already be canceled in rw.Run.
		}(rw)
	}

	for i := 0; i < len(q); i++ {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			if msg.next != nil && len(msg.next.watches) > 0 {
				nextQ = append(nextQ, msg.next)
			}

			errs = append(errs, msg.err)
		}
	}

	err = utilerrors.NewAggregate(errs)
	return
}

func while(running *bool, conditionFn func() bool, doFn func()) {
	*running = true
	defer func() { *running = false }()

	for conditionFn() {
		doFn()
	}
}

func (wm *watchManager) Run(ctx context.Context) (err error) {
	func() {
		wm.Lock()
		defer wm.Unlock()

		if wm.cancelFn != nil {
			wm.cancelFn()
		}

		wm.ctx, wm.cancelFn = context.WithCancel(ctx)
		ctx = wm.ctx
	}()

	defer func() {
		wm.Lock()
		defer wm.Unlock()

		if wm.cancelFn != nil {
			wm.cancelFn()
			wm.ctx, wm.cancelFn = nil, nil
		}
	}()

	wm.log.V(-1).Info("Running")
	defer func() { wm.log.V(-1).Info("Stopping", "error", err) }()

	for {
		var (
			tick, tock int64
			running    bool
		)

		select {
		case <-ctx.Done():
			return
		case t, ok := <-wm.ticker:
			if !ok {
				return
			}

			atomic.StoreInt64(&tick, t.UnixMicro())

			if running {
				continue
			}

			go while(
				&running,
				func() bool { return atomic.LoadInt64(&tick) > atomic.LoadInt64(&tock) },
				func() {
					var q []*revisionWatcher

					atomic.StoreInt64(&tock, atomic.LoadInt64(&tick))

					func() {
						wm.Lock()
						defer wm.Unlock()

						q = wm.queue
						wm.queue = nil
					}()

					if len(q) <= 0 {
						return
					}

					if q, err = wm.dispatchQueue(ctx, q); err != nil {
						wm.log.Error(err, "Error dispatching watch queue")
					}

					if len(q) > 0 {
						wm.enqueue(q...)

						if err == nil {
							if err = wm.backend.tickWatchDispatchTicker(ctx); err != nil {
								wm.log.Error(err, "Error ticking watch dispatcher ticker")
							}
						}
					}
				},
			)
		}
	}
}

func (wm *watchManager) Watch(stream etcdserverpb.Watch_WatchServer) (err error) {
	var (
		ctx, cancelFn = context.WithCancel(stream.Context())
		w             = &watchServer{
			stream:       stream,
			mgr:          wm,
			sendCancelCh: make(chan *watchImpl, wm.cancelChannelSize),
		}
	)

	defer cancelFn()

	wm.log.V(-1).Info("Watching")
	defer func() { wm.log.V(-1).Info("Stopping watch", "error", err) }()

	go w.sendCancels(ctx)

	for err == nil {
		var req *etcdserverpb.WatchRequest

		if err = ctx.Err(); err != nil {
			break
		}

		if req, err = stream.Recv(); err != nil {
			break
		}

		wm.log.V(-1).Info("Processing", "request", req)

		switch {
		case req.GetCreateRequest() != nil:
			err = w.accept(ctx, req.GetCreateRequest())
		case req.GetCancelRequest() != nil:
			w.cancel(ctx, req.GetCancelRequest().GetWatchId(), nil)
		default:
			// TODO Handle WatchProgressRequest
		}

		wm.log.V(-1).Info("Processed", "request", req)
	}

	return
}

// TODO Refactor watchManager and watchServer to avoid cyclic dependency.
type watchServer struct {
	sync.Mutex

	stream etcdserverpb.Watch_WatchServer
	mgr    *watchManager

	watches map[int64]*watchImpl
	watchID int64

	sendCancelCh chan *watchImpl
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

	if ws.watches == nil {
		ws.watches = make(map[int64]*watchImpl)
	}

	ws.watches[req.WatchId] = wi

	return wi
}

func (ws *watchServer) unregisterWatch(w watch) {
	ws.Lock()
	defer ws.Unlock()

	delete(ws.watches, w.WatchId())
}

func (ws *watchServer) sendCancels(ctx context.Context) {
	for {
		var (
			w  *watchImpl
			ok bool
		)

		select {
		case <-ctx.Done():
			return
		case w, ok = <-ws.sendCancelCh:
			if !ok {
				return
			}

			if w == nil {
				continue
			}

			ws.sendCancel(ctx, w) // Error is already logged.
		}
	}
}

func (ws *watchServer) sendCancel(ctx context.Context, w *watchImpl) (err error) {
	var (
		watchID        int64
		ok, unregister bool
		watchCtxErr    error
		cancelHeader   *etcdserverpb.ResponseHeader
		cancelReason   string

		log = ws.mgr.log.WithName("sendCancel")
	)

	if w == nil {
		return
	}

	watchID = w.WatchId()

	func() {
		ws.Lock()
		defer ws.Unlock()

		_, ok = ws.watches[watchID]
	}()

	defer func() {
		if unregister {
			ws.unregisterWatch(w)
		}
	}()

	if !ok {
		return
	}

	w.Lock()
	defer w.Unlock()

	if w.cancelSent {
		return
	}

	defer func() {
		log.Info("Sent cancel", "watch request", w.req, "cancelSent", w.cancelSent, "cancelReason", cancelReason, "error", err)
	}()

	if watchCtxErr = w.ctx.Err(); watchCtxErr == nil {
		return // Send cancel only if context is done.
	}

	unregister = true // If the context is done, then the watch must be unregistered.

	if cancelHeader = w.cancelHeader; cancelHeader == nil {
		func() {
			var b = w.watchServer.mgr.backend

			b.RLock()
			defer b.RUnlock()

			cancelHeader = w.watchServer.mgr.backend.newResponseHeader(ctx)
		}()
	}

	if cancelReason = w.cancelReason; len(cancelReason) <= 0 {
		cancelReason = watchCtxErr.Error()
	}

	err = w.stream.Send(&etcdserverpb.WatchResponse{
		Header:       cancelHeader,
		WatchId:      w.req.WatchId,
		Canceled:     true,
		CancelReason: cancelReason,
	})

	w.cancelSent = true // TODO Retry
	return
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

	if metaP, err = ws.mgr.backend.getMetadataPeelableForRevision(ctx, metaHead, revision); err != nil {
		return
	}

	metaP.Close()

	return
}

func (ws *watchServer) accept(ctx context.Context, req *etcdserverpb.WatchCreateRequest) (err error) {
	var (
		w      = ws.registerWatch(ctx, req)
		b      = ws.mgr.backend
		log    = ws.mgr.log.WithName("accept")
		header *etcdserverpb.ResponseHeader
	)

	log.V(-1).Info("accepting", "request", req)
	defer func() { log.V(-1).Info("accept returned", "error", err) }()

	b.RLock()
	defer b.RUnlock()

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

	if err = ws.checkRevisionValid(ctx, req.StartRevision); err != nil && !errors.Is(err, rpctypes.ErrGRPCFutureRev) {
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
		changesOnly: false,
		interval:    &closedOpenInterval{start: req.GetKey(), end: req.GetRangeEnd()},
		watches:     []watch{w},
		log:         ws.mgr.log.WithName("rw"),
	})

	err = b.tickWatchDispatchTicker(ctx)
	return
}

func (ws *watchServer) cancel(ctx context.Context, watchID int64, cause error) {
	var (
		w   watch
		log = ws.mgr.log.WithName("cancel")
		rh  *etcdserverpb.ResponseHeader
	)

	log.V(-1).Info("cancelling", "watchId", watchID, "cause", cause)

	func() {
		ws.Mutex.Lock()
		defer ws.Mutex.Unlock()

		w = ws.watches[watchID]
	}()

	if w == nil {
		return
	}

	func() {
		ws.mgr.backend.RLock()
		defer ws.mgr.backend.RUnlock()

		rh = ws.mgr.backend.newResponseHeader(ctx)
	}()

	w.Cancel(rh, cause)
}

type watchImpl struct {
	sync.Mutex

	watchServer  *watchServer
	ctx          context.Context
	cancelFn     context.CancelFunc
	req          *etcdserverpb.WatchCreateRequest
	stream       etcdserverpb.Watch_WatchServer
	cancelHeader *etcdserverpb.ResponseHeader
	cancelReason string
	cancelSent   bool
}

var _ watch = (*watchImpl)(nil)

func (w *watchImpl) Context() context.Context {
	return w.ctx
}

func (w *watchImpl) Cancel(header *etcdserverpb.ResponseHeader, err error) {
	const defaultCancelReason = "watch closed"

	if w == nil {
		return // TODO Log
	}

	w.Lock()
	defer w.Unlock()

	if w.cancelSent {
		return
	}

	w.cancelFn()

	w.cancelHeader = header
	if err != nil {
		w.cancelReason = err.Error()
	} else {
		w.cancelReason = defaultCancelReason
	}

	w.watchServer.sendCancelCh <- w
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
		log      = w.watchServer.mgr.log.WithName("FilterAndSend").WithValues(
			"key", w.req.GetKey(),
			"rangeEnd", w.req.GetRangeEnd(),
			"startRevision", w.req.GetStartRevision(),
			"watchId", w.req.GetWatchId(),
			"prevKV", w.req.GetPrevKv(),
			"filters", w.req.GetFilters(),
		)
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
			if ev.Type == mvccpb.PUT && ev.Kv.CreateRevision != ev.Kv.ModRevision && ev.PrevKv == nil {
				log.Info("PrevKV=nil", "ev", ev)
			}
			events = append(events, ev)
		}
	}

	if !w.ProgressNotify() && len(events) == 0 {
		return
	}

	// TODO fragment, filter
	err = w.stream.Send(&etcdserverpb.WatchResponse{Header: header, WatchId: w.req.WatchId, Events: events})
	return
}
