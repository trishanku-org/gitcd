package backend

import (
	"context"
	"fmt"
	"strconv"

	"github.com/trishanku/gitcd/pkg/git"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type int64Cmp int64

func (a int64Cmp) Cmp(b int64) cmpResult {
	switch {
	case int64(a) < b:
		return cmpResultLess
	case int64(a) == b:
		return cmpResultEqual
	default:
		return cmpResultMore
	}
}

type int64ReaderFunc func(*etcdserverpb.Compare) int64

var compareTarget_int64Reader = map[etcdserverpb.Compare_CompareTarget]int64ReaderFunc{
	etcdserverpb.Compare_CREATE:  func(c *etcdserverpb.Compare) int64 { return c.GetCreateRevision() },
	etcdserverpb.Compare_LEASE:   func(c *etcdserverpb.Compare) int64 { return c.GetLease() },
	etcdserverpb.Compare_MOD:     func(c *etcdserverpb.Compare) int64 { return c.GetModRevision() },
	etcdserverpb.Compare_VERSION: func(c *etcdserverpb.Compare) int64 { return c.GetVersion() },
}

func getCompareTargetInt64(c *etcdserverpb.Compare) (int64, error) {
	if readerFn, ok := compareTarget_int64Reader[c.GetTarget()]; ok {
		return readerFn(c), nil
	}

	return 0, fmt.Errorf("unsupported compare target %s", c.GetTarget().String())
}

type checkCompareResultFunc func(cmpResult) bool

var compareResult_checker = map[etcdserverpb.Compare_CompareResult]checkCompareResultFunc{
	etcdserverpb.Compare_EQUAL:     func(r cmpResult) bool { return r == cmpResultEqual },
	etcdserverpb.Compare_NOT_EQUAL: func(r cmpResult) bool { return r != cmpResultEqual },
	etcdserverpb.Compare_GREATER:   func(r cmpResult) bool { return r == cmpResultMore },
	etcdserverpb.Compare_LESS:      func(r cmpResult) bool { return r == cmpResultLess },
}

func checkCompareResult(c *etcdserverpb.Compare, r cmpResult) (bool, error) {
	if checkerFn, ok := compareResult_checker[c.GetResult()]; ok {
		return checkerFn(r), nil
	}

	return false, fmt.Errorf("unsupported compare result %s", c.GetResult().String())
}

func (b *backend) readInt64(ctx context.Context, te git.TreeEntry, path string) (ignore, skip bool, i int64, err error) {
	var (
		t git.Tree
		v []byte
	)

	if t, err = b.repo.ObjectGetter().GetTree(ctx, te.EntryID()); err != nil {
		ignore, err = true, nil
		return
	}

	defer t.Close()

	if te, err = t.GetEntryByPath(ctx, path); err != nil {
		ignore, err = true, nil
		return
	}

	if te.EntryType() != git.ObjectTypeBlob {
		ignore, skip = true, true
		return
	}

	if v, err = b.getContentForTreeEntry(ctx, te); err != nil {
		return
	}

	i, err = strconv.ParseInt(string(v), 10, 64)
	return
}

func copyResponseHeaderFrom(h *etcdserverpb.ResponseHeader) *etcdserverpb.ResponseHeader {
	var r = *h
	return &r
}

func (b *backend) doTxnRequestOps(
	ctx context.Context,
	metaHead git.Commit,
	requestOps []*etcdserverpb.RequestOp,
	res *etcdserverpb.TxnResponse,
	newRevision int64,
	commitTreeFn commitTreeFunc,
) (
	metaMutated bool,
	newMetaHeadID git.ObjectID,
	dataMutated bool,
	newDataHeadID git.ObjectID,
	err error,
) {
	var (
		rangeRes                           *etcdserverpb.RangeResponse
		putRes                             *etcdserverpb.PutResponse
		deleteRangeRes                     *etcdserverpb.DeleteRangeResponse
		txnRes                             *etcdserverpb.TxnResponse
		subMetaMutated, subDataMutated     bool
		subNewMetaHeadID, subNewDataHeadID git.ObjectID
		subHeader                          *etcdserverpb.ResponseHeader
	)

	if len(requestOps) == 0 {
		return
	}

	subHeader = copyResponseHeaderFrom(res.Header)

	// Do first requestOp.
	switch sop := requestOps[0].GetRequest().(type) {
	case *etcdserverpb.RequestOp_RequestRange:
		if rangeRes, err = b.doRange(ctx, metaHead, sop.RequestRange); err != nil {
			return
		}

		res.Responses = append(res.Responses, &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponseRange{
				ResponseRange: rangeRes,
			},
		})

	case *etcdserverpb.RequestOp_RequestPut:
		putRes = &etcdserverpb.PutResponse{Header: subHeader}

		if metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doPut(
			ctx,
			metaHead,
			sop.RequestPut,
			putRes,
			newRevision,
			commitTreeFn,
		); err != nil {
			return
		}

		res.Responses = append(res.Responses, &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponsePut{
				ResponsePut: putRes,
			},
		})

	case *etcdserverpb.RequestOp_RequestDeleteRange:
		deleteRangeRes = &etcdserverpb.DeleteRangeResponse{Header: subHeader}

		if metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doDeleteRange(
			ctx,
			metaHead,
			sop.RequestDeleteRange,
			deleteRangeRes,
			newRevision,
			commitTreeFn,
		); err != nil {
			return
		}

		res.Responses = append(res.Responses, &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
				ResponseDeleteRange: deleteRangeRes,
			},
		})

	case *etcdserverpb.RequestOp_RequestTxn:
		txnRes = &etcdserverpb.TxnResponse{Header: subHeader}

		if metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doTxn(
			ctx,
			metaHead,
			sop.RequestTxn,
			txnRes,
			newRevision,
			commitTreeFn,
		); err != nil {
			return
		}

		res.Responses = append(res.Responses, &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponseTxn{
				ResponseTxn: txnRes,
			},
		})

	default:
		err = fmt.Errorf("unsupported request type %T", sop)
	}

	if metaMutated {
		if metaHead, err = b.repo.ObjectGetter().GetCommit(ctx, newMetaHeadID); err != nil {
			return
		}

		defer metaHead.Close()
	}

	// Delegate rest of the requestOps recursively.
	if subMetaMutated, subNewMetaHeadID, subDataMutated, subNewDataHeadID, err = b.doTxnRequestOps(
		ctx,
		metaHead,
		requestOps[1:],
		res,
		newRevision,
		b.replaceCurrentCommit,
	); err != nil {
		return
	}

	if subMetaMutated {
		metaMutated, newMetaHeadID = true, subNewMetaHeadID
	}

	if subDataMutated {
		dataMutated, newDataHeadID = true, subNewDataHeadID
	}

	return
}

func (b *backend) doTxn(
	ctx context.Context,
	metaHead git.Commit,
	req *etcdserverpb.TxnRequest,
	res *etcdserverpb.TxnResponse,
	newRevision int64,
	commitTreeFn commitTreeFunc,
) (
	metaMutated bool,
	newMetaHeadID git.ObjectID,
	dataMutated bool,
	newDataHeadID git.ObjectID,
	err error,
) {
	var (
		metaRoot, dataRoot git.Tree
		compare            = true
		ie                 *intervalExplorer
		requestOps         []*etcdserverpb.RequestOp
	)

	if metaHead != nil {
		var dataHead git.Commit

		if metaRoot, err = b.repo.Peeler().PeelToTree(ctx, metaHead); err != nil {
			return
		}

		defer metaRoot.Close()

		if dataHead, err = b.getDataCommitForMetadata(ctx, metaRoot); err != nil {
			return
		}

		defer dataHead.Close()

		if dataRoot, err = b.repo.Peeler().PeelToTree(ctx, dataHead); err != nil {
			return
		}

		defer dataRoot.Close()
	}

	ie = &intervalExplorer{
		keyPrefix: b.keyPrefix,
		repo:      b.repo,
	}

	for _, c := range req.GetCompare() {
		var (
			receiverFn intervalExplorerReceiverFunc
			filterFns  []intervalExplorerFilterFunc
		)

		if !compare {
			break
		}

		if c == nil {
			continue
		}

		if c.GetTarget() == etcdserverpb.Compare_VALUE {
			if dataRoot == nil {
				compare = false
				continue
			}

			ie.tree = dataRoot

			receiverFn = func(ctx context.Context, k string, te git.TreeEntry) (done, skip bool, err error) {
				var (
					value, target []byte
					entryCompare  bool
				)

				if value, err = b.getContentForTreeEntry(ctx, te); err != nil {
					return
				}

				if entryCompare, err = checkCompareResult(c, key(value).Cmp(key(target))); err != nil {
					return
				}

				if !entryCompare {
					compare = false
					done = true
				}

				return
			}

			filterFns = append(filterFns, newObjectTypeFilter(git.ObjectTypeBlob))
		} else {
			if metaRoot == nil {
				compare = false
				continue
			}

			ie.tree = metaRoot

			receiverFn = func(ctx context.Context, k string, te git.TreeEntry) (done, skip bool, err error) {
				var (
					value, target        int64
					ignore, entryCompare bool
				)

				if ignore, skip, value, err = b.readInt64(ctx, te, c.GetTarget().String()); err != nil {
					return
				}

				if ignore || skip {
					return
				}

				if target, err = getCompareTargetInt64(c); err != nil {
					return
				}

				if entryCompare, err = checkCompareResult(c, int64Cmp(value).Cmp(target)); err != nil {
					return
				}

				if !entryCompare {
					compare = false
					done = true
				}

				return
			}

			filterFns = append(filterFns, newObjectTypeFilter(git.ObjectTypeTree))
		}

		ie.interval = newClosedOpenInterval(c.GetKey(), c.GetRangeEnd())

		if err = ie.forEachMatchingKey(ctx, receiverFn, filterFns...); err != nil {
			return
		}
	}

	if compare {
		requestOps = req.Success
	} else {
		requestOps = req.Failure
	}

	metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doTxnRequestOps(ctx, metaHead, requestOps, res, newRevision, commitTreeFn)
	return
}

func (b *backend) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (res *etcdserverpb.TxnResponse, err error) {
	var (
		metaRef                      git.Reference
		metaHead                     git.Commit
		metaMutated, dataMutated     bool
		newMetaHeadID, newDataHeadID git.ObjectID
		newRevision                  int64
	)

	if metaRef, err = b.getMetadataReference(ctx); err == nil {
		defer metaRef.Close()
	}

	if metaHead, err = b.repo.Peeler().PeelToCommit(ctx, metaRef); err == nil {
		defer metaHead.Close()
	}

	res = &etcdserverpb.TxnResponse{
		Header: b.newResponseHeaderFromMetaPeelable(ctx, metaHead),
	}

	newRevision = res.GetHeader().GetRevision() + 1

	if metaMutated, newMetaHeadID, dataMutated, newDataHeadID, err = b.doTxn(
		ctx,
		metaHead,
		req,
		res,
		newRevision,
		b.inheritCurrentCommit,
	); err != nil {
		return
	}

	setHeaderRevision(res.Header, metaMutated, newRevision)

	err = b.advanceReferences(ctx, metaMutated, newMetaHeadID, dataMutated, newDataHeadID, res.GetHeader().GetRevision())
	return
}
