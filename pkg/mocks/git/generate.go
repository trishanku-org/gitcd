// Code generated by hack/meta-generate.sh. DO NOT EDIT.
//go:generate mockgen -package git -destination mocks.go github.com/trishanku/gitcd/pkg/git Blob,BlobBuilder,Commit,CommitBuilder,CommitWalker,Errors,Interface,Object,ObjectBuilder,ObjectConverter,ObjectGetter,ObjectReceiver,Peelable,Peeler,Reference,ReferenceCollection,Repository,Tree,TreeBuilder,TreeEntry,TreeWalker
package git
