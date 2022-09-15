package git

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"time"
)

// ObjectType defines the type for the type of Object in a Git repository.
type ObjectType int8

const (
	ObjectTypeInvalid = ObjectType(iota - 1)
	_
	ObjectTypeCommit
	ObjectTypeTree
	ObjectTypeBlob
	ObjectTypeTag
)

// ObjectReceiver defines the interface that receives specific ObjectTypes.
type ObjectReceiver interface {
	Type() ObjectType
	Receive(context.Context, Object) error
}

// ObjectReceiverFunc defines the signature for a function receiving an object.
type ObjectReceiverFunc func(context.Context, Object) error

// NewObjectReceiver constructs a new ObjectReceiver for the given type and with the given receiver function.
func NewObjectReceiver(t ObjectType, receiverFn ObjectReceiverFunc) ObjectReceiver {
	return &objReceiver{t: t, receiverFn: receiverFn}
}

type objReceiver struct {
	t          ObjectType
	receiverFn ObjectReceiverFunc
}

func (r *objReceiver) Type() ObjectType {
	return r.t
}

func (r *objReceiver) Receive(ctx context.Context, o Object) error {
	return r.receiverFn(ctx, o)
}

// Peelable defines the interface for something that can be peeled to a specific ObjectType.
type Peelable interface {
	io.Closer

	Peel(context.Context, ObjectReceiver) error
}

// ObjectID defines the type for the Object IDs in a Git repository.
type ObjectID [20]byte

func (id ObjectID) String() string { return hex.EncodeToString(id[:]) }

const objectIDEncodingLength = 40

func NewObjectID(s string) (id ObjectID, err error) {
	var sl []byte

	if len(s) != objectIDEncodingLength {
		return id, fmt.Errorf("invalid ObjectID %q", s)
	}

	if sl, err = hex.DecodeString(s); err != nil {
		return
	}

	if len(sl) != len(id) {
		return id, fmt.Errorf("invalid ObjectID %q", s)
	}

	copy(id[:], sl)
	return
}

// Object defines access to an Object in a Git repository.
// It must be closed after use to release the resources it holds.
type Object interface {
	Peelable

	ID() ObjectID
	Type() ObjectType
}

// BlobSize defines the type for the size of a Blob Object.
type BlobSize int64

// Blob defines accesss to a Blob Object.
type Blob interface {
	Object

	Size() BlobSize
	Content() ([]byte, error)
	ContentReader() (io.ReadCloser, error)
}

// Filemode defines the type for the filemode of a TreeEntry.
type Filemode uint32

const (
	FilemodeBlob           = Filemode(0100644)
	FilemodeBlobExecutable = Filemode(0100755)
	FilemodeTree           = Filemode(0040000)
)

type TreeEntry interface {
	EntryName() string
	EntryID() ObjectID
	EntryType() ObjectType
	EntryMode() Filemode
}

// TreeEntryReceiverFunc defines the signature for a function receiving a TreeEntry.
type TreeEntryReceiverFunc func(context.Context, TreeEntry) (done bool, err error)

// Tree defines access to a Tree Object.
type Tree interface {
	Object

	// GetEntryByPath tries to look up an entry in the sub-tree for the given path.
	GetEntryByPath(context.Context, string) (TreeEntry, error)

	// ForEachEntry iterates through the immediate shallow entries of the tree at its level.
	ForEachEntry(context.Context, TreeEntryReceiverFunc) error
}

// CommitIDReceiverFunc defines the signature for a function receiving a Commit ID.
type CommitIDReceiverFunc func(context.Context, ObjectID) (done bool, err error)

// CommitReceiverFunc defines the signature for a function receiving a Commit.
type CommitReceiverFunc func(context.Context, Commit) (done bool, err error)

// Commit defines access to a Commit object.
type Commit interface {
	Object

	Message() string
	TreeID() ObjectID

	// ForEachParentID iterates through each immediate parent commit ID.
	ForEachParentID(context.Context, CommitIDReceiverFunc) error

	// ForEachParent iterates through each immediate parent commit.
	// It closes each parent commit object after the receiver function has been called during the iteration.
	ForEachParent(context.Context, CommitReceiverFunc) error
}

// ObjectConverter defines the interface to convert a generic Object to specific ObjectType.
type ObjectConverter interface {
	ToBlob(context.Context, Object) (Blob, error)
	ToTree(context.Context, Object) (Tree, error)
	ToCommit(context.Context, Object) (Commit, error)
}

// ReferenceName defines the type for the names of the references of a Git repository.
type ReferenceName string

// Reference defines access to a reference of a Git repository.
// It must be closed after use to release the resources it holds.
type Reference interface {
	Peelable

	Name() ReferenceName

	IsBranch() bool
	IsRemote() bool
	IsSymbolic() bool
}

// ReferenceCollection defines access to the references of a Git repository.
type ReferenceCollection interface {
	io.Closer

	Get(context.Context, ReferenceName) (Reference, error)
	Create(ctx context.Context, refName ReferenceName, id ObjectID, force bool, msg string) error
}

type ObjectGetter interface {
	GetObject(context.Context, ObjectID) (Object, error)
	GetBlob(context.Context, ObjectID) (Blob, error)
	GetTree(context.Context, ObjectID) (Tree, error)
	GetCommit(context.Context, ObjectID) (Commit, error)
}

type Peeler interface {
	PeelToBlob(context.Context, Peelable) (Blob, error)
	PeelToTree(context.Context, Peelable) (Tree, error)
	PeelToCommit(context.Context, Peelable) (Commit, error)
}

// TreeWalkerReceiverFunc defines the signature of the receiver function while walking a Tree.
// The string parameter would be the path to the parent of the TreeEntry.
type TreeWalkerReceiverFunc func(ctx context.Context, parentPath string, te TreeEntry) (done, skip bool, err error)

type TreeWalker interface {
	io.Closer

	ForEachTreeEntry(context.Context, Tree, TreeWalkerReceiverFunc) error
}

// CommitWalkerReceiverFunc defines the signature for a function receiving a Commit while walking a commit history.
type CommitWalkerReceiverFunc func(context.Context, Commit) (done, skip bool, err error)

// CommitWalker defines the interface to walk a commit history in pre-order.
// It closes each parent commit object after the receiver function has been called during the iteration.
type CommitWalker interface {
	io.Closer

	ForEachCommit(context.Context, Commit, CommitWalkerReceiverFunc) error
}

// ObjectBuilder defines the interface to build an object in a Git repository.
type ObjectBuilder interface {
	io.Closer

	Build(context.Context) (ObjectID, error)
}

// BlobBuilder defines the interface to build blob in a Git repository.
type BlobBuilder interface {
	ObjectBuilder

	SetContent([]byte) error
}

// TreeBuilder defines the interface to build a tree in a Git repository.
type TreeBuilder interface {
	ObjectBuilder

	AddEntry(entryName string, entryID ObjectID, entryMode Filemode) error
	RemoveEntry(entryName string) error
}

type CommitBuilder interface {
	ObjectBuilder

	SetAuthorName(string) error
	SetAuthorEmail(string) error
	SetAuthorTime(time.Time) error
	SetCommitterName(string) error
	SetCommitterEmail(string) error
	SetCommitterTime(time.Time) error
	SetMessage(string) error
	SetTree(Tree) error
	SetTreeID(ObjectID) error
	AddParents(...Commit) error
	AddParentIDs(...ObjectID) error
}

// ReferenceNameReceiverFunc defines the signature for the receiver function while walking reference names.
type ReferenceNameReceiverFunc func(context.Context, ReferenceName) (done bool, err error)

// DiffChangeType defines the possible types of individual changes in a diff of a tree.
type DiffChangeType int8

const (
	DiffChangeTypeAdded DiffChangeType = iota + 1
	DiffChangeTypeDeleted
	DiffChangeTypeModified
)

// DiffChange defines the interface to access individual blob changes while walking a diff for a tree.
type DiffChange interface {
	Path() string
	Type() DiffChangeType
	OldBlobID() ObjectID
	NewBlobID() ObjectID
}

// DiffChangeReceiverFunc defines the signature for the receiver function while walking a diff for a tree.
type DiffChangeReceiverFunc func(context.Context, DiffChange) (done bool, err error)

// Diff defines the interface to interact with the diff between two trees.
type Diff interface {
	io.Closer

	ForEachDiffChange(context.Context, DiffChangeReceiverFunc) error
}

type MergeConfictResolution int8

const (
	MergeConfictResolutionFavorOurs = MergeConfictResolution(iota + 1)
	MergeConfictResolutionFavorTheirs

	DefaultConflictResolution = MergeConfictResolutionFavorTheirs
)

// MergeRetentionPolicy defines the interface to check if a tree entry must be retained during merge/rebase.
type MergeRetentionPolicy interface {
	Retain(ctx context.Context, tePath string) (retain bool, err error)
}

var DefaultMergeRetentionPolicy = AllMergeRetentionPolicy()

func AllMergeRetentionPolicy() MergeRetentionPolicy { return allMRP{} }

type allMRP struct{}

func (allMRP) Retain(_ context.Context, _ string) (retain bool, err error) { retain = true; return }

func NoneMergeRetentionPolicy() MergeRetentionPolicy { return noneMRP{} }

type noneMRP struct{}

func (noneMRP) Retain(_ context.Context, _ string) (retain bool, err error) { return }

// OrMergeRetentionPolicy returns a policy which retains if any one of the given policies retains.
func OrMergeRetentionPolicy(mrps ...MergeRetentionPolicy) MergeRetentionPolicy {
	return orMRP(mrps)
}

// orMRP retains if any one of the given policies retains.
type orMRP []MergeRetentionPolicy

func (o orMRP) Retain(ctx context.Context, tePath string) (retain bool, err error) {
	for _, mrp := range o {
		if retain, err = mrp.Retain(ctx, tePath); retain || err != nil {
			return
		}
	}

	return
}

// AndMergeRetentionPolicy returns a policy which retains if all of the given policies retain.
func AndMergeRetentionPolicy(mrps ...MergeRetentionPolicy) MergeRetentionPolicy {
	return andMRP(mrps)
}

// andMRP retains only if all of the policies retain.
type andMRP []MergeRetentionPolicy

func (a andMRP) Retain(ctx context.Context, tePath string) (retain bool, err error) {
	for _, mrp := range a {
		if retain, err = mrp.Retain(ctx, tePath); !retain || err != nil {
			return
		}
	}

	return
}

// NotMergeRetentionPolicy returns a policy which negates the given policy.
func NotMergeRetentionPolicy(mrp MergeRetentionPolicy) MergeRetentionPolicy {
	return notMRP{mrp: mrp}
}

// notMRP negates the given policy.
type notMRP struct {
	mrp MergeRetentionPolicy
}

func (n notMRP) Retain(ctx context.Context, tePath string) (retain bool, err error) {
	retain, err = n.mrp.Retain(ctx, tePath)
	retain = !retain
	return
}

// RegexpMergeRetentionPolicy returns a policy which retains paths which matches the given regexp pattern.
func RegexpMergeRetentionPolicy(re *regexp.Regexp) MergeRetentionPolicy {
	return (*regexpMRP)(re)
}

// regexpMRP retains if the tree entry path matches the given regexp.
type regexpMRP regexp.Regexp

func (r *regexpMRP) regexp() *regexp.Regexp {
	return (*regexp.Regexp)(r)
}

func (r *regexpMRP) Retain(ctx context.Context, tePath string) (retain bool, err error) {
	retain = r.regexp().MatchString(tePath)
	return
}

// Merger defines the interface to merge changes from diffrent commit trees.
// It only merges the commit trees and automatically resolves conflicts.
// Creating the new commit based on the merged tree is out of scope of the Merger.
// So, the core functionality here can be used to do actual merges, rebases or cherry-picks.
//
// The overall options and preferances such as favoring ours or theirs to resolve conflicts,
// retention policy to filter changes coming in or allowing fast-forward changes can be
// configured in the Merger before merging the commit trees.
type Merger interface {
	io.Closer

	GetConfictResolution() MergeConfictResolution
	GetRetentionPolicy() MergeRetentionPolicy

	SetConflictResolution(conflictResolution MergeConfictResolution)
	SetRetentionPolicy(retentionPolicy MergeRetentionPolicy)

	MergeTrees(ctx context.Context, ancestor, ours, theirs Tree) (mutate bool, treeID ObjectID, err error)
	MergeTreesFromCommits(ctx context.Context, ours, theirs Commit) (mutated bool, treeID ObjectID, fastForward bool, err error)
	MergeBase(ctx context.Context, ours, theirs Commit) (baseCommitID ObjectID, err error)
}

// RemoteName defines the type for the names of the remotes of a Git repository.
type RemoteName string

// RefSpec defines the type for refspecs of remotes.
type RefSpec string

// Remote defines access to a remote of a Git repository.
// It must be closed after use to release the resources it holds.
type Remote interface {
	io.Closer

	Name() RemoteName
	Fetch(ctx context.Context, refSpecs []RefSpec) error
	Push(ctx context.Context, refSpecs []RefSpec) error
}

// RemoteCollection defines access to the remotes of a Git repository.
type RemoteCollection interface {
	io.Closer

	Get(context.Context, RemoteName) (Remote, error)
}

// Repository defines access to a Git repository.
type Repository interface {
	io.Closer

	References() (ReferenceCollection, error)
	Remotes() (RemoteCollection, error)

	ObjectGetter() ObjectGetter
	ObjectConverter() ObjectConverter
	Peeler() Peeler
	TreeWalker() TreeWalker
	CommitWalker() CommitWalker
	BlobBuilder(context.Context) (BlobBuilder, error)
	TreeBuilder(context.Context) (TreeBuilder, error)
	TreeBuilderFromTree(context.Context, Tree) (TreeBuilder, error)
	CommitBuilder(context.Context) (CommitBuilder, error)

	TreeDiff(ctx context.Context, oldT, newT Tree) (Diff, error)

	Merger(Errors) Merger

	Size() (int64, error)
}

type Errors interface {
	IsNotFound(err error) bool
	IsIterOver(err error) bool

	IgnoreNotFound(err error) error
	IgnoreIterOver(err error) error
}

// Interface defines access to a Git implementation.
type Interface interface {
	OpenOrInitBareRepository(ctx context.Context, path string) (Repository, error)
	Errors() Errors
}
