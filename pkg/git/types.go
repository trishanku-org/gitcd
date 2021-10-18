package git

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
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

// CommitReceiverFunc defines the signature for a function receiving a Commit.
type CommitReceiverFunc func(context.Context, Commit) (done bool, err error)

// Commit defines access to a Commit object.
type Commit interface {
	Object

	Message() string
	TreeID() ObjectID

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
	Get(context.Context, ReferenceName) (Reference, error)
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
	ForEachTreeEntry(context.Context, Tree, TreeWalkerReceiverFunc) error
}

// CommitWalkerReceiverFunc defines the signature for a function receiving a Commit while walking a commit history.
type CommitWalkerReceiverFunc func(context.Context, Commit) (done, skip bool, err error)

// CommitWalker defines the interface to walk a commit history in pre-order.
// It closes each parent commit object after the receiver function has been called during the iteration.
type CommitWalker interface {
	ForEachCommit(context.Context, Commit, CommitWalkerReceiverFunc) error
}

// Repository defines access to a Git repository.
type Repository interface {
	io.Closer

	References() (ReferenceCollection, error)

	ObjectGetter() ObjectGetter
	ObjectConverter() ObjectConverter
	Peeler() Peeler
	TreeWalker() TreeWalker
	CommitWalker() CommitWalker
}

// Interface defines access to a Git implementation.
type Interface interface {
	OpenOrInitBareRepository(ctx context.Context, path string) (Repository, error)
}
