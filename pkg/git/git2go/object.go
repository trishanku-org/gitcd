package git2go

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"reflect"
	"time"

	impl "github.com/libgit2/git2go/v31"
	"github.com/trishanku/gitcd/pkg/git"
)

func toImplObjectType(t git.ObjectType) impl.ObjectType {
	if t > 0 {
		return impl.ObjectType(t)
	}

	return impl.ObjectInvalid
}

func toGitObjectType(t impl.ObjectType) git.ObjectType {
	if t > 0 {
		return git.ObjectType(t)
	}

	return git.ObjectTypeInvalid
}

// object implements the Object interface defined in the parent git package.
type object impl.Object

var _ git.Object = (*object)(nil)

func (o *object) impl() *impl.Object   { return (*impl.Object)(o) }
func (o *object) Close() error         { return free(o.impl()) }
func (o *object) ID() git.ObjectID     { return git.ObjectID(*o.impl().Id()) }
func (o *object) Type() git.ObjectType { return toGitObjectType(o.impl().Type()) }

func (o *object) Peel(ctx context.Context, r git.ObjectReceiver) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var implO = o.impl()

	if implTO, err := implO.Peel(toImplObjectType(r.Type())); err != nil {
		return err
	} else {
		return r.Receive(ctx, (*object)(implTO))
	}
}

// blob implements the Blob interface defined in the parent git package.
type blob impl.Blob

var _ git.Blob = (*blob)(nil)

func (b *blob) impl() *impl.Blob                                     { return (*impl.Blob)(b) }
func (b *blob) Close() error                                         { return free(b.impl()) }
func (b *blob) object() *object                                      { var implB = b.impl(); return (*object)(&implB.Object) }
func (b *blob) ID() git.ObjectID                                     { return b.object().ID() }
func (b *blob) Type() git.ObjectType                                 { return b.object().Type() }
func (b *blob) Peel(ctx context.Context, r git.ObjectReceiver) error { return b.object().Peel(ctx, r) }
func (b *blob) Size() git.BlobSize                                   { return git.BlobSize(b.impl().Size()) }
func (b *blob) Content() ([]byte, error)                             { return b.impl().Contents(), nil }

func (b *blob) ContentReader() (io.ReadCloser, error) {
	return ioutil.NopCloser(bytes.NewReader(b.impl().Contents())), nil
}

// treeEntry implements the TreeEntry interface defined in the parent git package.
type treeEntry impl.TreeEntry

var _ git.TreeEntry = (*treeEntry)(nil)

func (te *treeEntry) impl() *impl.TreeEntry     { return (*impl.TreeEntry)(te) }
func (te *treeEntry) EntryName() string         { return te.impl().Name }
func (te *treeEntry) EntryID() git.ObjectID     { return git.ObjectID(*te.impl().Id) }
func (te *treeEntry) EntryType() git.ObjectType { return toGitObjectType(te.impl().Type) }
func (te *treeEntry) EntryMode() git.Filemode   { return git.Filemode(te.impl().Filemode) }

// tree implements the Tree interface defined in the parent git package.
type tree impl.Tree

var _ git.Tree = (*tree)(nil)

func (t *tree) impl() *impl.Tree                                     { return (*impl.Tree)(t) }
func (t *tree) Close() error                                         { return free(t.impl()) }
func (t *tree) object() *object                                      { var implO = t.impl(); return (*object)(&implO.Object) }
func (t *tree) ID() git.ObjectID                                     { return t.object().ID() }
func (t *tree) Type() git.ObjectType                                 { return t.object().Type() }
func (t *tree) Peel(ctx context.Context, r git.ObjectReceiver) error { return t.object().Peel(ctx, r) }

func (t *tree) GetEntryByPath(ctx context.Context, path string) (git.TreeEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if implTE, err := t.impl().EntryByPath(path); err != nil {
		return nil, err
	} else {
		return (*treeEntry)(implTE), nil
	}
}

func (t *tree) ForEachEntry(ctx context.Context, fn git.TreeEntryReceiverFunc) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	for i, nentries := uint64(0), t.impl().EntryCount(); i < nentries; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		if done, err := fn(ctx, (*treeEntry)(t.impl().EntryByIndex(i))); err != nil {
			return err
		} else if done {
			return nil
		}
	}

	return nil
}

// commit implements the Commit interface defined in the parent git package.
type commit impl.Commit

var _ git.Commit = (*commit)(nil)

func (c *commit) impl() *impl.Commit   { return (*impl.Commit)(c) }
func (c *commit) Close() error         { return free(c.impl()) }
func (c *commit) object() *object      { var implO = c.impl(); return (*object)(&implO.Object) }
func (c *commit) ID() git.ObjectID     { return c.object().ID() }
func (c *commit) Type() git.ObjectType { return c.object().Type() }

func (c *commit) Peel(ctx context.Context, r git.ObjectReceiver) error {
	return c.object().Peel(ctx, r)
}

func (c *commit) Message() string      { return c.impl().Message() }
func (c *commit) TreeID() git.ObjectID { return git.ObjectID(*c.impl().TreeId()) }

func (c *commit) ForEachParentID(ctx context.Context, fn git.CommitIDReceiverFunc) (err error) {
	if err = ctx.Err(); err != nil {
		return
	}

	for i, nparents := uint(0), c.impl().ParentCount(); i < nparents; i++ {
		var done bool

		if err = ctx.Err(); err != nil {
			return
		}

		if done, err = fn(ctx, git.ObjectID(*c.impl().ParentId(i))); err != nil || done {
			return
		}
	}

	return
}

func (c *commit) ForEachParent(ctx context.Context, fn git.CommitReceiverFunc) (err error) {
	if err = ctx.Err(); err != nil {
		return
	}

	for i, nparents := uint(0), c.impl().ParentCount(); i < nparents; i++ {
		var done bool

		if done, err = callCommitReceiverAndClose(ctx, (*commit)(c.impl().Parent(i)), fn); err != nil || done {
			return
		}
	}

	return
}

func callCommitReceiverAndClose(ctx context.Context, c git.Commit, fn git.CommitReceiverFunc) (done bool, err error) {
	if err = ctx.Err(); err != nil {
		return
	}

	defer c.Close()

	return fn(ctx, c)
}

type blobBuilder struct {
	repo    *repository
	content []byte
}

func (b *blobBuilder) Close() error { return nil }

func (b *blobBuilder) Build(ctx context.Context) (id git.ObjectID, err error) {
	var oid *impl.Oid

	if err = ctx.Err(); err != nil {
		return
	}

	if oid, err = b.repo.impl.CreateBlobFromBuffer(b.content); err == nil {
		id = git.ObjectID(*oid)
	}

	return
}

func (b *blobBuilder) SetContent(content []byte) error {
	b.content = content
	return nil
}

type treeBuilder impl.TreeBuilder

func (b *treeBuilder) impl() *impl.TreeBuilder { return (*impl.TreeBuilder)(b) }
func (b *treeBuilder) Close() error            { return free(b.impl()) }

func (b *treeBuilder) Build(ctx context.Context) (id git.ObjectID, err error) {
	var oid *impl.Oid

	if err = ctx.Err(); err != nil {
		return
	}

	if oid, err = b.impl().Write(); err == nil {
		id = git.ObjectID(*oid)
	}

	return
}

func (b *treeBuilder) AddEntry(entryName string, entryID git.ObjectID, entryMode git.Filemode) error {
	return b.impl().Insert(entryName, (*impl.Oid)(&entryID), impl.Filemode(entryMode))
}

func (b *treeBuilder) RemoveEntry(entryName string) error {
	return b.impl().Remove(entryName)
}

type commitBuilder struct {
	repo                        *repository
	authorName, committerName   string
	authorEmail, committerEmail string
	authorTime, committerTime   time.Time
	message                     string
	treeID                      git.ObjectID
	parentIDs                   []git.ObjectID
}

func (b *commitBuilder) Close() error { return nil }

func defaultCommitSignatureFrom(t, s *impl.Signature) error {
	var emptyTime = time.Time{}

	if len(t.Name) == 0 {
		if len(s.Name) == 0 {
			return errors.New("invalid empty commit name")
		}

		t.Name = s.Name
	}

	if len(t.Email) == 0 {
		if len(s.Email) == 0 {
			return errors.New("invalid empty commit email")
		}

		t.Email = s.Email
	}

	if reflect.DeepEqual(t.When, emptyTime) {
		if reflect.DeepEqual(s.When, emptyTime) {
			t.When = time.Now()
			return nil
		}

		t.When = s.When
	}

	return nil
}

func (b *commitBuilder) Build(ctx context.Context) (id git.ObjectID, err error) {
	var (
		parentOids        = make([]*impl.Oid, len(b.parentIDs))
		author, committer *impl.Signature
		newOid            *impl.Oid
	)

	if err = ctx.Err(); err != nil {
		return
	}

	for i := range b.parentIDs {
		parentOids[i] = (*impl.Oid)(&b.parentIDs[i])
	}

	author = &impl.Signature{Name: b.authorName, Email: b.authorEmail, When: b.authorTime}
	committer = &impl.Signature{Name: b.committerName, Email: b.committerEmail, When: b.committerTime}

	if err = defaultCommitSignatureFrom(author, committer); err != nil {
		return
	}

	if err = defaultCommitSignatureFrom(committer, author); err != nil {
		return
	}

	if err = ctx.Err(); err != nil {
		return
	}

	if newOid, err = b.repo.impl.CreateCommitFromIds("", author, committer, b.message, (*impl.Oid)(&b.treeID), parentOids...); err == nil {
		id = git.ObjectID(*newOid)
	}

	return
}

func (b *commitBuilder) SetAuthorName(name string) error      { b.authorName = name; return nil }
func (b *commitBuilder) SetAuthorEmail(email string) error    { b.authorEmail = email; return nil }
func (b *commitBuilder) SetAuthorTime(t time.Time) error      { b.authorTime = t; return nil }
func (b *commitBuilder) SetCommitterName(name string) error   { b.committerName = name; return nil }
func (b *commitBuilder) SetCommitterEmail(email string) error { b.committerEmail = email; return nil }
func (b *commitBuilder) SetCommitterTime(t time.Time) error   { b.committerTime = t; return nil }
func (b *commitBuilder) SetMessage(message string) error      { b.message = message; return nil }
func (b *commitBuilder) SetTree(t git.Tree) error             { b.treeID = t.ID(); return nil }
func (b *commitBuilder) SetTreeID(id git.ObjectID) error      { b.treeID = id; return nil }

func (b *commitBuilder) AddParents(parents ...git.Commit) error {
	for _, p := range parents {
		b.parentIDs = append(b.parentIDs, p.ID())
	}
	return nil
}

func (b *commitBuilder) AddParentIDs(parentIDs ...git.ObjectID) error {
	b.parentIDs = append(b.parentIDs, parentIDs...)
	return nil
}
