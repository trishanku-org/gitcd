package git2go

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"

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

var _ git.Object = &object{}

func (o *object) impl() *impl.Object   { return (*impl.Object)(o) }
func (o *object) Close() error         { o.impl().Free(); return nil }
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

var _ git.Blob = &blob{}

func (b *blob) impl() *impl.Blob                                     { return (*impl.Blob)(b) }
func (b *blob) Close() error                                         { b.impl().Free(); return nil }
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

var _ git.TreeEntry = &treeEntry{}

func (te *treeEntry) impl() *impl.TreeEntry     { return (*impl.TreeEntry)(te) }
func (te *treeEntry) EntryName() string         { return te.impl().Name }
func (te *treeEntry) EntryID() git.ObjectID     { return git.ObjectID(*te.impl().Id) }
func (te *treeEntry) EntryType() git.ObjectType { return toGitObjectType(te.impl().Type) }
func (te *treeEntry) EntryMode() git.Filemode   { return git.Filemode(te.impl().Filemode) }

// tree implements the Tree interface defined in the parent git package.
type tree impl.Tree

var _ git.Tree = &tree{}

func (t *tree) impl() *impl.Tree                                     { return (*impl.Tree)(t) }
func (t *tree) Close() error                                         { t.impl().Free(); return nil }
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

var _ git.Commit = &commit{}

func (c *commit) impl() *impl.Commit   { return (*impl.Commit)(c) }
func (c *commit) Close() error         { c.impl().Free(); return nil }
func (c *commit) object() *object      { var implO = c.impl(); return (*object)(&implO.Object) }
func (c *commit) ID() git.ObjectID     { return c.object().ID() }
func (c *commit) Type() git.ObjectType { return c.object().Type() }

func (c *commit) Peel(ctx context.Context, r git.ObjectReceiver) error {
	return c.object().Peel(ctx, r)
}

func (c *commit) Message() string      { return c.impl().Message() }
func (c *commit) TreeID() git.ObjectID { return git.ObjectID(*c.impl().TreeId()) }

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
