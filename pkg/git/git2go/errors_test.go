package git2go

import (
	"errors"
	"fmt"

	impl "github.com/libgit2/git2go/v31"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/trishanku/gitcd/pkg/git"
)

var _ = Describe("Errors", func() {
	type checkIsError struct {
		errors []error
		match  types.GomegaMatcher
	}

	type matcherFunc func(interface{}) types.GomegaMatcher

	type checkIgnoreError struct {
		errors    []error
		matcherFn matcherFunc
	}

	var (
		errs git.Errors

		errorsForClosedOpenErrorCodeRange = func(start, end impl.ErrorCode) (errs []error) {
			for i := start; i < end; i++ {
				errs = append(errs, &impl.GitError{Code: i})
			}
			return
		}

		describeIsError = func(spec string, isErrorFn func(errs git.Errors, err error) bool, checks []checkIsError) {
			Describe(spec, func() {
				for _, s := range checks {
					func(s checkIsError) {
						for _, err := range s.errors {
							func(err error) {
								It(fmt.Sprintf("%#v", err), func() { Expect(isErrorFn(errs, err)).To(s.match) })
							}(err)
						}
					}(s)
				}
			})
		}

		matcherFuncFor = func(matchFn func() types.GomegaMatcher) matcherFunc {
			return func(_ interface{}) types.GomegaMatcher {
				return matchFn()
			}
		}
		describeIgnoreError = func(spec string, isErrorFn func(errs git.Errors, err error) error, checks []checkIgnoreError) {
			Describe(spec, func() {
				for _, s := range checks {
					func(s checkIgnoreError) {
						for _, err := range s.errors {
							func(err error) {
								It(fmt.Sprintf("%#v", err), func() { Expect(isErrorFn(errs, err)).To(s.matcherFn(err)) })
							}(err)
						}
					}(s)
				}
			})
		}
	)

	BeforeEach(func() {
		errs = New().Errors()
		Expect(errs).ToNot(BeNil())
	})

	describeIsError(
		"IsNotFound",
		func(errs git.Errors, err error) bool { return errs.IsNotFound(err) },
		[]checkIsError{
			{errors: errorsForClosedOpenErrorCodeRange(impl.ErrorCodeApplyFail, impl.ErrorCodeNotFound), match: BeFalse()},
			{errors: errorsForClosedOpenErrorCodeRange(impl.ErrorCodeNotFound, impl.ErrorCodeNotFound+1), match: BeTrue()},
			{errors: errorsForClosedOpenErrorCodeRange(impl.ErrorCodeGeneric, impl.ErrorCodeOK), match: BeFalse()},
			{errors: []error{errors.New("error")}, match: BeFalse()},
		},
	)

	describeIgnoreError(
		"IgnoreNotFound",
		func(errs git.Errors, err error) error { return errs.IgnoreNotFound(err) },
		[]checkIgnoreError{
			{errors: errorsForClosedOpenErrorCodeRange(impl.ErrorCodeApplyFail, impl.ErrorCodeNotFound), matcherFn: MatchError},
			{errors: errorsForClosedOpenErrorCodeRange(impl.ErrorCodeNotFound, impl.ErrorCodeNotFound+1), matcherFn: matcherFuncFor(BeNil)},
			{errors: errorsForClosedOpenErrorCodeRange(impl.ErrorCodeGeneric, impl.ErrorCodeOK), matcherFn: MatchError},
			{errors: []error{errors.New("error")}, matcherFn: MatchError},
		},
	)
})
