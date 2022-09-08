package git2go

import (
	"bufio"
	"io"
	"os"
	"path"
	"strings"

	net_url "net/url"

	impl "github.com/libgit2/git2go/v31"
)

const (
	ENV_GIT_CREDENTIAL_USERNAME    = "GIT_CREDENTIAL_USERNAME"
	ENV_GIT_CREDENTIAL_PASSWORD    = "GIT_CREDENTIAL_PASSWORD"
	ENV_XDG_CONFIG_HOME            = "XDG_CONFIG_HOME"
	FILE_HOME_GIT_CREDENTIALS      = ".git-credentials"
	SUBPATH_CONFIG_GIT_CREDENTIALS = "git/credentials"
	SUBPATH_CONFIG                 = ".config"
)

// NewCredentialUserpass reads credentials from the environment variables GIT_CREDENTIAL_USERNAME and GIT_CREDENTIAL_PASSWORD.
// It calls NewCredentialUserpassFromStore as a backup if credentials are not found in the environment variables.
func NewCredentialUserpass(url string, username_from_url string, allowed_types impl.CredentialType) (cred *impl.Credential, err error) {
	var username, password = os.Getenv(ENV_GIT_CREDENTIAL_USERNAME), os.Getenv(ENV_GIT_CREDENTIAL_PASSWORD)

	if len(username) > 0 && len(password) > 0 {
		cred, err = impl.NewCredentialUserpassPlaintext(username, password)
		return
	}

	cred, err = NewCredentialUserpassFromStore(url, username_from_url, allowed_types)
	return
}

// NewCredentialUserpassFromStore reads credentials from the given url and username from where 'git config credential.helper store'
// might read. See https://git-scm.com/docs/git-credential-store for more details.
// This function doesn't work if some other credential.helper is used.
// There is no Go wrapper provided by git2go as of v33. Hence, this functionality is duplicated here. The existing
// libgit2 function git_credential_userpass is not called here to avoid using CGO in this project directly in addition to
// indirectly via git2go. This decision might be reconsidered if more functionality is needed which exists in libgit2
// but is missing from git2go.
func NewCredentialUserpassFromStore(url string, username_from_url string, allowed_types impl.CredentialType) (cred *impl.Credential, err error) {
	var (
		credsPaths []string
		match      *net_url.URL
	)

	if match, err = net_url.Parse(url); err != nil {
		return
	}

	if credsPaths, err = getCredentialsFilePaths(); err != nil || len(credsPaths) <= 0 {
		return
	}

	for _, credsPath := range credsPaths {
		if cred, err = getMatchingCredentialUserPassFromStorePath(credsPath, match, username_from_url, allowed_types); err == nil {
			return
		}
	}

	return
}

func getCredentialsFilePaths() (credentialsPaths []string, err error) {
	var (
		home, xdgConfigHome         string
		homeExists, xdgConfigExists bool
	)

	xdgConfigHome = os.Getenv(ENV_XDG_CONFIG_HOME)
	xdgConfigExists = len(xdgConfigHome) > 0

	if home, err = os.UserHomeDir(); err != nil && !xdgConfigExists {
		return
	}

	if homeExists = len(home) > 0; homeExists {
		credentialsPaths = append(credentialsPaths, path.Join(home, FILE_HOME_GIT_CREDENTIALS))
	}

	if xdgConfigExists {
		credentialsPaths = append(credentialsPaths, path.Join(xdgConfigHome, SUBPATH_CONFIG_GIT_CREDENTIALS))
	} else if homeExists {
		credentialsPaths = append(credentialsPaths, path.Join(home, SUBPATH_CONFIG, SUBPATH_CONFIG_GIT_CREDENTIALS))
	}

	return
}

func getMatchingCredentialUserPassFromStorePath(
	credentialsPath string,
	match *net_url.URL,
	username_from_url string,
	allowed_types impl.CredentialType,
) (
	cred *impl.Credential,
	err error,
) {
	var (
		rc                    io.ReadCloser
		scanner               *bufio.Scanner
		usernameFromURLExists = len(username_from_url) > 0
	)

	if rc, err = os.Open(credentialsPath); err != nil {
		return
	}

	defer rc.Close()

	scanner = bufio.NewScanner(rc)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		var (
			line               = scanner.Text()
			u                  *net_url.URL
			err2               error
			username, password string
			passwordSet        bool
		)

		if u, err2 = net_url.Parse(line); err2 != nil {
			continue // Ignore non-URL lines
		}

		if username = u.User.Username(); usernameFromURLExists && username_from_url != username {
			continue
		}

		if match.Host != u.Host {
			continue
		}

		if len(strings.TrimLeft(u.Path, "/")) > 0 && !strings.HasPrefix(match.Path, u.Path) {
			continue
		}

		if password, passwordSet = u.User.Password(); !passwordSet {
			continue
		}

		if cred, err = impl.NewCredentialUserpassPlaintext(username, password); err == nil {
			return
		}
	}

	err = &impl.GitError{
		Message: "credentials not found",
		Class:   impl.ErrorClassCallback,
		Code:    impl.ErrorCodeNotFound,
	}

	return
}
