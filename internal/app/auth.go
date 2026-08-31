package app

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

type AuthorizationPolicy struct {
	Version      int                     `json:"version"`
	Repositories map[string][]TokenGrant `json:"repositories"`
}

type TokenGrant struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Read   bool   `json:"read"`
	Write  bool   `json:"write"`
}

type authorizer struct {
	sharedToken []byte
	grants      []compiledGrant
}

type compiledGrant struct {
	digest [sha256.Size]byte
	read   bool
	write  bool
}

func loadAuthorizer(repositoryID, sharedToken, policyPath string) (*authorizer, error) {
	a := &authorizer{sharedToken: []byte(sharedToken)}
	if policyPath == "" {
		if sharedToken == "" {
			return nil, errors.New("HTTP server requires WALGIT_HTTP_TOKEN or -auth-file")
		}
		return a, nil
	}
	f, err := os.Open(policyPath)
	if err != nil {
		return nil, fmt.Errorf("open authorization policy: %w", err)
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	var policy AuthorizationPolicy
	if err := decoder.Decode(&policy); err != nil {
		return nil, fmt.Errorf("decode authorization policy: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("authorization policy contains trailing JSON values")
		}
		return nil, fmt.Errorf("decode authorization policy trailing data: %w", err)
	}
	if policy.Version != 1 {
		return nil, fmt.Errorf("unsupported authorization policy version %d", policy.Version)
	}
	grants, ok := policy.Repositories[repositoryID]
	if !ok {
		return nil, fmt.Errorf("authorization policy has no entry for repository %q", repositoryID)
	}
	for i, grant := range grants {
		raw, err := hex.DecodeString(grant.SHA256)
		if err != nil || len(raw) != sha256.Size {
			return nil, fmt.Errorf("authorization grant %d (%q) has an invalid SHA-256 digest", i, grant.Name)
		}
		if !grant.Read && !grant.Write {
			return nil, fmt.Errorf("authorization grant %d (%q) grants no permissions", i, grant.Name)
		}
		var digest [sha256.Size]byte
		copy(digest[:], raw)
		a.grants = append(a.grants, compiledGrant{digest: digest, read: grant.Read, write: grant.Write})
	}
	if len(a.grants) == 0 && sharedToken == "" {
		return nil, fmt.Errorf("authorization policy has no grants for repository %q", repositoryID)
	}
	return a, nil
}

func (a *authorizer) authorize(candidate string, write bool) bool {
	if candidate == "" {
		return false
	}
	sharedMatch := 0
	if len(a.sharedToken) > 0 && len(candidate) == len(a.sharedToken) {
		sharedMatch = subtle.ConstantTimeCompare([]byte(candidate), a.sharedToken)
	}
	digest := sha256.Sum256([]byte(candidate))
	grantMatch := 0
	for _, grant := range a.grants {
		permitted := grant.read
		if write {
			permitted = grant.write
		}
		if permitted {
			grantMatch |= subtle.ConstantTimeCompare(digest[:], grant.digest[:])
		}
	}
	return sharedMatch|grantMatch == 1
}

func requestToken(rAuthorization string, basicPassword string, hasBasic bool) string {
	if strings.HasPrefix(rAuthorization, "Bearer ") {
		return strings.TrimPrefix(rAuthorization, "Bearer ")
	}
	if hasBasic {
		return basicPassword
	}
	return ""
}

func HashToken(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}
