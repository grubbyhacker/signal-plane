package resumeupload

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

const AppID int64 = 4300339
const InstallationID int64 = 146625575
const Repository = "grubbyhacker/resume-builder"
const MaxAssetBytes int64 = 20 * 1024

type GitHubClient struct {
	Client        *http.Client
	APIBase       string
	PrivateKeyPEM []byte
	Now           func() time.Time
}
type release struct {
	ID          int64   `json:"id"`
	Tag         string  `json:"tag_name"`
	Target      string  `json:"target_commitish"`
	Draft       bool    `json:"draft"`
	Prerelease  bool    `json:"prerelease"`
	PublishedAt string  `json:"published_at"`
	Assets      []asset `json:"assets"`
}
type asset struct {
	ID          int64  `json:"id"`
	Size        int64  `json:"size"`
	Name        string `json:"name"`
	ContentType string `json:"content_type"`
	Digest      string `json:"digest"`
}

func (client *GitHubClient) Hydrate(ctx context.Context, repositoryID, installationID, releaseID int64) (workledger.ReleaseOperation, error) {
	if repositoryID <= 0 || installationID != InstallationID || releaseID <= 0 {
		return workledger.ReleaseOperation{}, errors.New("webhook repository, installation, or release identity is invalid")
	}
	token, err := client.installationToken(ctx)
	if err != nil {
		return workledger.ReleaseOperation{}, err
	}
	var repo struct {
		ID       int64  `json:"id"`
		FullName string `json:"full_name"`
	}
	if err := client.getJSON(ctx, token, "/repos/"+Repository, &repo); err != nil {
		return workledger.ReleaseOperation{}, err
	}
	if repo.ID != repositoryID || repo.FullName != Repository {
		return workledger.ReleaseOperation{}, errors.New("repository identity does not match the fixed source")
	}
	var rel release
	if err := client.getJSON(ctx, token, "/repos/"+Repository+"/releases/"+fmt.Sprint(releaseID), &rel); err != nil {
		return workledger.ReleaseOperation{}, err
	}
	if rel.ID != releaseID || rel.Draft || rel.Prerelease || rel.PublishedAt == "" {
		return workledger.ReleaseOperation{}, errors.New("release is not an exact published production release")
	}
	commit, err := client.resolveCommit(ctx, token, rel.Tag)
	if err != nil {
		return workledger.ReleaseOperation{}, err
	}
	if rel.Target != commit {
		return workledger.ReleaseOperation{}, errors.New("release target_commitish is not the resolved full commit")
	}
	date, err := validateTag(rel.Tag, commit)
	if err != nil {
		return workledger.ReleaseOperation{}, err
	}
	var selected *asset
	pattern := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*_` + date + `\.structured\.md$`)
	for i := range rel.Assets {
		a := &rel.Assets[i]
		if pattern.MatchString(a.Name) && a.ContentType == "text/markdown" && a.Size > 0 && a.Size <= MaxAssetBytes {
			if selected != nil {
				return workledger.ReleaseOperation{}, errors.New("release contains ambiguous structured Markdown assets")
			}
			selected = a
		}
	}
	if selected == nil {
		return workledger.ReleaseOperation{}, errors.New("release contains no canonical structured Markdown asset")
	}
	if !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(selected.Digest) {
		return workledger.ReleaseOperation{}, errors.New("asset lacks a provider sha256 digest")
	}
	body, err := client.download(ctx, token, selected.ID)
	if err != nil {
		return workledger.ReleaseOperation{}, err
	}
	if int64(len(body)) != selected.Size || !utf8.Valid(body) || len(body) > int(MaxAssetBytes) {
		return workledger.ReleaseOperation{}, errors.New("downloaded asset size or text content is invalid")
	}
	digest := sha256.Sum256(body)
	computed := "sha256:" + hex.EncodeToString(digest[:])
	if computed != selected.Digest {
		return workledger.ReleaseOperation{}, errors.New("provider and computed asset digests differ")
	}
	op := workledger.ReleaseOperation{Repository: Repository, RepositoryID: repositoryID, InstallationID: installationID, ReleaseID: releaseID, Tag: rel.Tag, PublishedAt: rel.PublishedAt, TargetCommitish: rel.Target, CommitSHA: commit, AssetID: selected.ID, AssetName: selected.Name, AssetSize: selected.Size, AssetContentType: selected.ContentType, ProviderDigest: selected.Digest, ComputedDigest: computed}
	return op, op.Validate()
}

func (client *GitHubClient) DownloadVerified(ctx context.Context, operation workledger.ReleaseOperation) ([]byte, error) {
	if err := operation.Validate(); err != nil {
		return nil, err
	}
	current, err := client.Hydrate(ctx, operation.RepositoryID, operation.InstallationID, operation.ReleaseID)
	if err != nil {
		return nil, err
	}
	if current != operation {
		return nil, errors.New("release provenance changed after admission")
	}
	token, err := client.installationToken(ctx)
	if err != nil {
		return nil, err
	}
	body, err := client.download(ctx, token, operation.AssetID)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(body)
	if int64(len(body)) != operation.AssetSize || "sha256:"+hex.EncodeToString(digest[:]) != operation.ComputedDigest {
		return nil, errors.New("release asset changed after admission")
	}
	return body, nil
}

func validateTag(tag, commit string) (string, error) {
	m := regexp.MustCompile(`^v(\d{4})\.(\d{2})\.(\d{2})-([0-9a-f]{7})$`).FindStringSubmatch(tag)
	if m == nil || !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(commit) || m[4] != commit[:7] {
		return "", errors.New("release tag does not identify the resolved full commit")
	}
	return m[1] + m[2] + m[3], nil
}
func (client *GitHubClient) base() string {
	if client.APIBase != "" {
		return strings.TrimSuffix(client.APIBase, "/")
	}
	return "https://api.github.com"
}
func (client *GitHubClient) http() *http.Client {
	result := http.Client{Timeout: 20 * time.Second}
	if client.Client != nil {
		result = *client.Client
	}
	result.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		host := request.URL.Hostname()
		if len(via) > 0 && request.URL.Scheme == via[0].URL.Scheme && request.URL.Host == via[0].URL.Host {
			return nil
		}
		if request.URL.Scheme == "https" && host == "release-assets.githubusercontent.com" {
			if len(via) > 0 && via[0].URL.Hostname() != host {
				request.Header.Del("Authorization")
			}
			return nil
		}
		return errors.New("GitHub redirect left the fixed provider boundary")
	}
	return &result
}
func (client *GitHubClient) getJSON(ctx context.Context, token, path string, out any) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, client.base()+path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := client.http().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("github %s returned %d", path, resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out)
}
func (client *GitHubClient) download(ctx context.Context, token string, id int64) ([]byte, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, client.base()+"/repos/"+Repository+"/releases/assets/"+fmt.Sprint(id), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/octet-stream")
	resp, err := client.http().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("asset download returned %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, MaxAssetBytes+1))
}
func (client *GitHubClient) resolveCommit(ctx context.Context, token, tag string) (string, error) {
	var ref struct{ Object struct{ Type, SHA string } }
	if err := client.getJSON(ctx, token, "/repos/"+Repository+"/git/ref/tags/"+url.PathEscape(tag), &ref); err != nil {
		return "", err
	}
	if ref.Object.Type == "commit" {
		return ref.Object.SHA, nil
	}
	if ref.Object.Type == "tag" {
		var object struct{ Object struct{ Type, SHA string } }
		if err := client.getJSON(ctx, token, "/repos/"+Repository+"/git/tags/"+ref.Object.SHA, &object); err != nil {
			return "", err
		}
		if object.Object.Type == "commit" {
			return object.Object.SHA, nil
		}
	}
	return "", errors.New("release tag does not resolve to a commit")
}
func (client *GitHubClient) installationToken(ctx context.Context) (string, error) {
	jwt, err := client.jwt()
	if err != nil {
		return "", err
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, client.base()+"/app/installations/"+fmt.Sprint(InstallationID)+"/access_tokens", nil)
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := client.http().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("installation token returned %d", resp.StatusCode)
	}
	var value struct{ Token string }
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&value); err != nil || value.Token == "" {
		return "", errors.New("installation token response is invalid")
	}
	return value.Token, nil
}

func (client *GitHubClient) ValidateCredentials() error { _, err := client.jwt(); return err }
func (client *GitHubClient) jwt() (string, error) {
	block, _ := pem.Decode(client.PrivateKeyPEM)
	if block == nil {
		return "", errors.New("GitHub App private key is invalid")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		if legacy, e := x509.ParsePKCS1PrivateKey(block.Bytes); e == nil {
			key = legacy
		} else {
			return "", err
		}
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return "", errors.New("GitHub App private key is not RSA")
	}
	now := time.Now()
	if client.Now != nil {
		now = client.Now()
	}
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	payload, _ := json.Marshal(map[string]any{"iat": now.Add(-time.Minute).Unix(), "exp": now.Add(9 * time.Minute).Unix(), "iss": AppID})
	unsigned := header + "." + enc.EncodeToString(payload)
	sum := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(rand.Reader, rsaKey, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	return unsigned + "." + enc.EncodeToString(sig), nil
}
