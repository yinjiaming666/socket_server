package deployctl

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

var errArtifactManifestNotFound = errors.New("artifact manifest not found")

// ArtifactManifest 描述一次不可变构建制品的来源和运行平台。
type ArtifactManifest struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuiltAt   string `json:"built_at"`
	GoVersion string `json:"go_version"`
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
}

// releaseIDFromConfig 生成可安全用于远程目录名的发布编号。
func releaseIDFromConfig(cfg Config) string {
	if value := sanitizeReleaseID(cfg.ReleaseVersion); value != "" {
		return value
	}
	timestamp := cfg.Now().UTC().Format("20060102T150405Z")
	commit := sanitizeReleaseID(cfg.GitCommit)
	if len(commit) > 12 {
		commit = commit[:12]
	}
	if commit == "" {
		return timestamp
	}
	return timestamp + "-" + commit
}

// sanitizeReleaseID 仅保留目录名和 shell 参数中安全的字符。
func sanitizeReleaseID(value string) string {
	value = strings.TrimSpace(value)
	var builder strings.Builder
	for _, current := range value {
		switch {
		case current >= 'a' && current <= 'z':
			builder.WriteRune(current)
		case current >= 'A' && current <= 'Z':
			builder.WriteRune(current)
		case current >= '0' && current <= '9':
			builder.WriteRune(current)
		case current == '.', current == '-', current == '_':
			builder.WriteRune(current)
		}
	}
	return strings.Trim(builder.String(), ".-_")
}

// writeArtifactManifest 将构建元数据写入制品根目录。
func (d *Deployer) writeArtifactManifest(packageDir string) error {
	manifest := ArtifactManifest{
		Version:   d.releaseID,
		Commit:    strings.TrimSpace(d.cfg.GitCommit),
		BuiltAt:   d.cfg.Now().UTC().Format(time.RFC3339),
		GoVersion: runtime.Version(),
		GOOS:      envOrDefault("GOOS", "linux"),
		GOARCH:    envOrDefault("GOARCH", "amd64"),
	}
	body, err := json.MarshalIndent(&manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal artifact manifest: %w", err)
	}
	body = append(body, byte(10))
	path := filepath.Join(packageDir, "manifest.json")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return fmt.Errorf("write artifact manifest: %w", err)
	}
	return nil
}

// readArtifactManifest 从归档中读取 manifest.json，不把整个发布包加载到内存。
func readArtifactManifest(packagePath string) (*ArtifactManifest, error) {
	file, err := os.Open(packagePath)
	if err != nil {
		return nil, fmt.Errorf("open artifact manifest: %w", err)
	}
	defer file.Close()

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("open artifact gzip: %w", err)
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return nil, errArtifactManifestNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("read artifact tar: %w", err)
		}
		name := strings.TrimPrefix(filepath.ToSlash(filepath.Clean(header.Name)), "./")
		if name != "manifest.json" || !header.FileInfo().Mode().IsRegular() {
			continue
		}
		var manifest ArtifactManifest
		decoder := json.NewDecoder(io.LimitReader(tarReader, 1<<20))
		if err := decoder.Decode(&manifest); err != nil {
			return nil, fmt.Errorf("decode artifact manifest: %w", err)
		}
		return &manifest, nil
	}
}

// ensurePackageChecksum 校验已有摘要，缺失时为兼容 SKIP_BUILD 制品补充摘要文件。
func ensurePackageChecksum(packagePath string) error {
	checksumPath := packagePath + ".sha256"
	if _, err := os.Stat(checksumPath); err == nil {
		return verifyPackageChecksum(packagePath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check package checksum: %w", err)
	}
	return writePackageChecksum(packagePath)
}

// writePackageChecksum 计算归档 SHA256，并使用标准 sha256sum 文本格式保存。
func writePackageChecksum(packagePath string) error {
	sum, err := calculateFileSHA256(packagePath)
	if err != nil {
		return err
	}
	line := fmt.Sprintf("%s  %s%c", sum, filepath.Base(packagePath), byte(10))
	if err := os.WriteFile(packagePath+".sha256", []byte(line), 0o644); err != nil {
		return fmt.Errorf("write package checksum: %w", err)
	}
	return nil
}

// verifyPackageChecksum 在上传前检查本地制品是否被修改。
func verifyPackageChecksum(packagePath string) error {
	body, err := os.ReadFile(packagePath + ".sha256")
	if err != nil {
		return fmt.Errorf("read package checksum: %w", err)
	}
	fields := strings.Fields(string(body))
	if len(fields) == 0 {
		return errors.New("package checksum is empty")
	}
	actual, err := calculateFileSHA256(packagePath)
	if err != nil {
		return err
	}
	if !strings.EqualFold(fields[0], actual) {
		return fmt.Errorf("package checksum mismatch: expected %s actual %s", fields[0], actual)
	}
	return nil
}

// calculateFileSHA256 流式计算文件摘要，避免把大型发布包整体读入内存。
func calculateFileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open package for checksum: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("calculate package checksum: %w", err)
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
