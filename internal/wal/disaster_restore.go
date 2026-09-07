package wal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// InspectRestoreAuthority verifies one authority without consulting its peer.
// This is deliberately separate from Scrub: a disaster drill must still work
// when the other provider is completely unavailable.
func InspectRestoreAuthority(primaryLocation, repoID, sourceName string) (Manifest, AuthorityScrubReport, error) {
	named, err := openRestoreAuthority(primaryLocation, sourceName)
	if err != nil {
		return Manifest{}, AuthorityScrubReport{Name: sourceName}, err
	}
	report, snapshot := inspectAuthority(repoID, named)
	if !report.Healthy || snapshot == nil {
		return Manifest{}, report, fmt.Errorf("%s authority is not fully verified", sourceName)
	}
	if snapshot.floor != 0 {
		return Manifest{}, report, fmt.Errorf("%s authority starts at legacy certificate floor %d and cannot rebuild an empty repository", sourceName, snapshot.floor)
	}
	return cloneManifest(snapshot.manifest), report, nil
}

// ReplayRestoreAuthority restores every object and transaction from exactly one
// fully verified authority. The caller owns reference application so Git can
// retain its normal atomic update-ref semantics.
func ReplayRestoreAuthority(primaryLocation, repoID, sourceName, gitObjects string, apply func(ManifestEntry, EntryMeta) error) (Manifest, AuthorityScrubReport, error) {
	if strings.TrimSpace(gitObjects) == "" {
		return Manifest{}, AuthorityScrubReport{Name: sourceName}, errors.New("Git object directory is required")
	}
	named, err := openRestoreAuthority(primaryLocation, sourceName)
	if err != nil {
		return Manifest{}, AuthorityScrubReport{Name: sourceName}, err
	}
	report, snapshot := inspectAuthority(repoID, named)
	if !report.Healthy || snapshot == nil {
		return Manifest{}, report, fmt.Errorf("%s authority is not fully verified", sourceName)
	}
	if snapshot.floor != 0 {
		return Manifest{}, report, fmt.Errorf("%s authority starts at legacy certificate floor %d and cannot rebuild an empty repository", sourceName, snapshot.floor)
	}
	for _, object := range snapshot.certificates {
		var certificate CommitCertificate
		if err := json.Unmarshal(object.Data, &certificate); err != nil {
			return Manifest{}, report, fmt.Errorf("decode verified certificate %s: %w", object.SHA256, err)
		}
		for _, certified := range certificate.Entries {
			meta, err := replayEntryDescriptor(certified.Entry, gitObjects, named.authority)
			if err != nil {
				return Manifest{}, report, fmt.Errorf("replay generation %d: %w", certified.Entry.Generation, err)
			}
			if !equalUpdates(meta.Updates, certified.Updates) {
				return Manifest{}, report, fmt.Errorf("replay generation %d descriptor changed after verification", certified.Entry.Generation)
			}
			if apply != nil {
				if err := apply(certified.Entry, meta); err != nil {
					return Manifest{}, report, err
				}
			}
		}
	}
	return cloneManifest(snapshot.manifest), report, nil
}

func openRestoreAuthority(primaryLocation, sourceName string) (namedMaintenanceAuthority, error) {
	if sourceName != "primary" && sourceName != "secondary" {
		return namedMaintenanceAuthority{}, errors.New("restore source must be primary or secondary")
	}
	location := strings.TrimSpace(primaryLocation)
	secondary := sourceName == "secondary"
	if secondary {
		location = strings.TrimSpace(os.Getenv("WALGIT_BLOB_SECONDARY_STORE"))
		if location == "" {
			return namedMaintenanceAuthority{}, errors.New("secondary restore requires WALGIT_BLOB_SECONDARY_STORE")
		}
	}
	if location == "" {
		return namedMaintenanceAuthority{}, errors.New("primary storage location is required")
	}
	identity, err := normalizeBlobLocation(location)
	if err != nil {
		return namedMaintenanceAuthority{}, err
	}
	authority, err := openAuthority(location, secondary)
	if err != nil {
		return namedMaintenanceAuthority{}, fmt.Errorf("open %s restore authority: %w", sourceName, err)
	}
	maintenance, ok := authority.(maintenanceAuthority)
	if !ok {
		return namedMaintenanceAuthority{}, fmt.Errorf("%s authority does not support verified restore", sourceName)
	}
	return namedMaintenanceAuthority{name: sourceName, location: identity, authority: maintenance}, nil
}
