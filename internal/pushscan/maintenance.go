package pushscan

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Maintenance struct {
	Scanner                  *Scanner
	ReconcileInterval        time.Duration
	FingerprintPruneInterval time.Duration
	Clock                    func() time.Time
	nextReconcile            time.Time
	nextFingerprintPrune     time.Time
}

func (maintenance *Maintenance) Startup(ctx context.Context) error {
	if err := maintenance.validate(); err != nil {
		return err
	}
	now := maintenance.now()
	maintenance.nextReconcile = now.Add(maintenance.ReconcileInterval)
	maintenance.nextFingerprintPrune = now.Add(maintenance.FingerprintPruneInterval)
	var startupErrors []error
	if err := maintenance.Scanner.Store.PruneFingerprints(ctx, now); err != nil {
		startupErrors = append(startupErrors, fmt.Errorf("startup fingerprint prune: %w", err))
	}
	if err := maintenance.Scanner.Reconcile(ctx); err != nil {
		startupErrors = append(startupErrors, fmt.Errorf("startup reconciliation: %w", err))
	}
	return errors.Join(startupErrors...)
}

func (maintenance *Maintenance) RunDue(ctx context.Context) error {
	if err := maintenance.validate(); err != nil {
		return err
	}
	if maintenance.nextReconcile.IsZero() || maintenance.nextFingerprintPrune.IsZero() {
		return errors.New("push scanner maintenance has not started")
	}
	now := maintenance.now()
	var maintenanceErrors []error
	if !now.Before(maintenance.nextFingerprintPrune) {
		maintenance.nextFingerprintPrune = now.Add(maintenance.FingerprintPruneInterval)
		if err := maintenance.Scanner.Store.PruneFingerprints(ctx, now); err != nil {
			maintenanceErrors = append(maintenanceErrors, fmt.Errorf("periodic fingerprint prune: %w", err))
		}
	}
	if !now.Before(maintenance.nextReconcile) {
		maintenance.nextReconcile = now.Add(maintenance.ReconcileInterval)
		if err := maintenance.Scanner.Reconcile(ctx); err != nil {
			maintenanceErrors = append(maintenanceErrors, fmt.Errorf("periodic reconciliation: %w", err))
		}
	}
	return errors.Join(maintenanceErrors...)
}

func (maintenance *Maintenance) validate() error {
	if maintenance.Scanner == nil || maintenance.Scanner.Store == nil || maintenance.ReconcileInterval <= 0 || maintenance.FingerprintPruneInterval <= 0 {
		return errors.New("push scanner maintenance configuration is incomplete")
	}
	return nil
}

func (maintenance *Maintenance) now() time.Time {
	if maintenance.Clock != nil {
		return maintenance.Clock().UTC()
	}
	return time.Now().UTC()
}
