Thredder v1.0 completes the Java concurrency learning and portfolio project.

- Fixed worker pool, bounded queue, CLI and job lifecycle tracking.
- Atomic claims, ordered resource locking and priority aging.
- Timed admission, concurrent-submitter limits and deadline-based cooperative shutdown.
- Durable job recovery and a separate idempotent receiver demonstrating safe same-key retries.
- Reproducible benchmarks: 32 configurations, 96 measured trials, retained raw data.
- README, roadmap, changelog and interview walkthrough.

Verification: full concurrency/recovery/idempotency test suite, clean-source build and CLI checks, and benchmark data validation passed on macOS with OpenJDK 27. Sources target Java 17 APIs; execution on Java 17 and Windows is not verified.

This is a local learning/portfolio implementation, not a production-service or exactly-once execution guarantee. See the README for persistence, retention and performance limitations.

Assets: source ZIP and SHA-256 checksum. MIT licensed.
