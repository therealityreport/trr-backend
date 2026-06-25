# Adaptive Instagram Scrape Control Plane

We will centralize Instagram scrape pressure decisions in a shared adaptive control plane while letting each lane enforce the resulting budget in its own way. This avoids duplicated backoff logic across posts, comments, media mirror, and database writes, and it lets TRR pursue account-level backfill speed while backing down when Supabase health, Instagram flag risk, proxy health, retries, active jobs, or write failures show elevated risk.

## Considered Options

- Put speed and backoff policy inside each scraper lane.
- Use one shared control plane that publishes simple lane budgets.

## Consequences

Lane implementations stay responsible for translating budgets into concrete behavior, such as reducing comment pagination, post detail fetches, media downloads, upload concurrency, or database write batch sizes. Benchmarks may adjust budgets during bounded runs, but permanent defaults require evidence that completeness, speed, Supabase health, and Instagram-health gates all pass.
