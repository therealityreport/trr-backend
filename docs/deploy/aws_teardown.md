# AWS Teardown Pass (TRR-Backend)

This runbook is the final cleanup for the legacy AWS API shell after the
Render + Modal cutover.

Do not run it before the documented observation window ends:

- earliest execution time: `2026-03-13T16:09:13-04:00`

This pass intentionally covers only the remaining AWS resources that still
exist after the cutover:

- `trr-api-asg`
- `trr-api-lt`
- `trr-api-alb`
- `trr-api-tg`
- private route table `rtb-04176689ad967a8ae` default route to NAT
- `nat-004581b7931e685e7`
- `eipalloc-0c6c7ef0913e7a3d8`
- `sg-054ae25e1699a3845` (`trr-alb-sg`)
- `sg-09ad087d9a6b689dd` (`trr-api-sg`)
- `trr-jobplane-metrics-publisher`
- `trr-metrics-publisher-role`
- `trr-metadata-db-final-2026-03-07`
- CloudWatch alarms for this migration scope
- old EC2/bootstrap log groups plus the metrics-publisher Lambda and SSM groups
- S3 buckets:
  - `trr-backend`
  - `screenalytics`
  - `ltsr-data-bucket`

Out of scope for this pass:

- ACM certificates
- Better Stack wiring
- Render/Vercel/Modal config changes

## Operator Script

Use:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/ops/aws_teardown_pass.py
```

Default behavior is `check-only`. It will:

1. enforce the observation-window gate
2. validate:
   - Render `/health`
   - one Render show read route
   - one Modal-backed admin route
   - Vercel production root
3. confirm there are no EC2 instances or EBS volumes left
4. print current AWS teardown inventory

To execute the destructive teardown after the gate:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/ops/aws_teardown_pass.py --execute
```

For machine-readable output:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/ops/aws_teardown_pass.py --json
python3.11 scripts/ops/aws_teardown_pass.py --execute --json
```

## Deletion Order

The script deletes resources in this order:

1. `trr-api-asg`
2. `trr-api-lt`
3. `trr-api-alb`
4. `trr-api-tg`
5. private route table `rtb-04176689ad967a8ae` default route to NAT
6. `trr-alb-sg`
7. `trr-api-sg`
8. NAT gateway `nat-004581b7931e685e7`
9. NAT EIP `eipalloc-0c6c7ef0913e7a3d8`
9. CloudWatch alarms:
   - `trr-api-target-5xx`
   - `trr-api-target-5xx-high`
   - `trr-long-job-failures-high`
   - `trr-queue-depth-high`
   - `trr-stale-leases-high`
10. old log groups:
   - `/aws/lambda/trr-jobplane-metrics-publisher`
   - `/aws/ssm/AWS-RunShellScript`
   - `/trr/api/bootstrap`
   - `/trr/ec2/cloud-init`
   - `/trr/ec2/cloud-init-output`
   - `/trr/worker/bootstrap`
11. Lambda `trr-jobplane-metrics-publisher`
12. IAM role `trr-metrics-publisher-role`
13. snapshot `trr-metadata-db-final-2026-03-07`
14. S3 buckets:
   - `trr-backend`
   - `screenalytics`
   - `ltsr-data-bucket`

If either security group still has dependencies after ALB/TG removal, the
script stops instead of forcing deletion.

## Post-Run Expectations

After a successful `--execute` run:

- no `trr-api-*` AWS runtime shell remains
- no private route still points at the retired NAT
- no NAT gateway or NAT EIP remains
- no ALB-managed public IPv4 remains for the retired stack
- no metrics-publisher Lambda or IAM role remains
- the manual RDS snapshot is gone
- the three retired S3 buckets are gone
- Render health still passes
- the Render Modal-backed admin route still passes
- Vercel production still passes
- no CloudWatch alarms remain for this migration scope
