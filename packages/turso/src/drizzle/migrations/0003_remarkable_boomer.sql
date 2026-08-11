CREATE TABLE `workflow_streams` (
	`run_id` text NOT NULL,
	`stream_name` text NOT NULL,
	`tail_index` integer DEFAULT -1 NOT NULL,
	`done` integer DEFAULT false NOT NULL,
	`created_at` text NOT NULL,
	`updated_at` text NOT NULL,
	PRIMARY KEY(`run_id`, `stream_name`)
);
--> statement-breakpoint
CREATE INDEX `idx_workflow_streams_run` ON `workflow_streams` (`run_id`,`stream_name`);--> statement-breakpoint
CREATE TABLE `workflow_waits` (
	`wait_id` text PRIMARY KEY NOT NULL,
	`run_id` text NOT NULL,
	`status` text DEFAULT 'waiting' NOT NULL,
	`resume_at` text,
	`completed_at` text,
	`created_at` text NOT NULL,
	`updated_at` text NOT NULL,
	`spec_version` integer
);
--> statement-breakpoint
CREATE INDEX `idx_waits_run` ON `workflow_waits` (`run_id`,`wait_id`);--> statement-breakpoint
CREATE TABLE `workflow_stream_chunks` (
	`run_id` text NOT NULL,
	`stream_name` text NOT NULL,
	`chunk_index` integer NOT NULL,
	`data` blob NOT NULL,
	`created_at` text NOT NULL,
	PRIMARY KEY(`run_id`, `stream_name`, `chunk_index`)
);
--> statement-breakpoint
CREATE INDEX `idx_workflow_stream_chunks` ON `workflow_stream_chunks` (`run_id`,`stream_name`,`chunk_index`);--> statement-breakpoint
ALTER TABLE `workflow_events` ADD `occurred_at` text;--> statement-breakpoint
ALTER TABLE `workflow_events` ADD `resume_id` text;--> statement-breakpoint
ALTER TABLE `workflow_hooks` ADD `is_webhook` integer;--> statement-breakpoint
ALTER TABLE `workflow_hooks` ADD `is_system` integer;--> statement-breakpoint
ALTER TABLE `workflow_hooks` ADD `token_retention_until` text;--> statement-breakpoint
ALTER TABLE `workflow_hooks` ADD `resume_context` blob;--> statement-breakpoint
ALTER TABLE `queue_messages` ADD `headers` blob;--> statement-breakpoint
ALTER TABLE `queue_messages` ADD `lease_until` text;--> statement-breakpoint
ALTER TABLE `queue_messages` ADD `updated_at` text;--> statement-breakpoint
ALTER TABLE `workflow_runs` ADD `error_code` text;--> statement-breakpoint
ALTER TABLE `workflow_runs` ADD `attributes` blob;--> statement-breakpoint
ALTER TABLE `workflow_runs` ADD `encryption_public_key` text;--> statement-breakpoint
ALTER TABLE `workflow_runs` ADD `expired_at` text;--> statement-breakpoint
CREATE INDEX `idx_events_run_correlation` ON `workflow_events` (`run_id`,`correlation_id`,`event_id`);--> statement-breakpoint
CREATE UNIQUE INDEX `idx_events_child_entity_unique`
ON `workflow_events` (`run_id`,`type`,`correlation_id`)
WHERE `type` IN ('step_created', 'hook_created', 'wait_created');--> statement-breakpoint
CREATE UNIQUE INDEX `idx_events_resume_id_unique`
ON `workflow_events` (`run_id`,`resume_id`)
WHERE `resume_id` IS NOT NULL;--> statement-breakpoint
CREATE UNIQUE INDEX `idx_queue_active_idempotency_key`
ON `queue_messages` (`idempotency_key`)
WHERE `idempotency_key` IS NOT NULL
  AND `status` IN ('pending', 'processing');--> statement-breakpoint
UPDATE `queue_messages`
SET `updated_at` = `created_at`
WHERE `updated_at` IS NULL;--> statement-breakpoint
INSERT OR IGNORE INTO `workflow_streams`
  (`run_id`, `stream_name`, `tail_index`, `done`, `created_at`, `updated_at`)
SELECT
  sr.`run_id`,
  sr.`stream_name`,
  COUNT(CASE WHEN sc.`chunk_id` IS NOT NULL AND COALESCE(sc.`is_eof`, 0) = 0 THEN 1 END) - 1,
  MAX(CASE WHEN COALESCE(sc.`is_eof`, 0) = 1 THEN 1 ELSE 0 END),
  MIN(COALESCE(sc.`created_at`, sr.`created_at`)),
  MAX(COALESCE(sc.`created_at`, sr.`created_at`))
FROM `stream_runs` sr
LEFT JOIN `stream_chunks` sc ON sc.`stream_name` = sr.`stream_name`
GROUP BY sr.`run_id`, sr.`stream_name`;--> statement-breakpoint
INSERT OR IGNORE INTO `workflow_stream_chunks`
  (`run_id`, `stream_name`, `chunk_index`, `data`, `created_at`)
SELECT
  sr.`run_id`,
  sc.`stream_name`,
  ROW_NUMBER() OVER (
    PARTITION BY sr.`run_id`, sc.`stream_name`
    ORDER BY sc.`chunk_id`
  ) - 1,
  sc.`data`,
  sc.`created_at`
FROM `stream_runs` sr
JOIN `stream_chunks` sc ON sc.`stream_name` = sr.`stream_name`
WHERE COALESCE(sc.`is_eof`, 0) = 0;
