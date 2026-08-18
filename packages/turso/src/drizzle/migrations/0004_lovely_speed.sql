CREATE TABLE `workflow_event_slots` (
	`run_id` text PRIMARY KEY NOT NULL
);
--> statement-breakpoint
PRAGMA foreign_keys=OFF;--> statement-breakpoint
CREATE TABLE `__new_workflow_events` (
	`event_id` text NOT NULL,
	`run_id` text NOT NULL,
	`step_id` text,
	`type` text NOT NULL,
	`correlation_id` text,
	`payload` blob,
	`created_at` text NOT NULL,
	`occurred_at` text,
	`resume_id` text,
	PRIMARY KEY(`run_id`, `event_id`)
);
--> statement-breakpoint
INSERT INTO `__new_workflow_events`("event_id", "run_id", "step_id", "type", "correlation_id", "payload", "created_at", "occurred_at", "resume_id") SELECT "event_id", "run_id", "step_id", "type", "correlation_id", "payload", "created_at", "occurred_at", "resume_id" FROM `workflow_events`;--> statement-breakpoint
DROP TABLE `workflow_events`;--> statement-breakpoint
ALTER TABLE `__new_workflow_events` RENAME TO `workflow_events`;--> statement-breakpoint
PRAGMA foreign_keys=ON;--> statement-breakpoint
CREATE INDEX `idx_events_run` ON `workflow_events` (`run_id`,`event_id`);--> statement-breakpoint
CREATE INDEX `idx_events_correlation` ON `workflow_events` (`correlation_id`,`event_id`);--> statement-breakpoint
CREATE INDEX `idx_events_run_correlation` ON `workflow_events` (`run_id`,`correlation_id`,`event_id`);--> statement-breakpoint
CREATE UNIQUE INDEX `idx_events_child_entity_unique` ON `workflow_events` (`run_id`,`type`,`correlation_id`) WHERE `type` IN ('step_created', 'hook_created', 'wait_created');--> statement-breakpoint
CREATE UNIQUE INDEX `idx_events_resume_id_unique` ON `workflow_events` (`run_id`,`resume_id`) WHERE `resume_id` IS NOT NULL;
