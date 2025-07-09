-- +goose Up
-- +goose StatementBegin
CREATE TABLE `messages`
(
    `id`         text,
    `queue_id`   text,
    `data`       text,
    `status`     integer,
    `hash`       text,
    `priority`   integer,
    `lock_until` datetime,
    `sub_queue`  text,
    PRIMARY KEY (`id`)
);
CREATE INDEX `idx_messages_hash` ON `messages` (`hash`);
CREATE INDEX `queue_status_priority_idx` ON `messages` (`status`, `priority` desc);
CREATE INDEX `status_lock_until_idx` ON `messages` (`status`, `lock_until`);
CREATE INDEX `queue_id_idx` ON `messages` (`id`, `sub_queue`);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS `queue_id_idx` ON `messages`;
DROP INDEX IF EXISTS `status_lock_until_idx` ON `messages`;
DROP INDEX IF EXISTS `queue_status_priority_idx` ON `messages`;
DROP INDEX IF EXISTS `idx_messages_hash` ON `messages`;
DROP TABLE IF EXISTS `messages`;
-- +goose StatementEnd
