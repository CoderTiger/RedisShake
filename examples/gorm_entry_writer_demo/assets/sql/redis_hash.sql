CREATE TABLE IF NOT EXISTS `redis_hash` (
  `key_hash` VARCHAR(64) NOT NULL COMMENT 'SHA256',
  `key` TEXT NOT NULL,
  `field_hash` VARCHAR(64) NOT NULL COMMENT 'SHA256',
  `field` VARCHAR(767) NOT NULL,
  `value` TEXT NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`key_hash`, `field_hash`),
  UNIQUE INDEX `key_hash_UNIQUE` (`key_hash` ASC) VISIBLE,
  UNIQUE INDEX `field_hash_UNIQUE` (`field_hash` ASC) VISIBLE) /*T![ttl] TTL = `created_at` + INTERVAL 7 DAY TTL_ENABLE = 'ON'*/;
