CREATE TABLE IF NOT EXISTS `redis_set` (
  `key_hash` VARCHAR(64) NOT NULL COMMENT 'SHA256',
  `key` VARCHAR(1024) NOT NULL,
  `member_hash` VARCHAR(64) NOT NULL COMMENT 'SHA256',
  `member` VARCHAR(1024) NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`key_hash`, `member_hash`),
  UNIQUE INDEX `key_hash_UNIQUE` (`key_hash` ASC) VISIBLE,
  UNIQUE INDEX `member_hash_UNIQUE` (`member_hash` ASC) VISIBLE)
ENGINE = InnoDB /*T![ttl] TTL = `created_at` + INTERVAL 7 DAY TTL_ENABLE = 'ON'*/;
