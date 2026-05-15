CREATE TABLE IF NOT EXISTS `pubmed` (
  `id` INT NOT NULL,
  `title` TEXT NOT NULL,
  `journal` TEXT NULL,
  `date` VARCHAR(10) NULL,
  `pub_year` SMALLINT NULL,
  `authors` TEXT NULL,
  `abstract` MEDIUMTEXT NULL,
  `pmc_id` VARCHAR(16) NULL,
  `doi` VARCHAR(255) NULL,
  `publication_status` VARCHAR(64) NULL,
  `publication_type` TEXT NULL,
  `language` VARCHAR(32) NULL,
  `fetch_date` DATETIME NULL,
  `source_file` VARCHAR(255) NULL,
  PRIMARY KEY (`id`),
  KEY `pubmed_year_idx` (`pub_year`, `id`),
  KEY `pubmed_date_idx` (`date`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `mesh_descriptor` (
  `ui` VARCHAR(16) NOT NULL,
  `name` VARCHAR(1024) NOT NULL,
  PRIMARY KEY (`ui`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `mesh_qualifier` (
  `ui` VARCHAR(16) NOT NULL,
  `name` VARCHAR(1024) NOT NULL,
  PRIMARY KEY (`ui`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `pubmed_mesh` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `pmid` INT NOT NULL,
  `descriptor_ui` VARCHAR(16) NOT NULL,
  `qualifier_ui` VARCHAR(16) NULL,
  `descriptor_major_topic` BOOLEAN NOT NULL,
  `qualifier_major_topic` BOOLEAN NULL,
  PRIMARY KEY (`id`),
  KEY `pubmed_mesh_pmid_idx` (`pmid`),
  KEY `pubmed_mesh_descriptor_idx` (`descriptor_ui`, `pmid`),
  CONSTRAINT `pubmed_mesh_pmid_fk`
    FOREIGN KEY (`pmid`) REFERENCES `pubmed` (`id`) ON DELETE CASCADE,
  CONSTRAINT `pubmed_mesh_descriptor_fk`
    FOREIGN KEY (`descriptor_ui`) REFERENCES `mesh_descriptor` (`ui`),
  CONSTRAINT `pubmed_mesh_qualifier_fk`
    FOREIGN KEY (`qualifier_ui`) REFERENCES `mesh_qualifier` (`ui`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `pubmed_mirror_file_state` (
  `archive_name` VARCHAR(255) NOT NULL,
  `archive_group` VARCHAR(32) NOT NULL,
  `remote_last_modified` DATETIME NULL,
  `md5` VARCHAR(64) NULL,
  `downloaded_at` DATETIME NULL,
  `processed_at` DATETIME NULL,
  `status` VARCHAR(32) NOT NULL,
  `error_message` TEXT NULL,
  PRIMARY KEY (`archive_name`),
  KEY `pubmed_mirror_group_status_idx` (`archive_group`, `status`, `processed_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
