<?php

namespace Leven\DBA\Mock\Structure;

enum ColumnType
{
    case MOCK;
    case INT;
    case FLOAT;
    case TEXT;
    case JSON;

    public static function fromName(string $name): self
    {
        return match(strtolower($name)){
            'int', 'tinyint', 'smallint', 'mediumint', 'bigint', 'bool', => self::INT,
            'char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext', => self::TEXT,
            'float', 'double', => self::FLOAT,
            'json' => self::JSON,
            default => self::MOCK,
        };
    }
}