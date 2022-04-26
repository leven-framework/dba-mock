<?php

namespace Leven\DBA\Mock\Structure;

use Leven\DBA\Common\Exception\DriverException;

class Column
{

    public function __construct(
        public readonly string $name,
        public readonly ColumnType $type = ColumnType::MOCK,
        public readonly int $maxLength = 0,
        public readonly bool $nullable = true,
        public readonly null|string|int|float|bool $default = null,
        public readonly bool $unique = false,
        public readonly bool $autoIncrement = false,
    )
    {
        if($this->autoIncrement && $this->default !== null)
            throw new DriverException("autoIncrement column `$this->name` cannot have a default value");
    }


    public static function fromArray(string $key, array|string $value): self
    {
        return new static(
            name: $key,
            type:  ColumnType::fromName(is_array($value) ? ($value[0] ?? '') : $value),
            maxLength: $value[1] ?? 0,
            nullable: $value[2] ?? true,
            default: $value[3] ?? null,
            unique: $value[4] ?? false,
            autoIncrement: $value[5] ?? false,
        );
    }

    public function toArray(): array
    {
        return [$this->type->name, $this->maxLength, $this->nullable, $this->default, $this->unique, $this->autoIncrement];
    }

    protected function isValidJson(string $json): bool
    {
        json_decode($json);
        return json_last_error() === JSON_ERROR_NONE;
    }

    public function validateValue(mixed $value): void
    {
        if($value === null) {
            if ($this->nullable) return;
            else throw new DriverException("value for column `$this->name` must not be null");
        }

        $type = match($this->type){
            ColumnType::MOCK => is_string($value) || is_bool($value) || is_int($value) || is_float($value),
            ColumnType::INT => is_int($value),
            ColumnType::FLOAT => is_float($value) || is_int($value),
            ColumnType::TEXT => is_string($value) && ($this->maxLength === 0 || strlen($value) <= $this->maxLength),
            ColumnType::JSON => is_string($value) && $this->isValidJson($value),
            default => false,
        };

        if(!$type) throw new DriverException("value for column `$this->name` mismatches column type or max length");
    }

}