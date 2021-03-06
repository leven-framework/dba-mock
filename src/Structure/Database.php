<?php

namespace Leven\DBA\Mock\Structure;

use Leven\DBA\Common\Exception\DriverException;

class Database
{

    /** @var Table[] $tables */
    protected array $tables = [];


    public function getTable(string $tableName): Table
    {
        return $this->tables[$tableName]
            ?? throw new DriverException("table `$tableName` does not exist");
    }

    public function addTable(Table $table): void
    {
        $this->tables[$table->name] = $table;
    }


    public static function fromArray(array $array): static
    {
        $instance = new static;

        foreach ($array as $tableName => $tableContent)
            $instance->addTable(Table::fromArray($tableName, $tableContent));

        return $instance;
    }

    public function toArray(): array
    {
        foreach ($this->tables as $table)
            $output[$table->name] = $table->toArray();

        return $output ?? [];
    }

}