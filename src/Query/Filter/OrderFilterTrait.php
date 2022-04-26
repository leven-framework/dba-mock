<?php

namespace Leven\DBA\Mock\Query\Filter;

use Leven\DBA\Common\BuilderPart\OrderTrait;
use Leven\DBA\Mock\Structure\Table;

trait OrderFilterTrait
{

    use OrderTrait;

    protected function filterOrder(Table $table): Table
    {
        if(empty($this->order)) return $table;

        $columnNames = $table->getColumnNames();

        $table->uasortRows(function($a, $b) use ($columnNames) {
            foreach ($this->order as $column => $direction) {
                $index = array_search($column, $columnNames);
                $left[] = $direction === 'ASC' ? $a[$index] : $b[$index];
                $right[] = $direction === 'ASC' ? $b[$index] : $a[$index];
            }

            return ($left ?? []) <=> ($right ?? []);
        });

        return $table;
    }

}