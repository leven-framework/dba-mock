<?php

namespace Leven\DBA\Mock\Query\Filter;

use Leven\DBA\Common\BuilderPart\SetTrait;
use Leven\DBA\Mock\Structure\Table;

trait SetFilterTrait
{

    use SetTrait;

    protected function formatSetDataToRow(Table $table): array
    {
        foreach($this->data as $column => $value)
            $output[$table->getColumnIndex($column)] = $value;

        return $output ?? [];
    }

}