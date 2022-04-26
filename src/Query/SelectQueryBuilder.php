<?php

namespace Leven\DBA\Mock\Query;

use Leven\DBA\Common\SelectQueryInterface;
use Leven\DBA\Mock\Structure\Table;
use Leven\DBA\Mock\Query;
use Leven\DBA\Mock\Query\Filter\{ColumnFilterTrait, LimitFilterTrait, OrderFilterTrait, WhereFilterTrait};

class SelectQueryBuilder extends BaseQueryBuilder implements SelectQueryInterface
{

    use WhereFilterTrait;
    use LimitFilterTrait;
    use OrderFilterTrait;
    use ColumnFilterTrait;

    final protected function formatAssocColumns(Table $table): array
    {
        foreach($table->getRows() as $row)
            $output[] = array_combine($table->getColumnNames(), $row);

        return $output ?? [];
    }

    public function getQuery(): Query
    {
        $output = $this->pipe(
            $this->getTableCopy(),
            $this->filterWhere(...),
            $this->filterOrder(...),
            $this->filterLimit(...),
            $this->filterColumn(...),
            $this->formatAssocColumns(...),
        );

        return new Query(count($output), $output);
    }

}