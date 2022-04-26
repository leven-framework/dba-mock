<?php

namespace Leven\DBA\Mock\Query;

use Leven\DBA\Common\UpdateQueryInterface;
use Leven\DBA\Mock\MockAdapter;
use Leven\DBA\Mock\Query;
use Leven\DBA\Mock\Query\Filter\{LimitFilterTrait, OrderFilterTrait, SetFilterTrait, WhereFilterTrait};

class UpdateQueryBuilder extends BaseQueryBuilder implements UpdateQueryInterface
{

    use SetFilterTrait;
    use WhereFilterTrait;
    use LimitFilterTrait;
    use OrderFilterTrait;

    public function getQuery(): Query
    {
        $indices = $this->pipe(
            $this->getTableCopy(),
            $this->filterWhere(...),
            $this->filterOrder(...),
            $this->filterLimit(...),
            $this->getRowIndices(...),
        );

        $update = function(MockAdapter $adapter) use ($indices) {
            $table = $adapter->getDatabase()->getTable($this->table);

            $row = $this->formatSetDataToRow($table);
            foreach($indices as $index) $table->mergeIntoRow($index, $row);

            $adapter->save();
        };

        return new Query(count($indices), [], $update);
    }

}