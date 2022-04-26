<?php

namespace Leven\DBA\Mock;

use Closure;

class Query
{

    public function __construct(
        public readonly int $count,
        public readonly array $rows,
        public readonly ?Closure $update = null,
        public readonly ?string $autoIncrementFromTable = null,
    )
    {
    }

}