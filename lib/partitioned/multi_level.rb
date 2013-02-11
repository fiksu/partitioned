module Partitioned
  #
  # Table partitioning by a referenced id column which itself is partitioned
  # further weekly by a date column.
  # 
  class MultiLevel < PartitionedBase
    self.abstract_class = true
  end
end
