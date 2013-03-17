module Partitioned
  class MultiLevel
    #
    # the manger of partitioned requests for models partitioned multiple times
    #
    class PartitionManager < Partitioned::PartitionedBase::PartitionManager
      #
      # The once called function to prepare a parent table for partitioning as well
      # as create the schema that the child tables will be placed in.
      #
      # @param [Enumerable] enumerable (Array<Array>) the key values that should be used to create the parent partition tables.
      # @return [optional]
      def create_infrastructure(enumerable = [[]])
        super()
        enumerable.each do |*partition_key_values|
          create_partition_schema(*partition_key_values)
        end
      end

      #
      # Create a specific child table that does not currently
      # exist and whose schema (the schema that the table exists in)
      # also already exists (#create_infrastructure is designed to
      # create this).
      #
      # @param [*Array<Object>] partition_key_values all key values needed to create a partition
      # @return [optional]
      def create_new_partition(*partition_key_values)
        create_partition_table(*partition_key_values)
        if is_leaf_partition?(*partition_key_values)
          add_partition_table_index(*partition_key_values)  
          add_references_to_partition_table(*partition_key_values)
        else
          add_parent_table_rules(*partition_key_values)
        end
      end

      protected

      #
      # Is the table a child table without itself having any children.
      # generally leaf tables are where all indexes and foreign key
      # constraints will be placed because that is where the data will be.
      #
      # Non leaf tables will typically have a rule placed on them
      # (via add_parent_table_rules) that prevents any inserts from occurring
      # on them.
      #
      # @param [*Array<Object>] partition_key_values all key values specifying a given child table
      # @return [Boolean] true if this partition should contain records
      def is_leaf_partition?(*partition_key_values)
        return partition_key_values.length == parent_table_class.configurator.on_fields.length
      end
    end
  end
end
