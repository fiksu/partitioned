require 'active_record/model_schema'

module ActiveRecord
  module ModelSchema
    included do
      def table_name
        symbolized_attributes = attributes.symbolize_keys
        return self.class.table_name(*self.class.table_partition_keys.map{|attribute_name| symbolized_attributes[attribute_name]})
      end
    end

    module ClassMethods
      def table_name(*table_partition_key_values)
        reset_table_name unless defined?(@table_name)
        @table_name
      end

      # Sets the table name explicitly. Example:
      #
      #   class Project < ActiveRecord::Base
      #     self.table_name = "project"
      #   end
      #
      # You can also just define your own <tt>self.table_name</tt> method; see
      # the documentation for ActiveRecord::Base#table_name.
      def table_name=(value)
        @original_table_name = @table_name if defined?(@table_name)
        @table_name          = value && value.to_s
        @quoted_table_name   = nil
        reset_arel_table
        @relation            = Relation.new(self, arel_table)
      end

      def set_table_name(value = nil, &block) #:nodoc:
        deprecated_property_setter :table_name, value, block
        @quoted_table_name = nil
        reset_arel_table
        @relation          = Relation.new(self, arel_table)
      end

      #
      # Returns an array of attribute names (strings) used to fetch the key value(s)
      # the determine this specific partition table.
      #
      # @return [String] the column name used to partition this table
      # @return [Array<String>] the column names used to partition this table
      def table_partition_keys
        return []
      end

      #
      # The specific values for a partition of this active record's type which are defined by
      # {#self.table_partition_keys}
      #
      # @param [Hash] values key/value pairs to extract values from
      # @return [Object] value of partition key
      # @return [Array<Object>] values of partition keys
      def table_partition_key_values(values)
        symbolized_values = values.symbolize_keys
        return self.table_partition_keys.map{|key| symbolized_values[key.to_sym]}
      end

      #
      # This scoping is used to target the
      # active record find() to a specific child table and alias it to the name of the
      # parent table (so activerecord can generally work with it)
      #
      # Use as:
      #
      #   Foo.from_partition(KEY).find(:first)
      #
      # where KEY is the key value(s) used as the check constraint on Foo's table.
      #
      # @param [*Array<Object>] partition_field the field values to partition on
      # @return [Hash] the scoping
      def from_partition(*partition_field)
        table_alias_name = table_alias_name(*partition_field)
        from("#{table_name(*partition_field)} AS #{table_alias_name}").
          tap{|relation| relation.table.table_alias = table_alias_name}
      end

      #
      # This scope is used to target the
      # active record find() to a specific child table. Is probably best used in advanced
      # activerecord queries when a number of tables are involved in the query.
      #
      # Use as:
      #
      #   Foo.from_partitioned_without_alias(KEY).find(:all, :select => "*")
      #
      # where KEY is the key value(s) used as the check constraint on Foo's table.
      #
      # it's not obvious why :select => "*" is supplied.  note activerecord wants
      # to use the name of parent table for access to any attributes, so without
      # the :select argument the sql result would be something like:
      #
      #   SELECT foos.* FROM foos_partitions.pXXX
      #
      # which fails because table foos is not referenced.  using the form #from_partition
      # is almost always the correct thing when using activerecord.
      #
      # Because the scope is specific to a class (a class method) but unlike
      # class methods is not inherited, one  must use this form (#from_partitioned_without_alias) instead
      # of #from_partitioned_without_alias_scope to get the most derived classes specific active record scope.
      #
      # @param [*Array<Object>] partition_field the field values to partition on
      # @return [Hash] the scoping
      def from_partitioned_without_alias(*partition_field)
        table_alias_name = table_name(*partition_field)
        from(table_alias_name).
          tap{|relation| relation.table.table_alias = table_alias_name}
      end

      def table_alias_name(*partition_field)
        return table_name(*partition_field)
      end

      #
      # partitioning needs to be able to specify if 
      # we should prefetch the primary key (to determine
      # the specific table we will insert in to we
      # need to know the partition key values.
      #
      # this needs to be on the model NOT the connection
      #
      # for the simple case we just pass the question on to
      # the connection
      def prefetch_primary_key?
        connection.prefetch_primary_key?(table_name)
      end
    end

  end
end
