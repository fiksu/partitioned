#
# :include: ../../README
#

module Partitioned
  #
  # Used by PartitionedBase class methods that must be overridden.
  #
  class MethodNotImplemented < StandardError
    def initialize(model, method_name, is_class_method = true)
      super("#{model.name}#{is_class_method ? '.' : '#'}#{method_name}")
    end
  end

  #
  # PartitionedBase
  # an ActiveRecord::Base class that can be partitioned.
  #
  # Uses a domain specific language to configure, see Partitioned::PartitionedBase::Configurator
  # for more information.
  #
  # Extends Partitioned::BulkMethodsMixin to provide create_many and update_many.
  #
  # Uses PartitionManager to manage creation of child tables.
  #
  # Monkey patches some ActiveRecord routines to call back to this class when INSERT and UPDATE
  # statements are built (to determine the table_name with respect to values being inserted or updated)
  #
  class PartitionedBase < ActiveRecord::Base
    extend Partitioned::BulkMethodsMixin

    self.abstract_class = true

    #
    # Returns an array of attribute names (strings) used to fetch the key value(s)
    # the determine this specific partition table.
    #
    # @return [String] the column name used to partition this table
    # @return [Array<String>] the column names used to partition this table
    def self.table_partition_keys
      return configurator.on_fields
    end

    #
    # Normalize the value to be used for partitioning. This allows, for instance, a class that partitions on
    # a time field to group the times by month. An integer field might be grouped by every 10mil values, A
    # string field might be grouped by its first character.
    #
    # @param [Object] value the partition key value
    # @return [Object] the normalized value for the key value passed in
    def self.partition_normalize_key_value(value)
      return value
    end

    #
    # Range generation provided for methods like created_infrastructure that need a set of partition key values
    # to operate on.
    #
    # @param [Object] start_value the first value to generate the range from
    # @param [Object] end_value the last value to generate the range from
    # @param [Object] step (1) number of values to advance.
    # @return [Enumerable] the range generated
    def self.partition_generate_range(start_value, end_value, step = 1)
      return Range.new(start_value, end_value).step(step)
    end

    #
    # Return an instance of this partition table's table manager.
    #
    # @return [{PartitionManager}] the partition manager for this partitioned model
    def self.partition_manager
      @partition_manager = self::PartitionManager.new(self) unless @partition_manager.present?
      return @partition_manager
    end

    #
    # Return an instance of this partition table's sql_adapter (used by the partition manage to
    # create SQL statements)
    #
    # @return [{SqlAdapter}] the object used to create sql statements for this partitioned model
    def self.sql_adapter
      @sql_adapter = self::SqlAdapter.new(self) unless @sql_adapter.present?
      return @sql_adapter
    end

    #
    # Return a object used to read configurator information.
    #
    # @return [{Configurator::Reader}] the configuration reader for this partitioned model
    def self.configurator
      unless @configurator
        @configurator = self::Configurator::Reader.new(self)
      end
      return @configurator
    end

    #
    # Yields an object used to configure the ActiveRecord class for partitioning
    # using the Configurator Domain Specific Language.
    # 
    # usage:
    #   partitioned do |partition|
    #     partition.on    :company_id
    #     partition.index :id, :unique => true
    #     partition.foreign_key :company_id
    #   end
    #
    # @return [{Configurator::Dsl}] the Domain Specifical Language UI manager
    def self.partitioned
      @configurator_dsl ||= self::Configurator::Dsl.new(self)
      yield @configurator_dsl
    end

    #
    # Returns the configurator DSL object.
    #
    # @return [{Configurator::Dsl}] the Domain Specifical Language UI manager
    def self.configurator_dsl
      return @configurator_dsl
    end

    partitioned do |partition|
      #
      # The schema name to place all child tables.
      #
      # By default this will be the table name of the parent class with a suffix "_partitions".
      #
      # For a parent table name foos, that would be foos_partitions
      #
      # N.B.: if the parent table is not in the default schema ("public") the name of the
      # partition schema is prefixed by the schema name of the parent table and an
      # underscore.  That is, if a parent table schema/table name is "other.foos"
      # the schema for its partitions will be "other_foos_partitions"
      #
      partition.schema_name lambda {|model|
        schema_parts = []
        table_parts = model.table_name.split('.')
        # table_parts should be either ["table_name"] or ["schema_name", "table_name"]
        if table_parts.length == 2
          # XXX should we find the schema_path here and accept anything in the path as "public"
          unless table_parts.first == "public"
            schema_parts << table_parts.first
          end
        end
        schema_parts << table_parts.last
        schema_parts << 'partitions'
        return schema_parts.join('_')
      }

      #
      # The table name of the table who is the direct ancestor of a child table.
      # The child table is defined by the partition key values passed in.
      #
      # By default this is just the active record's notion of the name of the class.
      # Multi Level partitioning requires more work.
      #
      partition.parent_table_name lambda {|model, *partition_key_values|
        return model.table_name
      }

      #
      # The schema name of the table who is the direct ancestor of a child table.
      # The child table is defined by the partition key values passed in.
      #
      partition.parent_table_schema_name lambda {|model, *partition_key_values|
        table_parts = model.table_name.split('.')
        # table_parts should be either ["table_name"] or ["schema_name", "table_name"]
        return table_parts.first if table_parts.length == 2
        return "public"
      }

      #
      # The prefix for a child table's name. This is typically a letter ('p') so that
      # the base_name of the table can be a number generated programtically from
      # the partition key values.
      #
      # For instance, a child table of the table 'foos' may be partitioned on
      # the column company_id whose value is 42.  specific child table would be named
      # 'foos_partitions.p42'
      #
      # The 'p' is the name_prefix because 'foos_partitions.42' is not a valid table name
      # (without quoting).
      #
      partition.name_prefix lambda {|model, *partition_key_values|
        return "p"
      }

      #
      # The child tables name without the schema name.
      #
      partition.part_name lambda {|model, *partition_key_values|
        configurator = model.configurator
        return "#{configurator.name_prefix}#{configurator.base_name(*partition_key_values)}"
      }

      #
      # The full name of a child table defined by the partition key values.
      #
      partition.table_name lambda {|model, *partition_key_values|
        configurator = model.configurator
        return "#{configurator.schema_name}.#{configurator.part_name(*partition_key_values)}"
      }

      #
      # A reasonable alias for this table
      #
      partition.table_alias_name lambda {|model, *partition_key_values|
        return model.configurator.parent_table_name(*partition_key_values).gsub('.', '_')
      }

      #
      # The name of the child table without a schema name or prefix. this is used to
      # build child table names for multi-level partitions.
      #
      # For a table named foos_partitions.p42, this would be "42"
      #
      partition.base_name lambda { |model, *partition_key_values|
        return model.partition_normalize_key_value(*partition_key_values).to_s
      }
    end

    #
    # this methods are hand delegated because forwardable module conflicts
    # with rails delegate.
    #

    ##
    # :singleton-method: drop_partition_table
    # delegated to Partitioned::PartitionedBase::PartitionManager#drop_partition_table
    def self.drop_partition_table(*partition_key_values)
      partition_manager.drop_partition_table(*partition_key_values)
    end

    ##
    # :singleton-method: create_partition_table
    # delegated to Partitioned::PartitionedBase::PartitionManager#create_partition_table
    def self.create_partition_table(*partition_key_values)
      partition_manager.create_partition_table(*partition_key_values)
    end

    ##
    # :singleton-method: add_partition_table_index
    # delegated to Partitioned::PartitionedBase::PartitionManager#add_partition_table_index
    def self.add_partition_table_index(*partition_key_values)
      partition_manager.add_partition_table_index(*partition_key_values)
    end

    ##
    # :singleton-method: add_references_to_partition_table
    # delegated to Partitioned::PartitionedBase::PartitionManager#add_references_to_partition_table
    def self.add_references_to_partition_table(*partition_key_values)
      partition_manager.add_references_to_partition_table(*partition_key_values)
    end

    ##
    # :method: create_partition_schema
    # delegated to Partitioned::PartitionedBase::PartitionManager#create_partition_schema
    def self.create_partition_schema(*partition_key_values)
      partition_manager.create_partition_schema(*partition_key_values)
    end

    ##
    # :singleton-method: add_parent_table_rules
    # delegated to Partitioned::PartitionedBase::PartitionManager#add_parent_table_rules
    def self.add_parent_table_rules(*partition_key_values)
      partition_manager.add_parent_table_rules(*partition_key_values)
    end

    ##
    # :method: archive_old_partitions
    # delegated to Partitioned::PartitionedBase::PartitionManager#archive_old_partitions
    def self.archive_old_partitions
      partition_manager.archive_old_partitions
    end

    ##
    # :method: drop_old_partitions
    # delegated to Partitioned::PartitionedBase::PartitionManager#drop_old_partitions
    def self.drop_old_partitions
      partition_manager.drop_old_partitions
    end

    ##
    # :method: create_new_partitions
    # delegated to Partitioned::PartitionedBase::PartitionManager#create_new_partitions
    def self.create_new_partitions
      partition_manager.create_new_partitions
    end

    ##
    # :method: archive_old_partition
    # delegated to Partitioned::PartitionedBase::PartitionManager#archive_old_partition
    def self.archive_old_partition(*partition_key_values)
      partition_manager.archive_old_partition(*partition_key_values)
    end

    ##
    # :method: drop_old_partition
    # delegated to Partitioned::PartitionedBase::PartitionManager#drop_old_partition
    def self.drop_old_partition(*partition_key_values)
      partition_manager.drop_old_partition(*partition_key_values)
    end

    ##
    # :method: create_new_partition
    # delegated to Partitioned::PartitionedBase::PartitionManager#create_new_partition
    def self.create_new_partition(*partition_key_values)
      partition_manager.create_new_partition(*partition_key_values)
    end

    ##
    # :method: create_new_partition_tables
    # delegated to Partitioned::PartitionedBase::PartitionManager#create_new_partition_tables
    def self.create_new_partition_tables(enumerable)
      partition_manager.create_new_partition_tables(enumerable)
    end

    ##
    # :method: create_infrastructure
    # delegated to Partitioned::PartitionedBase::PartitionManager#create_infrastructure
    def self.create_infrastructure
      partition_manager.create_infrastructure
    end

    ##
    # :method: table_name
    # delegated to Partitioned::PartitionedBase::PartitionManager#table_name
    def self.table_name(*partition_key_values)
      retval = super
      return retval if partition_key_values.blank?
      return partition_manager.partition_table_name(*partition_key_values)
    end

    ##
    # :method: partition_table_alias_name
    # delegated to Partitioned::PartitionedBase::PartitionManager#partition_table_alias_name
    def self.table_alias_name(*partition_key_values)
      return partition_manager.partition_table_alias_name(*partition_key_values)
    end
  end
end
