require 'forwardable'

module Partitioned
  class PartitionedBase
    #
    # PartitionManager
    # interface for all requests made to build partition tables.
    # these are typically delegated to us from the ActiveRecord class
    # (partitioned_base.rb defines the forwarding)
    class PartitionManager
      attr_reader :parent_table_class

      def initialize(parent_table_class)
        @parent_table_class = parent_table_class
      end

      #
      # Archive partitions that need such.
      # uses #archive_old_partition_key_values_set as the list of
      # partitions to remove.
      #
      def archive_old_partitions
        archive_old_partition_key_values_set.each do |*partition_key_values|
          archive_old_partition(*partition_key_values)
        end
      end

      #
      # Drop partitions that are no longer necessary.
      # uses #old_partition_key_values_set as the list of
      # partitions to remove.
      #
      def drop_old_partitions
        old_partition_key_values_set.each do |*partition_key_values|
          drop_old_partition(*partition_key_values)
        end
      end

      #
      # Create partitions that are needed (probably to handle data that
      # will be inserted into the database within the next few weeks).
      # uses #new_partition_key_value_set to determine the key values
      # for the specific child tables to create.
      #
      def create_new_partitions
        new_partition_key_values_set.each do |*partition_key_values|
         create_new_partition(*partition_key_values)
        end
      end

      #
      # Create any partition tables from a list.  the partition tables must
      # not already exist and its schema must already exist.
      #
      def create_new_partition_tables(enumerable)
        enumerable.each do |partition_key_values|
          create_new_partition(*partition_key_values)
        end
      end

      #
      # The once called function to prepare a parent table for partitioning as well
      # as create the schema that the child tables will be placed in.
      #
      def create_infrastructure
        create_partition_schema
        add_parent_table_rules
      end

      #
      # The once called function to drop a parent the schema.
      #
      def delete_infrastructure
        drop_partition_schema
        remove_parent_table_rules
      end

      #
      # An array of key values (each key value is an array of keys) that represent
      # the child partitions that should be created.
      #
      # Used by #create_new_partitions and generally called once a day to update
      # the database with new soon-to-be needed child tables.
      #
      def new_partition_key_values_set
        return configurator.janitorial_creates_needed
      end

      #
      # An array of key values (each key value is an array of keys) that represent
      # the child partitions that should be archived probably because they are
      # about to be dropped.
      #
      # Used by #archive_old_partitions and generally called once a day to clean up
      # unneeded child tables.
      #
      def archive_old_partition_key_values_set
        return configurator.janitorial_archives_needed
      end

      #
      # An array of key values (each key value is an array of keys) that represent
      # the child partitions that should be dropped because they are no longer needed.
      #
      # Used by #drop_old_partitions and generally called once a day to clean up
      # unneeded child tables.
      #
      def old_partition_key_values_set
        return configurator.janitorial_drops_needed
      end

      #
      # Archive a specific partition from the database given
      # the key value(s) of its check constraint columns.
      #
      def archive_old_partition(*partition_key_values)
        archive_partition_table(*partition_key_values)
      end

      #
      # Remove a specific partition from the database given
      # the key value(s) of its check constraint columns.
      #
      def drop_old_partition(*partition_key_values)
        drop_partition_table(*partition_key_values)
      end

      #
      # Create a specific child table that does not currently
      # exist and whose schema (the schema that the table exists in)
      # also already exists (#create_infrastructure is designed to
      # create this).
      #
      def create_new_partition(*partition_key_values)
        create_partition_table(*partition_key_values)
        add_partition_table_index(*partition_key_values)
        add_references_to_partition_table(*partition_key_values)
        configurator.run_after_partition_table_create_hooks(*partition_key_values)
      end

      ##
      # :method: drop_partition_table
      # delegated to Partitioned::PartitionedBase::PartitionManager::SqlAdapter#drop_partition_table

      ##
      # :method: create_partition_table
      # delegated to Partitioned::PartitionedBase::PartitionManager::SqlAdapter#create_partition_table

      ##
      # :method: add_partition_table_index
      # delegated to Partitioned::PartitionedBase::PartitionManager::SqlAdapter#add_partition_table_index

      ##
      # :method: add_references_to_partition_table
      # delegated to Partitioned::PartitionedBase::PartitionManager::SqlAdapter#add_references_to_partition_table

      ##
      # :method: create_partition_schema
      # delegated to Partitioned::PartitionedBase::PartitionManager::SqlAdapter#create_partition_schema

      ##
      # :method: add_parent_table_rules
      # delegated to Partitioned::PartitionedBase::PartitionManager::SqlAdapter#add_parent_table_rules

      ##
      # :method: partition_table_name
      # delegated to Partitioned::PartitionedBase::PartitionManager::SqlAdapter#partition_table_name

      ##
      # :method: partition_table_alias_name
      # delegated to Partitioned::PartitionedBase::PartitionManager::SqlAdapter#partition_table_alias_name

      ##
      # :method: sql_adapter
      # delegated to Partitioned::PartitionedBase#sql_adapter

      ##
      # :method: configurator
      # delegated to Partitioned::PartitionedBase#configurator

      extend Forwardable
      def_delegators :parent_table_class, :sql_adapter, :configurator
      def_delegators :sql_adapter, :drop_partition_table, :create_partition_table, :add_partition_table_index,
         :add_references_to_partition_table, :create_partition_schema, :drop_partition_schema, :add_parent_table_rules,
         :remove_parent_table_rules, :partition_table_name, :partition_table_alias_name

    end
  end
end
