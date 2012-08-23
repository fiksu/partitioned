require 'active_record'
require 'active_record/base'
require 'active_record/connection_adapters/abstract_adapter'

#
# Patching {ActiveRecord::ConnectionAdapters::TableDefinition} and
# {ActiveRecord::ConnectionAdapters::PostgreSQLAdapter} to add functionality
# needed to abstract partition specific SQL statements.
#
module ActiveRecord::ConnectionAdapters
  #
  # Patches associated with building check constraints.
  #
  class TableDefinition
    #
    # Builds a SQL check constraint
    #
    # @param [String] constraint a SQL constraint
    def check_constraint(constraint)
      @columns << Struct.new(:to_sql).new("CHECK (#{constraint})")
    end
  end

  #
  # Patches extending the postgres adapter with new operations for managing
  # sequences (and sets of sequence values), schemas and foreign keys.
  # These should go into AbstractAdapter allowing any database adapter
  # to take advantage of these SQL builders.
  #
  class PostgreSQLAdapter < AbstractAdapter
    #
    # Get the next value in a sequence. Used on INSERT operation for
    # partitioning like by_id because the ID is required before the insert
    # so that the specific child table is known ahead of time.
    #
    # @param [String] sequence_name the name of the sequence to fetch the next value from
    # @return [Integer] the value from the sequence
    def next_sequence_value(sequence_name)
      return execute("select nextval('#{sequence_name}')").field_values("nextval").first.to_i
    end

    #
    # Get the some next values in a sequence.
    #
    # @param [String] sequence_name the name of the sequence to fetch the next values from
    # @param [Integer] batch_size count of values.
    # @return [Array<Integer>] an array of values from the sequence
    def next_sequence_values(sequence_name, batch_size)
      result = execute("select nextval('#{sequence_name}') from generate_series(1, #{batch_size})")
      return result.field_values("nextval").map(&:to_i)
    end

    #
    # Causes active resource to fetch the primary key for the table (using next_sequence_value())
    # just before an insert. We need the prefetch to happen but we don't have enough information
    # here to determine if it should happen, so Relation::insert has been modified to request of
    # the ActiveRecord::Base derived class if it requires a prefetch.
    #
    # @param [String] table_name the table name to query
    # @return [Boolean] returns true if the table should have its primary key prefetched.
    def prefetch_primary_key?(table_name)
      return false
    end

    #
    # Creates a schema given a name.
    #
    # @param [String] name the name of the schema.
    # @param [Hash] options ({}) options for creating a schema
    # @option options [Boolean] :unless_exists (false) check if schema exists.
    # @return [optional] undefined
    def create_schema(name, options = {})
      if options[:unless_exists]
        return if execute("select count(*) from pg_namespace where nspname = '#{name}'").getvalue(0,0).to_i > 0
      end
      execute("CREATE SCHEMA #{name}")
    end

    #
    # Drop a schema given a name.
    #
    # @param [String] name the name of the schema.
    # @param [Hash] options ({}) options for dropping a schema
    # @option options [Boolean] :if_exists (false) check if schema exists.
    # @option options [Boolean] :cascade (false) drop dependant objects
    # @return [optional] undefined
    def drop_schema(name, options = {})
      if options[:if_exists]
        return if execute("select count(*) from pg_namespace where nspname = '#{name}'").getvalue(0,0).to_i == 0
      end
      execute("DROP SCHEMA #{name}#{' cascade' if options[:cascade]}")
    end

    #
    # Add foreign key constraint to table.
    #
    # @param [String] referencing_table_name the name of the table containing the foreign key
    # @param [String] referencing_field_name the name of foreign key column
    # @param [String] referenced_table_name the name of the table referenced by the foreign key
    # @param [String] referenced_field_name (:id) the name of the column referenced by the foreign key
    # @return [optional] undefined
    def add_foreign_key(referencing_table_name, referencing_field_name, referenced_table_name, referenced_field_name = :id)
      execute("ALTER TABLE #{referencing_table_name} add foreign key (#{referencing_field_name}) references #{referenced_table_name}(#{referenced_field_name})")
    end
  end
end
