require 'forwardable'

module Partitioned
  class PartitionedBase
    #
    # SqlAdapter
    # manages requests of partitioned tables.
    #
    class SqlAdapter
      attr_reader :parent_table_class

      def initialize(parent_table_class)
        @parent_table_class = parent_table_class
      end

      #
      # Ensure our function for warning about improper partition usage is in place.
      #
      # Name: always_fail_on_insert(text); Type: FUNCTION; Schema: public
      #
      # Used to raise an exception explaining why a specific insert (into a parent
      # table which should never have records) should never be attempted.
      #
      def ensure_always_fail_on_insert_exists
        sql = <<-SQL
          CREATE OR REPLACE FUNCTION always_fail_on_insert(table_name text) RETURNS boolean
              LANGUAGE plpgsql
              AS $$
           BEGIN
             RAISE EXCEPTION 'partitioned table "%" does not support direct inserts, you should be inserting directly into child tables', table_name;
             RETURN false;
           END;
          $$;
        SQL
        execute(sql)
      end

      #
      # Child tables whose parent table is 'foos', typically exist in a schema named foos_partitions.
      #
      # *partition_key_values are needed here to support the use of multiple schemas to keep tables in.
      #
      def create_partition_schema(*partition_key_values)
        create_schema(configurator.schema_name, :unless_exists => true)
      end

      #
      # Child tables whose parent table is 'foos', typically exist in a schema named foos_partitions.
      #
      # *partition_key_values are needed here to support the use of multiple schemas to keep tables in.
      #
      def drop_partition_schema(*partition_key_values)
        drop_schema(configurator.schema_name, :cascade => true)
      end

      #
      # Does a specific child partition exist.
      #
      def partition_exists?(*partition_key_values)
        return find(:first,
                    :from => "pg_tables",
                    :select => "count(*) as count",
                    :conditions  => ["schemaname = ? and tablename = ?",
                                      configurator.schema_name,
                                      configurator.part_name(*partition_key_values)
                    ]).count.to_i == 1
      end

      #
      # Returns an array of partition table names from last to first limited to
      # the number of entries requested by its first parameter.
      #
      # The magic here is in the overridden method "last_n_partitions_order_by_clause"
      # which is designed to order a list of partition table names (table names without
      # their schema name) from last to first.
      #
      # If the child table names are the format "pYYYYMMDD" where YYYY is a four digit year, MM is
      # a month number and DD is a day number, you would use the following to order from last to
      # first:
      #   tablename desc
      #
      # For child table names of the format "pXXXX" where XXXX is a number, you may want something like:
      #   substring(tablename, 2)::integer desc
      #
      # For clarity, the sql executed is:
      #    select tablename from pg_tables where schemaname = $1 order by $2 limit $3
      # where:
      #  $1 = the name of schema (foos_partitions)
      #  $2 = the order by clause that would make the greatest table name listed first
      #  $3 = the parameter 'how_many'
      #
      def last_n_partition_names(how_many = 1)
        return find(:all,
                    :from => "pg_tables",
                    :select => :tablename,
                    :conditions  => ["schemaname = ?", configurator.schema_name],
                    :order => last_n_partitions_order_by_clause,
                    :limit => how_many).map(&:tablename)
      end

      #
      # Override this or order the tables from last (greatest value? greatest date?) to first.
      #
      def last_n_partitions_order_by_clause
        return configurator.last_partitions_order_by_clause
      end

      #
      # Used to create the parent table rule to ensure.
      #
      # This will cause an error on attempt to insert into the parent table.
      #
      # We want all records to exist in one of the child tables so the
      # query planner can optimize access to the records.
      #
      def add_parent_table_rules(*partition_key_values)
        ensure_always_fail_on_insert_exists

        insert_redirector_name = parent_table_rule_name("insert", "redirector", *partition_key_values)
        sql = <<-SQL
          CREATE OR REPLACE RULE #{insert_redirector_name} AS
            ON INSERT TO #{configurator.table_name(*partition_key_values)}
            DO INSTEAD
            (
               SELECT always_fail_on_insert('#{configurator.table_name(*partition_key_values)}')
            )
        SQL
        execute(sql)
      end

      #
      # Used to drop the parent table rule.
      #
      def remove_parent_table_rules(*partition_key_values)
        insert_redirector_name = parent_table_rule_name("insert", "redirector", *partition_key_values)
        sql = <<-SQL
          DROP RULE #{insert_redirector_name}
            ON INSERT CASCADE
        SQL
        execute(sql)
      end

      #
      # The name of the table (schemaname.childtablename) given the check constraint values.
      #
      def partition_table_name(*partition_key_values)
        return configurator.table_name(*partition_key_values)
      end

      #
      # A reasonable alias for the partition table
      #
      def partition_table_alias_name(*partition_key_values)
        return configurator.table_alias_name(*partition_key_values)
      end

      #
      # Create a single child table.
      #
      def create_partition_table(*partition_key_values)
        create_table(configurator.table_name(*partition_key_values), {
                       :id => false,
                       :options => "INHERITS (#{configurator.parent_table_name(*partition_key_values)})"
                     }) do |t|
          constraint = configurator.check_constraint(*partition_key_values)
          t.check_constraint constraint if constraint
        end
      end

      #
      # Remove a specific single child table.
      #
      def drop_partition_table(*partition_key_values)
        drop_table(configurator.table_name(*partition_key_values))
      end

      #
      # Add indexes that must exist on child tables. Only leaf child tables
      # need indexes as parent table indexes are not used in postgres.
      #
      def add_partition_table_index(*partition_key_values)
        configurator.indexes(*partition_key_values).each do |field,options|
          used_options = options.clone
          unless used_options.has_key?(:name)
            name = [*field].join('_')
            used_options[:name] = used_options[:unique] ? unique_index_name(name, *partition_key_values) : index_name(name, *partition_key_values)
          end
          add_index(partition_table_name(*partition_key_values), field, used_options)
        end
      end

      #
      # Used when creating the name of a SQL rule.
      #
      def parent_table_rule_name(name, suffix = "rule", *partition_key_values)
        return "#{configurator.table_name(*partition_key_values).gsub(/[.]/, '_')}_#{name}_#{suffix}"
      end

      #
      # Used to create index names.
      #
      def index_name(name, *partition_key_values)
        return "#{configurator.part_name(*partition_key_values)}_#{name}_idx"
      end

      #
      # Used to create index names.
      #
      def unique_index_name(name, *partition_key_values)
        return "#{configurator.part_name(*partition_key_values)}_#{name}_udx"
      end

      #
      # This is here for derived classes to set up references to added columns
      # (or columns in the parent that need foreign key constraints).
      #
      # Foreign keys are not inherited in postgres. So, a parent table
      # of the form:
      #
      #   -- this is the referenced table
      #   create table companies
      #   (
      #       id               serial not null primary key,
      #       created_at       timestamp not null default now(),
      #       updated_at       timestamp,
      #       name             text not null
      #   );
      #
      #   -- this is the parent table
      #   create table employees
      #   (
      #       id               serial not null primary key,
      #       created_at       timestamp not null default now(),
      #       updated_at       timestamp,
      #       name             text not null,
      #       company_id       integer not null references companies,
      #       supervisor_id    integer not null references employees
      #   );
      #
      #   -- some children
      #   create table employees_of_company_1 ( CHECK ( company_id = 1 ) ) INHERITS (employees);
      #   create table employees_of_company_2 ( CHECK ( company_id = 2 ) ) INHERITS (employees);
      #   create table employees_of_company_3 ( CHECK ( company_id = 3 ) ) INHERITS (employees);
      #
      # Since postgres does not inherit referential integrity from parent tables, the following
      # insert will work:
      #    insert into employees_of_company_1 (name, company_id, supervisor_id) values ('joe', 1, 10);
      # even if there is no record in companies with id = 1 and there is no record in employees with id = 10
      #
      # For proper referential integrity handling you must do the following:
      #   ALTER TABLE employees_of_company_1 add foreign key (company_id) references companies(id)
      #   ALTER TABLE employees_of_company_2 add foreign key (company_id) references companies(id)
      #   ALTER TABLE employees_of_company_3 add foreign key (company_id) references companies(id)
      #
      #   ALTER TABLE employees_of_company_1 add foreign key (supervisor_id) references employees_of_company_1(id)
      #   ALTER TABLE employees_of_company_2 add foreign key (supervisor_id) references employees_of_company_2(id)
      #   ALTER TABLE employees_of_company_3 add foreign key (supervisor_id) references employees_of_company_3(id)
      #
      # The second set of alter tables brings up a good another consideration about postgres references and partitions.
      # postgres will not follow references to a child table.  So, a foreign key reference to "employees" in this
      # set of alter statements would not work because postgres would expect the table "employees" to have
      # the specific referenced record, but the record really exists in a child of employees.  So, the alter statement
      # forces the reference check on the specific child table we know must contain this employees supervisor (since
      # such a supervisor would have to work for the same company in our model).
      #
      def add_references_to_partition_table(*partition_key_values)
        configurator.foreign_keys(*partition_key_values).each do |foreign_key|
          add_foreign_key(partition_table_name(*partition_key_values),
                          foreign_key.referencing_field,
                          foreign_key.referenced_table,
                          foreign_key.referenced_field)
        end
      end

      ##
      # :method: connection
      # delegated to the connection of the parent table class

      ##
      # :method: execute
      # delegated to the connection of the parent table class

      ##
      # :method: create_schema
      # delegated to the connection of the parent table class

      ##
      # :method: drop_schema
      # delegated to the connection of the parent table class

      ##
      # :method: add_index
      # delegated to the connection of the parent table class

      ##
      # :method: remove_index
      # delegated to the connection of the parent table class

      ##
      # :method: transaction
      # delegated to the connection of the parent table class

      ##
      # :method: find_by_sql
      # delegated to the connection of the parent table class

      ##
      # :method: find
      # delegated to the connection of the parent table class

      extend Forwardable
      def_delegators :parent_table_class, :connection, :find_by_sql, :transaction, :find, :configurator
      def_delegators :connection, :execute, :add_index, :remove_index, :create_schema, :drop_schema, :add_foreign_key,
                     :create_table, :drop_table
    end
  end
end
