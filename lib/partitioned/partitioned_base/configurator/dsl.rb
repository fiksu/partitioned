module Partitioned
  class PartitionedBase
    module Configurator
      #
      # The Domain Specific Language manager for configuring partitioning.
      #
      # @example of use:
      #
      #   class Employee < Partitioned::ByCreatedAt
      #     partitioned do |partition|
      #       partition.index :id, :unique => true
      #       partition.foreign_key :company_id
      #     end
      #   end
      #
      #   in the above example, block:
      #
      #     partitioned do |partition|
      #        ...
      #     end
      #
      #   Scopes a set of "partition directives".  The directives are accessed via the block parameter 'partition'
      #
      # Directives parameters have two forms, a canonical form which takes a set of parameters
      # and a dynamic form which takes a single parameter which is either a string that should be interpolated or
      # a lambda.
      #
      # The dynamic forms are expected to return some value(s), similar to the canonical form.
      #
      # DYNAMIC FORM: LAMBDA
      # Lambdas have passed (sometimes) one and (sometimes) two parameters (depending on the directives requirements), as in:
      #
      # class Employee < Partitioned::Base
      #   def self.partition_time_field
      #     return :created_at
      #   end
      #
      #   partitioned do |partition|
      #     partition.on lambda{ |model| return model.partition_time_field}
      #     partition.constraint lambda{|model,time_field_value|
      #       return "#{model.partition_time_field} >= '#{time_field_value.strftime}' and
      #         #{model.partition_time_field} < '#{(time_field_value + 1.day).strftime}'"
      #     }
      #   end
      # end
      #
      # Single parameter lambdas are passed the most derived Partitioned::PartitionedBase class that defines the
      # model of this partitioned table:
      #    lambda {|model| return nil}
      #
      # Arguments:
      #  model - the Partitioned::PartitionedBase class that is being partitioned.
      #
      # Multi parameter lambdas are passed the model being partitioned and the values to specify a single child table.
      #    lambda {|model,*partition_key_values| return nil}
      #
      # Arguments:
      #  model - the Partitioned::PartitionedBase class that is being partitioned.
      #  *partition_key_values - the values used to partition a child table.
      #
      # Another example of a lambda that is passed partitioned values would be:
      #    lambda {|model,created_at| return nil}
      #
      # This example, which names its parameter 'created_at', is more natural to deal with if
      # you know the parameter passed in will always be a single field and the field's name
      # is 'created_at'.
      #
      # DYNAMIC FORM: STRINGS
      # Strings to be interpolated have access to two parameters:
      #  model - the ActiveRecord class that is being partitioned.
      #  partition_key_values - an array of values used to partition a child table.
      #  field_value - the first element of partition_key_values (for convience)
      #
      # for instance:
      # class Employee < Partitioned::PartitionBase
      #   partitioned do |partition|
      #     partition.on :foo
      #     partition.index :bar
      #     partition.index :baz
      #     partition.constraint %q!foo = #{field_value}!
      #   end
      # end
      #
      # is the same as:
      # class Employee < Partitioned::PartitionBase
      #   def self.partitioned_field
      #     return :foo
      #   end
      #   def self.index_field1
      #     return :bar
      #   end
      #   def self.index_field2
      #     return :baz
      #   end
      #   partitioned do |partition|
      #     partition.on '#{model.partitioned_field}'
      #     partition.index lambda { |model, *partition_key_values|
      #       return Configurator::Data::Index.new(model.index_field1, {})
      #     }
      #     partition.index lambda { |model, *partition_key_values|
      #       return Configurator::Data::Index.new(model.index_field2, {})
      #     }
      #     partition.constraint %q!#{model.partitioned_field} = #{field_value}!
      #   end
      # end
      #
      # IMPORTANT: note that lambdas receive, as their first parameter 'model', the most derived class that is being partition.
      # This is not always the same as the class accessible through 'self' because 'self' is evaluated at lambda definition time.
      # Use 'model' for general access to the Partitioned:PartitionedBase model.
      # 
      # The follow example shows incorrect access to the model:
      #
      #!bad! class Employee < Partitioned::PartitionBase
      #!bad!   def self.index_field
      #!bad!     return :bar
      #!bad!   end
      #!bad!   partitioned do |partition|
      #!bad!     partition.on lambda { |model| return index_field }
      #!bad!     partition.index lambda { |model, *partition_key_values|
      #!bad!       return Configurator::Data::Index.new(self.index_field, {})
      #!bad!     }
      #!bad!   end
      #!bad! end
      #!bad!
      #!bad! class FavoriteEmployee < Employee
      #!bad!   def self.index_field
      #!bad!     return :baz
      #!bad!   end
      #!bad! end
      #
      # In the above (bad code) the directive 'index' parameter is a lambda using "self.index_field" instead of "model.index_field"
      # and the directive 'on' parameter is a lambda using "index_field" instead of "model.index_field".
      # Since resolution of the self occurs at lambda definition time, self is Employee instead of
      # FavoriteEmployee and index_field will be :bar instead of :baz.  model.index_field will
      # resolve to FavoriteEmployee.index_field and be :baz as expected.
      #
      class Dsl
        #
        # raised when a partitioned DSL directive's parameters are considered invalid
        #
        class InvalidConfiguratorDirectiveValue < StandardError
          def initialize(model, table_name, directive, value, explanation)
            super("#{model.name} [#{table_name}] invalid value '#{value}' for partitioned directive '#{directive}'.  #{explanation}")
          end
        end

        attr_reader :data, :model

        def initialize(most_derived_activerecord_class, data_class = Partitioned::PartitionedBase::Configurator::Data)
          @model = most_derived_activerecord_class
          @data = data_class.new
        end

        #
        # The field used to partition child tables
        #
        # arguments:
        #   field - the name of the field
        #
        # usage:
        #   partitioned do |partition|
        #     partition.on :company_id
        #   end
        #
        # or:
        #
        # arguments:
        #   lambda(model) - proc returning the name of the field to partition child tables
        #
        # usage:
        #   partitioned do |partition|
        #     partition.on lambda {|model| model.partition_field}
        #   end
        #
        # or:
        #
        # arguments:
        #   string - a string to be interpolated naming the field to partition child tables
        #
        # usage:
        #   partitioned do |partition|
        #     partition.on '#{model.partition_field}'
        #   end
        #
        # one might use the latter forms to consolidate information about the field name when
        # it might be used in several DSL directives.
        #
        def on(field)
          data.on_field = field
        end

        #
        # Define an index to be created on all (leaf-) child tables.
        #
        # arguments:
        #  field - the name of the field (or an array of fields) to index
        #  options - (not required) options passed to add_table_index()
        #
        # usage:
        #   partitioned do |partition|
        #     partition.index :id, :unique => true
        #   end
        #
        # or:
        #
        # arguments:
        #  lambda(model, *partition_key_values) - a procedure that will return a Partitioned::PartitionedBase::Configurator::Data::Index
        #
        # usage:
        #   partitioned do |partition|
        #     partition.index lambda { |model, *partition_key_values|
        #       return Configurator::Data::Index.new(model.partition_field, {})
        #     }
        #   end
        #
        # note: this system only applies indexes to leaf child tables because indexes on parent tables,
        # will not be used by the inherited tables (in postgres).
        # 
        def index(field, options = {})
          if field.is_a? Proc
            data.indexes << field
          else
            data.indexes << Partitioned::PartitionedBase::Configurator::Data::Index.new(field, options)
          end
        end

        #
        # Define a foreign key on a (leaf-) child table.
        #
        # arguments:
        #  referencing_field - the local field that references a foreign key
        #  referenced_table - (optional: derived from referencing_field) the foreign table that is referenced
        #  referenced_field - (optional: default :id) the foreign tables key to reference
        #
        # usage:
        #   partitioned do |partition|
        #     partition.foreign_key :company_id
        #     partition.foreign_key :home_town_id, :cities
        #   end
        #
        # or:
        #
        # arguments:
        #  proc(model, *partition_key_values) - a procedure that will return an instance of
        #    Partitioned::PartitionedBase::Configurator::Data::ForeignKey
        #
        # usage:
        #   partitioned do |partition|
        #     partition.foreign_key lambda { |model, *partition_key_values|
        #       return Configurator::Data::ForeignKey.new(model.foreign_key_field)
        #     }
        #   end
        #
        # note: as with indexes, foreign key constraints are not inherited by child tables (in postgres).
        # this system only applies foreign keys to leaf tables.
        #
        def foreign_key(referencing_field, referenced_table = nil, referenced_field = :id)
          if referencing_field.is_a? Proc
            data.foreign_keys << referencing_field
          else
            data.foreign_keys << Partitioned::PartitionedBase::Configurator::Data::ForeignKey.
                                    new(referencing_field, referenced_table, referenced_field)
          end
        end

        def after_partition_table_create_hook(method_name)
          if method_name.is_a? Proc
            data.after_partition_table_create_hooks << method_name
          else
            # XXX should be a symbol
            data.after_partition_table_create_hooks << lambda {|model,*partition_key_values| model.send(method_name, *partition_key_values)}
          end
        end


        #
        # Define the check constraint for a given child table.
        #
        # arguments:
        #  constraint - a string defining the constraint for all child tables
        #
        # usage:
        #   partitioned do |partition|
        #     partition.constraint 'company_id = #{field_value}'
        #   end
        #
        # note: the usage of single quotes to prevent string interpolation at definition time.  This string will be interpolated at
        # run-time and "field_value" will be set to the value of the first element of partition_key_values.
        #
        # or:
        #
        # arguments:
        #  lambda(model, *partitioned_key_values) - a procedure returning a string defining the child table's constraint.  The
        #     child table is defined by the *partitioned key_values.
        #
        # usage:
        #   partitioned do |partition|
        #     partition.constraint lambda {|model, value|
        #       return "#{model.field_to_partition} = #{value}"
        #     }
        #   end
        #
        def check_constraint(constraint)
          data.check_constraint = constraint
        end

        #
        # Define the order by clause used to list all child table names in order of "last to be used" to "oldest to have been used".
        #
        # the sql used for a table ORDERS whose child tables exist in ORDERS_PARTITIONS would look like:
        #
        #   select tablename from pg_tables where schemaname = 'orders_partitions' order by tablename
        #
        # for instance, if child tables of ORDERS are partitioned by id range (say every 5 values) and have the form orders_partition.pN,
        # where N is the lowest value in the range of ids for that table, the order clause would probably be
        # "substring(tablename, 2)::integer desc" which would result in:
        #	p100
        #	p95
        #	p90
        #	p85
        #	p80
        #	p75
        #	p70
        #	p65
        #	p60
        #	p55
        #	p50
        #	p45
        #	p40
        #	p35
        #	p30
        #	p25
        #	p20
        #	p15
        #	p10
        #	p5
        #	p0
        #
        # this is used in PartitionedBase.last_n_partition_names(limit = 1) which should be used in code that wishes to determine
        # if there are enough room in child tables for future use.  such processing code is not apart of
        # partitioned code base, but this helper is here to assist in building such code.
        #
        # one might know that 1 partition is used per day and wish to have 10 partitions available for unforseeable spikes of data load.
        # one might run a script once per day which calls Order.last_n_partition_names(10) and if any of the returned tables have any rows
        # (or the sequence is set into any of the ranges in the child table) it is time to create new child tables
        #
        def order(order_type, options = {})
          if order_type.is_a? Proc
            data.order_type = order_type
          else
            data.order_type = Partitioned::PartitionedBase::Configurator::Data::Order.new(order_type, options)
          end
        end

        #
        # The name of the schema that will contain all child tables.
        #
        # arguments:
        #  value - a string, the name of schema
        #
        # usage:
        #   partitioned do |partition|
        #     partition.schema_name "foos_partitions"
        #   end
        #
        # or:
        #
        # arguments:
        #  lambda(model, *partitioned_key_values) - a proc returning a string which is the name of the schema
        #
        # usage:
        #   partitioned do |partition|
        #     partition.schema_name lambda {|model, *value|
        #       return "#{model.table_name}_partitions"
        #     }
        #   end
        #
        # or:
        #
        # arguments:
        #  string - to be interpolated at run time
        #
        # usage:
        #   partitioned do |partition|
        #     partition.schema_name '#{model.table_name}_partitions'
        #   end
        # 
        # the default is similar to the second usage: TABLENAME_partitions, for a table named 'foos' the schema name will be
        # foos_partitions
        #
        def schema_name(value)
          data.schema_name = value
        end

        #
        # The prefix for the child table's name.
        #
        # by default this is the (second) 'p' in the fully qualified name 'foos_partitions.p42'
        #
        # arguments:
        #  value - a string, the prefix name
        #
        # usage:
        #   partitioned do |partition|
        #     partition.name_prefix "p"
        #   end
        #
        # or:
        #
        # arguments:
        #  lambda(model, *partitioned_key_values) - a proc returning a string which is the prefix
        #
        # usage:
        #   partitioned do |partition|
        #     partition.name_prefix lambda {|model, *value|
        #       return "#{model.table_name}_child_"
        #     }
        #   end
        #
        # or:
        #
        # arguments:
        #  string - to be interpolated at run time
        #
        # usage:
        #   partitioned do |partition|
        #     partition.name_prefix '#{model.table_name}_child_'
        #   end
        # 
        # the default is 'p'
        #
        def name_prefix(value)
          data.name_prefix = value
        end

        #
        # The name of the child table without the schema name or name prefix.
        #
        # in the example: 'foos_partitions.p42' the base_name would be 42
        #
        # arguments:
        #  value - a string, the base name
        #
        # usage:
        #   partitioned do |partition|
        #     partition.base_name "42"
        #   end
        #
        # arguments:
        #  value - a string, the base name child table to be interpolated at runtime
        #
        # usage:
        #   partitioned do |partition|
        #     partition.base_name '#{model.partition_normalize_key_value(field_value)}'
        #   end
        #
        # note: the string passed to partition.base_name is built using single quotes so that the
        # the string will not be interpolated at definition time.  Rather, the string will be
        # interpolated at run-time when it will scope the interpolation with the
        # partition_key_values array (and field_value which the the first element of that array)
        #
        # or:
        #
        # arguments:
        #  lambda(model, *partitioned_key_values) - a proc returning a string which is the base name of the child table
        #
        # usage:
        #   partitioned do |partition|
        #     partition.base_name lambda {|model, *partition_key_values|
        #       return model.partition_normalize_key_value(*partition_key_values).to_s
        #     }
        #   end
        #
        # the default is similar to the second usage, that is: normalize the value and call to_s
        #
        def base_name(value)
          data.base_name = value
        end

        # Convert a base_name back to its original key value.
        def key_value(value)
          data.key_value = value
        end

        #
        # The child tables name without the schema name.
        #
        # in the example: 'foos_partitions.p42' the part_name would be p42
        #
        # arguments:
        #  value - a string, the part name
        #
        # usage:
        #   partitioned do |partition|
        #     partition.part_name "p42"
        #   end
        #
        # or:
        #
        # arguments:
        #  value - a string, the part name child table to be interpolated at runtime
        #
        # usage:
        #   partitioned do |partition|
        #     partition.part_name '#{model.table_name}_child_#{model.partition_normalize_key_value(field_value)}'
        #   end
        #
        # note: the string passed to partition.part_name is built using single quotes so that the
        # the string will not be interpolated at definition time.  Rather, the string will be
        # interpolated at run-time when it will scope the interpolation with the
        # partition_key_values array (and field_value which the the first element of that array)
        #
        # or:
        #
        # arguments:
        #  lambda(model, *partitioned_key_values) - a proc returning a string which is the part name of the child table
        #
        # usage:
        #   partitioned do |partition|
        #     partition.part_name lambda {|model, *partition_key_values|
        #       return "#{model.table_name}_child_#{model.partition_normalize_key_value(field_value)}"
        #     }
        #   end
        #
        # the default is similar to the third usage
        #
        def part_name(value)
          data.part_name = value
        end

        #
        # The full name of a child table defined by the partition key values.
        #
        # in the example: 'foos_partitions.p42' the table name would be foos_partitions.p42
        #
        # arguments:
        #  value - a string, the table name
        #
        # usage:
        #   partitioned do |partition|
        #     partition.table_name "foos_partitions.p42"
        #   end
        #
        # or:
        #
        # arguments:
        #  value - a string, the table name child table to be interpolated at runtime
        #
        # usage:
        #   partitioned do |partition|
        #     partition.table_name '#{model.table_name}_partitions.#{model.table_name}_child_#{model.partition_normalize_key_value(field_value)}'
        #   end
        #
        # or:
        #
        # arguments:
        #  lambda(model, *partitioned_key_values) - a proc returning a string which is the table name of the child table
        #
        # usage:
        #   partitioned do |partition|
        #     partition.table_name lambda {|model, *partition_key_values|
        #       return "#{model.table_name}_partitions.#{model.table_name}_child_#{model.partition_normalize_key_value(partition_key_values.first)}"
        #     }
        #   end
        #
        # the default is similar to the third usage
        #
        def table_name(value)
          data.table_name = value
        end

        # a reasonable alias for this table
        def table_alias_name(value)
          data.table_alias_name = value
        end

        #
        # The table name of the table who is the direct ancestor of a child table.
        #
        # arguments:
        #  value - a string, the parent table name
        #
        # usage:
        #   partitioned do |partition|
        #     partition.parent_table_name "foos"
        #   end
        #
        # or:
        #
        # arguments:
        #  value - a string, the parent table name child table to be interpolated at runtime
        #
        # usage:
        #   partitioned do |partition|
        #     partition.table_name '#{model.table_name}'
        #   end
        #
        # or:
        #
        # arguments:
        #  lambda(model, *partitioned_key_values) - a proc returning a string which is the parent table name of the child table
        #
        # usage:
        #   partitioned do |partition|
        #     partition.parent_table_name lambda {|model, *partition_key_values|
        #       return "#{model.table_name}"
        #     }
        #   end
        #
        # By default this is just the active record's notion of the name of the class.
        #
        def parent_table_name(value)
          data.parent_table_name = value
        end

        #
        # The schema name of the table who is the direct ancestor of a child table.
        #
        # arguments:
        #  value - a string, the schema name
        #
        # usage:
        #   partitioned do |partition|
        #     partition.parent_table_schema_name "public"
        #   end
        #
        # or:
        #
        # arguments:
        #  value - a string, the schema name to be interpolated at runtime
        #
        # usage:
        #   partitioned do |partition|
        #     partition.parent_table_schema_name '#{model.table_name}'
        #   end
        #
        # or:
        #
        # arguments:
        #  lambda(model, *partitioned_key_values) - a proc returning a string which is the schema name of the child table
        #
        # usage:
        #   partitioned do |partition|
        #     partition.parent_table_schema_name lambda {|model, *partition_key_values|
        #       return "#{model.table_name}"
        #     }
        #   end
        #
        # By default this is just the "public".
        #
        def parent_table_schema_name(value)
          data.parent_table_schema_name = value
        end

        #

        def janitorial_creates_needed(value)
          data.janitorial_creates_needed = value
        end

        def janitorial_archives_needed(value)
          data.janitorial_archives_needed = value
        end

        def janitorial_drops_needed(value)
          data.janitorial_drops_needed = value
        end
      end
    end
  end
end
